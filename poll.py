"""Metric 3 collector: poll closed (not yet finalized) markets and log state changes.

Why polling: REST has no status history and the WebSocket lifecycle channel has
no disputed/amended events (docs/phase0.md §5, §8.2). A dispute is only seen if
a sweep lands while the market sits in `disputed` or `amended`.

Outputs (append-only, under data/raw/poll/):
  sweeps.jsonl                 one line per sweep: coverage (start, end, pages, markets, error)
  changes-YYYY-MM-DD.jsonl.gz  one line per ticker whose tracked fields changed, with the
                               market object verbatim; `left_closed_set` when a ticker disappears
  pages-YYYY-MM-DD.jsonl.gz    full page bodies, first complete sweep of each UTC hour

Pagination over a set that changes during the sweep can miss or repeat a ticker, so
`left_closed_set` means "not seen in this sweep", not "finalized".
"""
import argparse
import gzip
import json
import time
from pathlib import Path

from kalshi_api import append_page, paginate, utcnow, write_json_atomic

DATA = Path("data")
POLL_DIR = DATA / "raw" / "poll"
STATE = DATA / "state" / "poll_last.json"
PARAMS = {"status": "closed", "mve_filter": "exclude"}
TRACKED = ("status", "result", "settlement_value_dollars", "close_time", "settlement_timer_seconds")


def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {"last": {}, "snapshot_hour": None}


def append_line(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "ab") as f:
        f.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))


def sweep(state: dict) -> dict:
    started = utcnow()
    day, hour = started[:10], started[:13]
    pages, seen, error = [], {}, None
    try:
        for url, body, data, _ in paginate("markets", PARAMS):
            pages.append((utcnow(), url, body))
            for m in data.get("markets", []):
                seen[m["ticker"]] = m
    except Exception as e:  # record the failed sweep instead of dying; coverage shows the gap
        error = repr(e)
    finished = utcnow()

    changes = 0
    if error is None:
        last = state["last"]
        for ticker, m in seen.items():
            now = {k: m.get(k) for k in TRACKED}
            prev = last.get(ticker)
            if prev != now:
                append_line(POLL_DIR / f"changes-{day}.jsonl.gz",
                            {"sweep_id": started, "observed_at": finished, "ticker": ticker,
                             "event": "first_seen" if prev is None else "changed",
                             "prev": prev, "market": m})
                changes += 1
        for ticker in last.keys() - seen.keys():
            append_line(POLL_DIR / f"changes-{day}.jsonl.gz",
                        {"sweep_id": started, "observed_at": finished, "ticker": ticker,
                         "event": "left_closed_set", "prev": last[ticker], "market": None})
            changes += 1
        state["last"] = {t: {k: m.get(k) for k in TRACKED} for t, m in seen.items()}
        if state["snapshot_hour"] != hour:
            for fetched_at, url, body in pages:
                append_page(POLL_DIR / f"pages-{day}.jsonl.gz", url, body, fetched_at, sweep_id=started)
            state["snapshot_hour"] = hour
        write_json_atomic(STATE, state)

    record = {"sweep_id": started, "started_at": started, "finished_at": finished,
              "pages": len(pages), "markets": len(seen), "changes": changes, "error": error}
    POLL_DIR.mkdir(parents=True, exist_ok=True)
    with open(POLL_DIR / "sweeps.jsonl", "a") as f:
        f.write(json.dumps(record) + "\n")
    return record


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--interval", type=int, default=300, help="seconds between sweep starts (default 300)")
    ap.add_argument("--once", action="store_true", help="run a single sweep and exit")
    args = ap.parse_args()
    state = load_state()
    while True:
        t0 = time.monotonic()
        r = sweep(state)
        print(json.dumps(r), flush=True)
        if args.once:
            return
        time.sleep(max(0, args.interval - (time.monotonic() - t0)))


if __name__ == "__main__":
    main()
