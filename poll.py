"""Metric 3 collector: poll closed (not yet finalized) markets and log state changes.

Why polling: REST has no status history and the WebSocket lifecycle channel has
no disputed/amended events (docs/phase0.md §5, §8.2). A dispute is only seen if
a sweep lands while the market sits in `disputed` or `amended`.

Outputs (append-only, under data/raw/poll/; the .gz ones are gzip segment streams):
  sweeps.jsonl                 one line per sweep: coverage (start, end, pages, markets, error)
  changes-YYYY-MM-DD[.NNNN].jsonl.gz  one line per ticker whose tracked fields changed, with the
                               market object verbatim; `left_closed_set` when a ticker disappears
  pages-YYYY-MM-DD[.NNNN].jsonl.gz    full page bodies, first complete sweep of each UTC hour

Pagination over a set that changes during the sweep can miss or repeat a ticker, so
`left_closed_set` means "not seen in this sweep", not "finalized".

Restart safety: state (last seen fields, committed segment sizes) is saved after each
sweep. A crash mid-sweep repeats that sweep's change lines on the next run, which
metrics.py tolerates. Downtime shows up as a gap in sweeps.jsonl. SIGTERM/SIGINT stop
after the current sweep.
"""
import argparse
import json
import signal
import time
from pathlib import Path

from kalshi_api import SegmentWriter, append_page, paginate, utcnow, write_json_atomic

DATA = Path("data")
POLL_DIR = DATA / "raw" / "poll"
SWEEPS = POLL_DIR / "sweeps.jsonl"
STATE = DATA / "state" / "poll_last.json"
PARAMS = {"status": "closed", "mve_filter": "exclude"}
TRACKED = ("status", "result", "settlement_value_dollars", "close_time", "settlement_timer_seconds")

stopping = False


def load_state() -> dict:
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    state.setdefault("last", {})
    state.setdefault("snapshot_hour", None)
    state.setdefault("segments", {})
    return state


def writer(state: dict, stem: str) -> SegmentWriter:
    return SegmentWriter(POLL_DIR, stem, state["segments"].setdefault(stem, {}))


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
        out = writer(state, f"changes-{day}")
        for ticker, m in seen.items():
            now = {k: m.get(k) for k in TRACKED}
            prev = last.get(ticker)
            if prev != now:
                out.append({"sweep_id": started, "observed_at": finished, "ticker": ticker,
                            "event": "first_seen" if prev is None else "changed", "prev": prev, "market": m})
                changes += 1
        for ticker in last.keys() - seen.keys():
            out.append({"sweep_id": started, "observed_at": finished, "ticker": ticker,
                        "event": "left_closed_set", "prev": last[ticker], "market": None})
            changes += 1
        state["last"] = {t: {k: m.get(k) for k in TRACKED} for t, m in seen.items()}
        if state["snapshot_hour"] != hour:
            snap = writer(state, f"pages-{day}")
            for fetched_at, url, body in pages:
                append_page(snap, url, body, fetched_at, sweep_id=started)
            state["snapshot_hour"] = hour
        write_json_atomic(STATE, state)

    record = {"sweep_id": started, "started_at": started, "finished_at": finished,
              "pages": len(pages), "markets": len(seen), "changes": changes, "error": error}
    with open(SWEEPS, "a") as f:
        f.write(json.dumps(record) + "\n")
    return record


def terminate_partial_line() -> None:
    """If a crash left sweeps.jsonl without a final newline, end that line so the next record stays intact."""
    if SWEEPS.exists() and SWEEPS.stat().st_size:
        with open(SWEEPS, "rb") as f:
            f.seek(-1, 2)
            if f.read(1) != b"\n":
                with open(SWEEPS, "a") as g:
                    g.write("\n")


def main() -> None:
    global stopping
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--interval", type=int, default=300, help="seconds between sweep starts (default 300)")
    ap.add_argument("--once", action="store_true", help="run a single sweep and exit")
    args = ap.parse_args()

    def request_stop(*_):
        global stopping
        stopping = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, request_stop)
    POLL_DIR.mkdir(parents=True, exist_ok=True)
    terminate_partial_line()
    state = load_state()
    while not stopping:
        t0 = time.monotonic()
        print(json.dumps(sweep(state)), flush=True)
        if args.once:
            return
        while not stopping and time.monotonic() - t0 < args.interval:
            time.sleep(1)
    print("stop requested; state saved", flush=True)


if __name__ == "__main__":
    main()
