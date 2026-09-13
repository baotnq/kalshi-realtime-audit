"""Crawl all settled non-MVE Kalshi markets into append-only raw files.

This is a historical batch (Phases 1–2), not realtime.

Two sources are required because the API splits data at a cutoff (docs/phase0.md §2):
  historical  GET /historical/markets?mve_filter=exclude     cursor only, no time filter
  live        GET /markets?status=settled&mve_filter=exclude  windowed by settlement time
Plus GET /historical/cutoff and GET /series (category lookup), once per run.

Outputs, under data/raw/:
  cutoff.jsonl.gz  series.jsonl.gz  historical_markets.jsonl.gz  live_markets.jsonl.gz

Resumable: the cursor is saved in data/state/crawl.json after every page is written.
A page written just before a crash may be written again on resume; build.py dedupes
by ticker.
"""
import argparse
import json
import threading
import time
from datetime import datetime
from pathlib import Path

from kalshi_api import append_page, fetch, paginate, utcnow, write_json_atomic

RAW = Path("data/raw")
STATE = Path("data/state/crawl.json")
LIVE_OVERLAP_S = 3600  # re-read the last hour of the previous live window; filter bounds are not documented as inclusive

lock = threading.Lock()


def iso_to_ts(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def save(state: dict) -> None:
    with lock:
        write_json_atomic(STATE, state)


def snapshot(path: str, file: str) -> dict:
    fetched_at = utcnow()
    url, body = fetch(path)
    append_page(RAW / file, url, body, fetched_at)
    return json.loads(body)


def run_stream(name: str, path: str, params: dict, st: dict, state: dict, max_pages: int | None) -> None:
    out = RAW / f"{name}_markets.jsonl.gz"
    pages = rows = 0
    t0 = time.monotonic()
    for url, body, data, next_cursor in paginate(path, params, cursor=st.get("cursor")):
        append_page(out, url, body, utcnow(), stream=name)
        pages += 1
        rows += len(data.get("markets", []))
        st["cursor"] = next_cursor
        st["pages"] = st.get("pages", 0) + 1
        st["rows"] = st.get("rows", 0) + len(data.get("markets", []))
        if next_cursor is None:
            st["done"] = True
        save(state)
        if pages % 25 == 0 or st.get("done"):
            rate = rows / (time.monotonic() - t0)
            print(f"[{name}] {st['rows']} rows total, {pages} pages this run, {rate:.0f} rows/s", flush=True)
        if max_pages and pages >= max_pages:
            print(f"[{name}] stopped at --max-pages {max_pages}; rerun to resume", flush=True)
            return


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-pages", type=int, help="stop each stream after N pages (testing); resumable")
    args = ap.parse_args()

    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    hist = state.setdefault("historical", {})
    live = state.setdefault("live", {})

    cutoff = snapshot("historical/cutoff", "cutoff.jsonl.gz")
    cutoff_ts = iso_to_ts(cutoff["market_settled_ts"])
    series = snapshot("series", "series.jsonl.gz")
    print(f"cutoff market_settled_ts={cutoff['market_settled_ts']}, {len(series['series'])} series", flush=True)

    # Live window: resume an unfinished one, else start where the last finished one ended.
    if not live.get("window") or live.get("done"):
        done_until = live.get("done_until")
        if done_until is not None and done_until < cutoff_ts:
            # Markets settled in [done_until, cutoff) have moved to historical since the last run.
            print(f"WARNING: live data ended at {done_until} but cutoff is now {cutoff_ts}; "
                  "re-crawling historical to close the gap", flush=True)
            state["historical"] = hist = {}
        start = cutoff_ts if done_until is None else max(done_until - LIVE_OVERLAP_S, 0)
        live.clear()
        live["window"] = [start, int(time.time())]
    lo, hi = live["window"]

    streams = []
    if not hist.get("done"):
        streams.append(("historical", "historical/markets", {"mve_filter": "exclude"}, hist))
    else:
        print("[historical] already complete", flush=True)
    streams.append(("live", "markets",
                    {"status": "settled", "mve_filter": "exclude", "min_settled_ts": lo, "max_settled_ts": hi}, live))

    errors = []

    def worker(*a):
        try:
            run_stream(*a, state, args.max_pages)
        except Exception as e:
            errors.append((a[0], e))

    threads = [threading.Thread(target=worker, args=s) for s in streams]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if live.get("done"):
        live["done_until"] = hi
        save(state)
    for name, e in errors:
        print(f"[{name}] FAILED: {e!r}; rerun to resume from the saved cursor", flush=True)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
