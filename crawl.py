"""Crawl all settled non-MVE Kalshi markets into append-only raw files.

This is a historical batch (Phases 1–2), not realtime.

Two sources are required because the API splits data at a cutoff (docs/phase0.md §2):
  historical  GET /historical/markets?mve_filter=exclude     cursor only, no time filter
  live        GET /markets?status=settled&mve_filter=exclude  windowed by settlement time
Plus GET /historical/cutoff and GET /series (category lookup), once per run.
`--events` instead fetches GET /events/{ticker} for events build.py left without a category.

Outputs, under data/raw/ (each a stream of gzip segments, see kalshi_api.py):
  cutoff  series  historical_markets  live_markets

Restart safety (rerun the same command after a crash, shutdown or kill):
- The cursor and the committed size of each raw segment are saved in
  data/state/crawl.json after every page. A damaged segment tail starts a new
  segment. A page re-read on resume is deduped by build.py.
- The API answers an unusable cursor with HTTP 200 and the first page, not an error
  (checked 2026-09-13). A resume that returns the stream's first page is logged as a
  restart. A cursor rejected with 4xx restarts the stream from page 1, once per run.
- If the historical cutoff moved since the crawl started, markets may have left the
  live endpoint before being read, so the historical stream is re-crawled.
- SIGTERM/SIGINT stop both streams after the current page.
Every such event is appended to state["events"] and reported in out/numbers.md.
"""
import argparse
import json
import signal
import threading
import time
import urllib.error
from datetime import datetime
from pathlib import Path

from kalshi_api import SegmentWriter, append_page, fetch, paginate, read_segments, utcnow, write_json_atomic

RAW = Path("data/raw")
STATE = Path("data/state/crawl.json")
EVENTS_TODO = Path("data/state/unresolved_events.json")  # written by build.py
EVENTS_CHANGED = Path("data/state/events_changed")        # tells `make data` to rebuild
LIVE_OVERLAP_S = 3600  # re-read the last hour of the previous live window; filter bounds are not documented as inclusive

lock = threading.Lock()
stop = threading.Event()


def iso_to_ts(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def save(state: dict) -> None:
    with lock:
        write_json_atomic(STATE, state)


def log_event(state: dict, message: str) -> None:
    print(f"EVENT: {message}", flush=True)
    with lock:
        state.setdefault("events", []).append({"at": utcnow(), "event": message})
    save(state)


def writer(state: dict, stem: str) -> SegmentWriter:
    return SegmentWriter(RAW, stem, state.setdefault("segments", {}).setdefault(stem, {}))


def snapshot(state: dict, path: str, stem: str) -> dict:
    fetched_at = utcnow()
    url, body = fetch(path)
    append_page(writer(state, stem), url, body, fetched_at)
    save(state)
    return json.loads(body)


def first_ticker_on_disk(stem: str) -> str | None:
    for page in read_segments(RAW, stem):
        markets = json.loads(page["body"]).get("markets") or []
        return markets[0]["ticker"] if markets else None
    return None


def run_stream(name: str, path: str, params: dict, st: dict, state: dict, max_pages: int | None) -> None:
    out = writer(state, f"{name}_markets")
    if st.get("pages") and "first_ticker" not in st:  # state written before this field existed
        st["first_ticker"] = first_ticker_on_disk(f"{name}_markets")
    pages = rows = 0
    t0 = time.monotonic()
    restarted = False
    while True:
        try:
            resumed_from = st.get("cursor")
            for url, body, data, next_cursor in paginate(path, params, cursor=resumed_from):
                markets = data.get("markets") or []
                head = markets[0]["ticker"] if markets else None
                if not st.get("pages"):
                    st["first_ticker"] = head
                elif resumed_from and pages == 0 and head is not None and head == st.get("first_ticker"):
                    # Phase-0 fact: the API answers an unusable cursor with HTTP 200 and the first page.
                    log_event(state, f"{name}: API returned the first page for the saved cursor (cursor ignored); "
                                     f"the stream restarts from page 1 and pages already stored are deduped by build.py")
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
                if stop.is_set():
                    print(f"[{name}] stop requested; state saved, rerun to resume", flush=True)
                    return
                if max_pages and pages >= max_pages:
                    print(f"[{name}] stopped at --max-pages {max_pages}; rerun to resume", flush=True)
                    return
            return
        except urllib.error.HTTPError as e:
            if st.get("cursor") is None or restarted:
                raise
            log_event(state, f"{name}: cursor rejected (HTTP {e.code}); restarting the stream from its first page")
            st["cursor"] = None
            save(state)
            restarted = True


def crawl_events(state: dict) -> None:
    """GET /events/{ticker} for events build.py could not categorize (legacy series gone from GET /series)."""
    todo = json.loads(EVENTS_TODO.read_text()) if EVENTS_TODO.exists() else []
    lookup = state.setdefault("event_lookup", {"fetched": [], "not_found": []})
    done = set(lookup["fetched"]) | set(lookup["not_found"])
    pending = [t for t in todo if t not in done]
    print(f"[events] {len(todo)} uncategorized events listed, {len(pending)} not fetched yet", flush=True)
    out = writer(state, "events")
    fetched = 0
    for ticker in pending:
        if stop.is_set():
            print("[events] stop requested; state saved, rerun to resume", flush=True)
            break
        fetched_at = utcnow()
        try:
            url, body = fetch(f"events/{ticker}")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            lookup["not_found"].append(ticker)
            save(state)
            continue
        append_page(out, url, body, fetched_at, event_ticker=ticker)
        lookup["fetched"].append(ticker)
        fetched += 1
        save(state)
        if fetched % 100 == 0:
            print(f"[events] {fetched} fetched this run", flush=True)
    if fetched:
        EVENTS_CHANGED.touch()
    print(f"[events] fetched {fetched}, not found so far {len(lookup['not_found'])}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-pages", type=int, help="stop each stream after N pages (testing); resumable")
    ap.add_argument("--events", action="store_true",
                    help="only fetch GET /events/{ticker} for events listed by build.py without a category")
    args = ap.parse_args()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    if args.events:
        crawl_events(state)
        return
    hist = state.setdefault("historical", {})
    live = state.setdefault("live", {})

    cutoff = snapshot(state, "historical/cutoff", "cutoff")
    cutoff_ts = iso_to_ts(cutoff["market_settled_ts"])
    series = snapshot(state, "series", "series")
    print(f"cutoff market_settled_ts={cutoff['market_settled_ts']}, {len(series['series'])} series", flush=True)

    prev_cutoff = state.get("cutoff_ts")
    if prev_cutoff is not None and prev_cutoff != cutoff_ts and hist:
        log_event(state, f"historical cutoff moved from {prev_cutoff} to {cutoff_ts} during the crawl; "
                         "re-crawling historical so markets that left the live endpoint are not missed")
        state["historical"] = hist = {}
    state["cutoff_ts"] = cutoff_ts

    # Live window: resume an unfinished one, else start where the last finished one ended.
    if not live.get("window") or live.get("done"):
        done_until = live.get("done_until")
        if done_until is not None and done_until < cutoff_ts:
            log_event(state, f"live data ended at {done_until} but the cutoff is now {cutoff_ts}; "
                             "re-crawling historical to close the gap")
            state["historical"] = hist = {}
        start = cutoff_ts if done_until is None else max(done_until - LIVE_OVERLAP_S, 0)
        live.clear()
        live["window"] = [start, int(time.time())]
    lo, hi = live["window"]
    save(state)

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
    while any(t.is_alive() for t in threads):  # join with a timeout so signals are handled promptly
        for t in threads:
            t.join(timeout=1)

    if live.get("done"):
        live["done_until"] = hi
        save(state)
    for name, e in errors:
        print(f"[{name}] FAILED: {e!r}; rerun to resume from the saved cursor", flush=True)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
