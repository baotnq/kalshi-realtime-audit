# Kalshi Realtime Audit

Resolution analytics for Kalshi markets from the public API, with three numbers per category:

1. **Settlement lag**: `close_time` → `settlement_ts`, and `expected_expiration_time` → `settlement_ts`.
2. **Non-binary settlement**: markets settling with `result = scalar` or `settlement_value_dollars` ∉ {0, 1}, and their volume.
3. **Dispute trail**: markets passing through `disputed` / `amended`, time spent there, and result changes.

**Metrics 1–2 are a historical batch, not realtime.** **Metric 3 is forward-only.** Kalshi's API exposes no status history, so it is recorded by polling from the first sweep onward. See `docs/phase0.md`.

## Run

```
git clone <repo> && cd kalshi-realtime-audit
make data      # crawl all settled non-combo markets → data/raw/, build data/kalshi.duckdb (hours; resumable);
               # then fetch categories for legacy events missing from GET /series and rebuild if any were new
make numbers   # → out/numbers.md, out/*.csv, out/*.png
make poll      # Metric 3 collector, every 5 min; run on an always-on host (deploy/kalshi-poll.service)
```

Requires Python 3.10+. No API key is needed.

## Stop and restart

Crawl and poller are safe to interrupt, including a power loss. Run the same command again and they resume.

```
kill $(cat data/state/crawl.pid) $(cat data/state/poll.pid)       # graceful: stop after the current page/sweep
nohup .venv/bin/python crawl.py >> data/logs/crawl.log 2>&1 & echo $! > data/state/crawl.pid
nohup .venv/bin/python poll.py  >> data/logs/poll.log  2>&1 & echo $! > data/state/poll.pid
```

- Cursors and committed raw-segment sizes are saved after every page. A tail damaged by a crash is never appended to; writing continues in a new segment (`*.0001.jsonl.gz`), and the damage is reported in `data/anomalies.parquet`.
- The API answers an unusable cursor with HTTP 200 and the first page. `crawl.py` detects that and logs a restart instead of silently re-crawling.
- If the historical cutoff moves during a crawl, the historical stream is re-crawled.
- Restarts and cutoff moves are listed in `out/numbers.md`. Poller downtime shows as gaps in poll coverage.
- `tests/restart_test.py` exercises all of the above (crash mid-write, SIGKILL, SIGTERM, ignored cursor, cutoff move, poller restart) against a temp copy.

## Layout

| Path | What |
|---|---|
| `kalshi_api.py` | public REST client (backoff on 429) and append-only gzip raw store with sha256 per page |
| `crawl.py` | `/historical/markets` + `/markets?status=settled`, `mve_filter=exclude`, resumable by cursor |
| `build.py` | raw → DuckDB table `markets`; invariant checks → `data/anomalies.parquet` (never dropped) |
| `poll.py` | polls `/markets?status=closed`, logs status changes and sweep coverage |
| `metrics.py` | the three tables, charts, and `out/numbers.md` |
| `docs/phase0.md` | every API assumption, with the request/response that shows it |
| `docs/updated_time.md` | why `updated_time` is not used as a dispute signal |

## Scope and limits

- Kalshi only. Multivariate combo markets are excluded at crawl time (phase0 §6).
- Category is the series category, matched by the longest `event_ticker` prefix that is a known series.
- The poller sees a dispute only if a sweep lands while the market is `disputed` or `amended`. Durations are accurate to ± one poll interval, and every sweep's coverage is logged.
- Raw data is not committed. `make data` regenerates it from the API.
