# Phase 0 — Kalshi public API discovery

Probed 2026-09-13 (UTC ~04:20–05:10) from an unauthenticated client, no API key.
Base URL: `https://api.elections.kalshi.com/trade-api/v2`. Docs fetched the same day from `https://docs.kalshi.com/llms.txt`.

Labels: **[T]** observed in a response or stated in docs · **[A]** inference · **[A-b]** not verified.

---

## 1. Auth, rate limit, pagination

**No API key needed for `GET /markets` or `GET /historical/markets`.** [T]

```
GET /markets?status=settled&limit=1000
→ HTTP 200, 1000 markets, 3.36 MB, 3.8 s, body keys: ["cursor","markets"]
  headers: cache-control: public, max-age=15 ; no X-RateLimit-* headers
GET /historical/markets?limit=2        → HTTP 200 (no key)
GET /historical/cutoff                 → HTTP 200 (no key)
```

**Page size:** `limit` max 1000 (docs: "Defaults to 100. Maximum value is 1000"). **Cursor:** opaque string in `cursor`; empty when done. [T]

**Invalid cursor is not an error** (checked 2026-09-13): `cursor=garbage` on both `/historical/markets` and `/markets` returns HTTP 200 with the **same first page** as no cursor. [T] Whether cursors expire is undocumented [A-b]. An expired cursor would therefore restart a crawl silently, so `crawl.py` detects a resume that returns the stream's first page and logs it.

**Rate limit (unauthenticated):** not documented. Docs state 429 carries no `Retry-After` / `X-RateLimit-*` headers, token-bucket, "apply exponential backoff". Observed: [T]

| Test | Result |
|---|---|
| 40 parallel requests (`limit=100`) | 13 × 200, 27 × 429 in 2.6 s |
| Sequential, target 2/s, 20 req (`limit=1000`) | 20 × 200; achieved 0.30 req/s |
| Sequential, target 5/s, 30 req (`limit=1000`) | 30 × 200; achieved 0.26 req/s |
| Sequential, target 10/s | TLS `Connection reset by peer` after the 5/s run |

**The binding constraint is response latency, not the token bucket:** a 1000-row page takes 3.3–4.7 s (median ~4 s). Sequential throughput ≈ **250 markets/s ≈ 0.9 M/hour**. Bursting in parallel triggers 429 and connection resets. [T] A small fixed concurrency (2–3) with backoff may raise this [A-b].

## 2. Live vs historical split — two endpoints are required

```
GET /historical/cutoff
→ {"market_settled_ts":"2026-07-14T00:00:00Z", "trades_created_ts":"2026-07-14T00:00:00Z", ...}
```

Docs (Historical Data): markets that **settled before `market_settled_ts`** are only in `GET /historical/markets`; `GET /markets` no longer returns them. Target live window is **3 months**, cutoff advances over time. [T]

Consequences for `crawl.py`:
- Crawl `/historical/markets` (all) + `/markets?status=settled` (since cutoff); dedupe by `ticker`. A market can move from live to historical between runs [A].
- `/historical/markets` filters are **mutually exclusive** and there are **no time filters** (only `tickers`, `event_ticker`, `series_ticker`, `mve_filter=exclude`). Resume is by cursor only. [T]
- `/markets` supports `min_settled_ts` / `max_settled_ts`, so the live part can be crawled in time windows. [T]
- Historical ordering is **not strictly by `settlement_ts`** (8 pages checked: first 2026-07-13T22:36, last 2026-07-12T13:20, not monotonic). [T]
- Depth: `series_ticker=KXHIGHNY` on historical returns 9,178 markets closing 2021-08-07 → 2026-07-13. Old tickers were renamed with a `KX` prefix (`HIGHNY`, `INXD`, `FED` return 0). [T]

## 3. Field presence on settled markets

Sample: 1,000 live settled (mostly MVE) + 8,000 historical non-MVE + 40,000 live non-MVE settled on 2026-09-10. [T]

| Brief field | Present | Notes |
|---|---|---|
| `created_time` | yes, always | |
| `open_time` | yes, always | |
| `close_time` | yes, always | |
| `expected_expiration_time` | yes, always | Can be **before** `close_time` by design (docs FAQ). |
| `expiration_time` | yes | **Deprecated** (docs). Also present: `latest_expiration_time` — use that instead. |
| `settlement_ts` | yes, always | |
| `settlement_timer_seconds` | yes, always | MVE: 5 s; sample sports market: 30 s. |
| `result` | yes | Values seen: `yes`, `no`, **`scalar`**. |
| `settlement_value_dollars` | yes | String, 4 decimals (`"0.0000"`, `"0.6200"`). |
| `early_close_condition` | **only when set** | Absent on 997/1000 MVE markets; present on normal sports markets. |
| `can_close_early` | yes, always | |
| `rules_primary` | yes, but **empty on MVE** (3/1000 non-empty) | |
| `rules_secondary` | yes | Empty on some MVE. |
| `category` | **NO** — not on markets | See §4. |
| `event_ticker` | yes, always | |
| `volume` | **NO** | Replaced by `volume_fp` (string, contracts). Also `volume_24h_fp`, `open_interest_fp`, `notional_value_dollars`. |

Also present and useful: `status` (always `finalized` on settled), `market_type` (`binary` in all samples), `updated_time`, `mve_collection_ticker`, `exchange_index`, `strike_type`, `expiration_value`. **No `determined_time`** in REST. [T]

## 4. Category

`GET /series` (no params) returns all **14,013 series** in one response, each with `category` (20 values: Sports 3,638 · Entertainment 2,537 · Politics 2,307 · Elections 1,747 · Financials 959 · Economics 780 · …). [T]

Join rule as first probed: `series_ticker = split(event_ticker, '-')[0]`. Matched **8,000/8,000** historical non-MVE and **1,000/1,000** MVE markets. All MVE combos map to `Exotics`. [T]

**Correction (Phase 2, 2026-09-13):** the first-segment rule fails for series whose ticker contains `-`. `GET /events/KXMLBWINS-BOS-26` returns `series_ticker: KXMLBWINS-BOS`, and `GET /series/KXMLBWINS` returns 404. [T] `build.py` now uses the **longest `-`-separated prefix of `event_ticker` that is a known series**. Unmatched markets are still written to anomalies as `unknown_series`. `GET /events` also carries `category` + `series_ticker` and covers all events regardless of cutoff, but excludes MVE [T]. Prefix join avoids crawling events; a mismatch count goes to anomalies [A].

## 5. Disputed / amended in historical data

**Not exposed historically.** [T]
- REST returns only the **current** `status`. Docs list `determined`, `disputed`, `amended` as possible REST statuses, but there is no status history field or endpoint.
- Every settled market in all samples has `status = finalized`; nothing records that it passed through `disputed`/`amended`.
- **WebSocket `market_lifecycle_v2` has no `disputed` or `amended` event type** (enum: `created, activated, deactivated, close_date_updated, determined, settled, price_level_structure_updated, metadata_updated`). This supersedes the 12/9 assumption that WS carries dispute transitions.
- WS **requires an authenticated connection** (API key) even for public channels. [T]

Snapshot of `GET /markets?status=closed&mve_filter=exclude` (17 pages, 16,116 markets): `closed` 15,732 · `determined` 384 · **`disputed` 0 · `amended` 0**. [T]

Consequence for Metric 3:
- The historical part of Metric 3 is **out**.
- Forward-only options: (a) poll `GET /markets?status=closed` without a key and record `status` changes (catches `disputed`/`amended` only if the poll lands inside that window); (b) WS with a key, inferring an amendment from a second `determined` event on the same ticker [A-b: not documented that amended re-emits `determined`].
- A single snapshot found 0 disputes among 16k closed markets, so expected N is small. Metric 3 may need weeks of polling before N > 0 [A].

## 6. Size and crawl wall time

**The "~2.2M settled markets" figure does not match current volume.** [T] Settled counts per hour on 2026-09-10:

| Hour (UTC) | non-MVE | MVE (combos) |
|---|---|---|
| 03–04 | 4,490 | ≥ 60,000 (capped) |
| 12–13 | 2,666 | ≥ 60,000 (capped) |
| 19–20 | 3,564 | not measured |

- non-MVE ≈ 2.7–4.5k/hour → **~65–110k/day** currently [A: 3 hours sampled]. The live 3-month window alone could hold several million non-MVE markets [A-b].
- MVE ≥ 60k/hour → **≥ 1.4M/day** [A]. MVE are auto-generated parlays with empty `rules_primary` and a 5 s settlement timer. Their volume swamps everything else.

Wall time at the observed ~250 markets/s sequential:

| Scope | Rows (estimate) | Wall time |
|---|---|---|
| 2.2M (brief figure) | 2.2M | ~2.5 h |
| non-MVE, all history | 3–10M [A-b] | ~3–11 h |
| incl. MVE | tens of millions+ [A-b] | days; raw JSONL ≈ 3.3 KB/market → 100+ GB |

The exact non-MVE total is unknown without crawling: `/historical/markets` has no count and no time filter. The first full `mve_filter=exclude` crawl gives the number.

## 7. Follow-up checks on the samples (same data, no new requests)

Samples: 8,000 historical non-MVE + 1,000 live MVE. [T]

- **Brief invariants: 0 violations** in both samples. Checked `created_time < close_time <= settlement_ts`, `result in {yes,no} ⇒ settlement_value_dollars in {0,1}`, and missing `settlement_ts`. The earlier worry about early-close markets was an assumption and did not hold. Early close moves `close_time` earlier, so `close <= settlement` still holds [A].
- **`settlement_timer_seconds`** (the window in which a determined result can be disputed), non-MVE: 300 s (4,028) · 3,600 s (2,097) · 1 s (711) · 60 s (595) · 180 s (321) · 1,800 s (109). MVE: 5 s (997/1000).
- **Close → settlement lag**, non-MVE sample: p50 366 s, p90 5,450 s, max 40,529 s. MVE: p50 18 s.
- **`updated_time` − `settlement_ts`**, non-MVE: p50 0 s. But **1,246/8,000 (15.6%) are more than 1 h**, max ≈ 22 days. The cause is unknown. It may be a trace of post-determination changes [A-b], investigated in `docs/updated_time.md`. MVE: max 1 s.

## 8. Decisions (approved by owner 2026-09-13)

1. **MVE excluded.** `mve_filter=exclude` on both endpoints, at crawl time. `numbers.md` states the exclusion and gives the sampled hourly MVE counts from §6, not a total. Crawling MVE happens only on an external trigger (e.g. Kalshi Research asks).
2. **Metric 3: historical part out. Unauthenticated REST polling of `status=closed`, no WS.** WS carries no dispute events (§5), so the brief's WS condition has no basis. `poll.py` reuses the crawl client and polls every 5 minutes (the most common timer is 300 s). Every sweep records its start and end time (coverage), so gaps are visible. Output is stated as "observed N days, M events". WS only on a trigger, e.g. polling shows disputes shorter than the poll interval.
3. **Raw stored gzip-compressed**, append-only. Each line holds the response body verbatim, the request URL, `fetched_at` and the body's sha256.
4. **Invariants kept exactly as in the brief.** Violations go to anomalies and are never dropped.

No credentials were used.
