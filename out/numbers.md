# Kalshi Realtime Audit — numbers

**Historical batch (Phases 1–2), not realtime.** Generated 2026-09-14T17:04Z from `data/kalshi.duckdb` (built 2026-09-14T16:49Z).
Source: Kalshi public REST API, no credentials. Assumptions: `docs/phase0.md`.

Crawl events (restarts, cutoff moves), from `data/state/crawl.json`:

- 2026-09-14T05:34:57Z: historical cutoff moved from 1783987200 to 1784073600 during the crawl; re-crawling historical so markets that left the live endpoint are not missed

Dataset: **15,280,713 settled markets**, settlement dates 2021-07-03 → 2026-09-13. Multivariate combo markets (MVE) are **excluded**
at crawl time (`mve_filter=exclude`). On 2026-09-10 they settled at ≥ 60,000 per sampled hour versus 2,700–4,500 non-combo
(phase0 §6). Category = the market's series category (`event_ticker` prefix → `GET /series`).

## 1. Settlement lag

| Category | N | close→settle p50 | p90 | p99 | expected exp.→settle p50 | p90 | settled before expected exp. |
|---|---|---|---|---|---|---|---|
| Crypto | 9,752,782 | 3m | 32m | 70m | -2m | 27m | 71.3% |
| Financials | 2,684,027 | 4m | 2.2h | 6.6h | 3m | 2.1h | 0.8% |
| Sports | 2,137,613 | 6m | 6m | 53m | -14m | 75m | 59.0% |
| Climate and Weather | 240,743 | 91m | 13.0h | 5.5d | 76m | 2.5h | 30.7% |
| Commodities | 229,977 | 63m | 92m | 211.0d | 63m | 92m | 5.7% |
| Entertainment | 109,823 | 10.9h | 35.0h | 3.5d | -7.9d | 8.6h | 65.8% |
| Mentions | 65,127 | 31m | 12.0h | 2.9d | -21.7h | 10.8h | 74.1% |
| Economics | 30,846 | 7.1h | 23.1h | 8.7d | -64m | 24.0h | 63.7% |
| Politics | 13,828 | 70m | 13.5h | 9.1d | 38m | 25.4h | 46.6% |
| Elections | 8,798 | 30m | 30m | 10.9h | -152.6d | 32.2h | 86.4% |
| Science and Technology | 3,975 | 115m | 18.4h | 10.1d | 89m | 19.1h | 29.7% |
| Transportation | 1,579 | 13.1h | 37.3h | 4.6d | 25.3h | 2.3d | 6.5% |
| Health | 638 | 25.1h | 3.0d | 32.5d | 3.0d | 15.8d | 6.8% |
| Companies | 462 | 2.5h | 36.1h | 3.9d | -21.6h | 16.1h | 81.4% |
| World | 386 | 13.0h | 2.9d | 6.5d | 5.0h | 9.6d | 40.2% |
| Social | 106 | 42m | 24.6h | 82.1d | -10.7d | 104m | 84.0% |
| AI | 2 | 30m | 30m | 30m | -121.5d | -121.5d | 100.0% |
| Education | 1 | 35.5h | 35.5h | 35.5h | 210.9d | 210.9d | 0.0% |

Measured: per market, `settlement_ts − close_time` and `settlement_ts − expected_expiration_time`, in seconds, over all
settled markets with a `settlement_ts`. Quantiles are continuous. Negative values mean settlement came before that
timestamp. `close_time` is the final value returned by the API, including early-close updates. Full precision:
`out/lag_by_category.csv`.

### Elections, before vs after 2025-11-01

| Period (by close_time) | N | close→settle p50 | p90 | expected exp.→settle p50 |
|---|---|---|---|---|
| close before 2025-11-01 | 894 | 30m | 38m | -18.7h |
| close on/after 2025-11-01 | 7,904 | 30m | 30m | -165.0d |

Measured: the same lag for category Elections, split by whether `close_time` is before 2025-11-01.

## 2. Non-binary settlement

| Category | N | non-binary | share | result not yes/no/scalar | share of volume |
|---|---|---|---|---|---|
| Crypto | 9,752,782 | 38 | 0.00% | 0 | 0.00% |
| Financials | 2,684,027 | 44 | 0.00% | 0 | 0.08% |
| Sports | 2,137,613 | 34,453 | 1.61% | 0 | 0.24% |
| Climate and Weather | 240,743 | 308 | 0.13% | 0 | 0.08% |
| Commodities | 229,977 | 0 | 0.00% | 0 | 0.00% |
| Entertainment | 109,823 | 38 | 0.03% | 0 | 1.93% |
| Mentions | 65,127 | 130 | 0.20% | 0 | 0.17% |
| Economics | 30,846 | 22 | 0.07% | 0 | 0.22% |
| Politics | 13,828 | 52 | 0.38% | 0 | 0.21% |
| Elections | 8,798 | 40 | 0.45% | 0 | 3.46% |
| Science and Technology | 3,975 | 1 | 0.03% | 0 | 0.01% |
| Transportation | 1,579 | 0 | 0.00% | 0 | 0.00% |
| Health | 638 | 6 | 0.94% | 0 | 0.42% |
| Companies | 462 | 8 | 1.73% | 0 | 0.01% |
| World | 386 | 0 | 0.00% | 0 | 0.00% |
| Social | 106 | 1 | 0.94% | 0 | 0.00% |
| AI | 2 | 0 | 0.00% | 0 | 0.00% |
| Education | 1 | 0 | 0.00% | 0 | 0.00% |

Measured: a market counts as non-binary when `result = scalar` or `settlement_value_dollars` ∉ {0, 1}. Volume is
`volume_fp` (contracts) summed per group. The "result not yes/no/scalar" column counts other or empty `result` values,
which are also counted in N. Full precision: `out/nonbinary_by_category.csv`.

## 3. Dispute trail (forward-only)

Poll coverage: 130 successful sweeps, 0 failed,
0 log lines cut by a crash,
2026-09-13T05:00:43.839945Z → 2026-09-14T17:00:00.904129Z (1.5 days), 5 gaps longer than
10 min totalling 25.8 h.

| Category | N observed | disputed | amended | result changed | still in review | median time disputed | median time amended |
|---|---|---|---|---|---|---|---|
| Crypto | 12,651 | 0 | 0 | 0 | 0 | – | – |
| Financials | 6,199 | 0 | 0 | 0 | 0 | – | – |
| Sports | 5,784 | 0 | 0 | 0 | 0 | – | – |
| Commodities | 4,546 | 0 | 0 | 0 | 0 | – | – |
| Entertainment | 2,336 | 0 | 0 | 0 | 0 | – | – |
| Climate and Weather | 1,439 | 0 | 0 | 0 | 0 | – | – |
| Economics | 1,025 | 0 | 0 | 0 | 0 | – | – |
| Mentions | 481 | 0 | 0 | 0 | 0 | – | – |
| Science and Technology | 201 | 0 | 0 | 0 | 0 | – | – |
| Politics | 97 | 0 | 0 | 0 | 0 | – | – |
| Elections | 15 | 0 | 0 | 0 | 0 | – | – |
| World | 10 | 0 | 0 | 0 | 0 | – | – |
| Health | 9 | 0 | 0 | 0 | 0 | – | – |
| Companies | 5 | 0 | 0 | 0 | 0 | – | – |
| (unknown series) | 3 | 0 | 0 | 0 | 0 | – | – |

Measured: `GET /markets?status=closed&mve_filter=exclude` polled every 5 min since the first sweep.
N observed = distinct markets seen at least once in `closed`/`determined`/`disputed`/`amended`. A market counts as
disputed or amended if any sweep saw it in that status. Time in a status runs from the first sweep that saw it to the
first later sweep that did not, so it is accurate to ± one poll interval, and a status shorter than the interval can be
missed. Result changed = more than one distinct non-empty `result` across sweeps and the final settled record. The
Kalshi API exposes no status history, so none of this exists before the first sweep.
