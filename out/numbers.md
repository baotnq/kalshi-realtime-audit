# Kalshi Realtime Audit — numbers

**Historical batch (Phases 1–2), not realtime.** Generated 2026-09-21T02:52Z from `data/kalshi.duckdb` (built 2026-09-21T02:32Z).
Source: Kalshi public REST API, no credentials. Assumptions: `docs/phase0.md`.

Crawl events (restarts, cutoff moves), from `data/state/crawl.json`:

- 2026-09-14T05:34:57Z: historical cutoff moved from 1783987200 to 1784073600 during the crawl; re-crawling historical so markets that left the live endpoint are not missed

Dataset: **15,280,713 settled markets**, settlement dates 2021-07-03 → 2026-09-13. Multivariate combo markets (MVE) are **excluded**
at crawl time (`mve_filter=exclude`). On 2026-09-10 they settled at ≥ 60,000 per sampled hour versus 2,700–4,500 non-combo
(phase0 §6). Category = the market's series category (`event_ticker` prefix → `GET /series`).

Convention: default figures are traded markets (`volume > 0`); all-market figures are shown alongside. All-market
columns include markets with zero recorded volume.

## 1. Settlement lag

| Category | N | close→settle p50 | p90 | p99 | expected exp.→settle p50 |
|---|---|---|---|---|---|
| Crypto | 873,743 | 3m | 31m | 2.2h | -2m |
| Financials | 276,962 | 30m | 5.1h | 25.0h | 4m |
| Sports | 1,703,183 | 6m | 6m | 31m | -18m |
| Climate and Weather | 182,947 | 3.1h | 10.7h | 37.5h | 66m |
| Commodities | 109,892 | 63m | 95m | 2.4d | 63m |
| Entertainment | 54,549 | 9.3h | 35.2h | 3.4d | -20.2h |
| Mentions | 64,842 | 31m | 11.9h | 2.6d | -21.7h |
| Economics | 24,408 | 7.1h | 14.1h | 6.2d | -54m |
| Politics | 12,792 | 71m | 13.3h | 4.5d | 38m |
| Elections | 8,429 | 30m | 30m | 9.5h | -153.0d |
| Science and Technology | 3,731 | 2.1h | 18.7h | 10.6d | 92m |
| Transportation | 1,104 | 13.1h | 37.2h | 4.6d | 25.3h |
| Health | 631 | 25.1h | 3.0d | 32.5d | 3.1d |
| Companies | 400 | 2.4h | 36.1h | 3.9d | -21.8h |
| World | 340 | 14.2h | 2.9d | 6.5d | 25.8h |
| Social | 70 | 30m | 16.5h | 12.0d | -6.7d |
| AI | 2 | 30m | 30m | 30m | -121.5d |
| Education | 1 | 35.5h | 35.5h | 35.5h | 210.9d |

Measured: per traded market, `settlement_ts − close_time` and `settlement_ts − expected_expiration_time`, in seconds,
over markets with a `settlement_ts` that traded at least one contract. Quantiles are continuous. Negative values mean
settlement came before that timestamp. `close_time` is the final value returned by the API, including early-close
updates. Full precision, including the all-markets columns: `out/lag_by_category.csv`.

### Reference: all settled markets (incl. zero-volume markets)

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

This view counts every settled market, including markets with zero recorded volume. The raw Commodities p99 is inflated
by a batch of zero-volume brackets settled on one date; the `volume > 0` figure is reported as primary.

### Elections, before vs after 2025-11-01

| Period (by close_time) | N | close→settle p50 | p90 | expected exp.→settle p50 |
|---|---|---|---|---|
| close before 2025-11-01 | 894 | 30m | 38m | -18.7h |
| close on/after 2025-11-01 | 7,904 | 30m | 30m | -165.0d |

Measured: the same lag for category Elections, split by whether `close_time` is before 2025-11-01.

## 2. Non-binary settlement

| Category | N | non-binary | share | share of volume | N all markets (incl. zero-volume) | non-binary (all) | share (all) | result not yes/no/scalar |
|---|---|---|---|---|---|---|---|---|
| Crypto | 873,743 | 32 | 0.00% | 0.00% | 9,752,782 | 38 | 0.00% | 0 |
| Financials | 276,962 | 44 | 0.02% | 0.08% | 2,684,027 | 44 | 0.00% | 0 |
| Sports | 1,703,183 | 18,722 | 1.10% | 0.24% | 2,137,613 | 34,453 | 1.61% | 0 |
| Climate and Weather | 182,947 | 183 | 0.10% | 0.08% | 240,743 | 308 | 0.13% | 0 |
| Commodities | 109,892 | 0 | 0.00% | 0.00% | 229,977 | 0 | 0.00% | 0 |
| Entertainment | 54,549 | 34 | 0.06% | 1.93% | 109,823 | 38 | 0.03% | 0 |
| Mentions | 64,842 | 130 | 0.20% | 0.17% | 65,127 | 130 | 0.20% | 0 |
| Economics | 24,408 | 18 | 0.07% | 0.22% | 30,846 | 22 | 0.07% | 0 |
| Politics | 12,792 | 42 | 0.33% | 0.21% | 13,828 | 52 | 0.38% | 0 |
| Elections | 8,429 | 39 | 0.46% | 3.46% | 8,798 | 40 | 0.45% | 0 |
| Science and Technology | 3,731 | 1 | 0.03% | 0.01% | 3,975 | 1 | 0.03% | 0 |
| Transportation | 1,104 | 0 | 0.00% | 0.00% | 1,579 | 0 | 0.00% | 0 |
| Health | 631 | 6 | 0.95% | 0.42% | 638 | 6 | 0.94% | 0 |
| Companies | 400 | 8 | 2.00% | 0.01% | 462 | 8 | 1.73% | 0 |
| World | 340 | 0 | 0.00% | 0.00% | 386 | 0 | 0.00% | 0 |
| Social | 70 | 1 | 1.43% | 0.00% | 106 | 1 | 0.94% | 0 |
| AI | 2 | 0 | 0.00% | 0.00% | 2 | 0 | 0.00% | 0 |
| Education | 1 | 0 | 0.00% | 0.00% | 1 | 0 | 0.00% | 0 |

Measured: a market counts as non-binary when `result = scalar` or `settlement_value_dollars` ∉ {0, 1}. The first three
columns count traded markets only; the "(all)" columns add markets with zero recorded volume. Volume is `volume_fp` (contracts)
summed per group over all markets. The "result not yes/no/scalar" column counts other or empty `result` values, which
are also counted in N all markets. Full precision: `out/nonbinary_by_category.csv`.

## 3. Dispute trail (forward-only)

Poll coverage: 480 successful sweeps, 1 failed,
0 log lines cut by a crash,
2026-09-13T05:00:43.839945Z → 2026-09-21T02:52:07.995436Z (7.9 days), 85 gaps longer than
10 min totalling 157.0 h.

| Category | N traded | N observed (all, incl. zero-volume) | disputed | amended | result changed | still in review | median time disputed | median time amended |
|---|---|---|---|---|---|---|---|---|
| Crypto | 3,121 | 56,700 | 0 | 0 | 0 | 0 | – | – |
| Commodities | 3,780 | 19,101 | 0 | 0 | 0 | 0 | – | – |
| Sports | 9,189 | 14,561 | 0 | 0 | 0 | 0 | – | – |
| Financials | 1,197 | 9,241 | 0 | 0 | 0 | 0 | – | – |
| Climate and Weather | 4,827 | 6,705 | 0 | 0 | 0 | 0 | – | – |
| Entertainment | 1,471 | 4,085 | 0 | 0 | 0 | 0 | – | – |
| Economics | 3,397 | 3,812 | 0 | 0 | 0 | 0 | – | – |
| Mentions | 406 | 873 | 5 | 5 | 0 | 0 | 2.5h | 82m |
| (unknown series) | 365 | 446 | 0 | 0 | 0 | 0 | – | – |
| Science and Technology | 145 | 285 | 0 | 0 | 0 | 0 | – | – |
| Politics | 115 | 194 | 0 | 0 | 0 | 0 | – | – |
| Elections | 87 | 96 | 0 | 0 | 0 | 0 | – | – |
| World | 0 | 10 | 0 | 0 | 0 | 0 | – | – |
| Health | 0 | 9 | 0 | 0 | 0 | 0 | – | – |
| Companies | 0 | 5 | 0 | 0 | 0 | 0 | – | – |

Measured: `GET /markets?status=closed&mve_filter=exclude` polled every 5 min since the first sweep.
N observed = distinct markets seen at least once in `closed`/`determined`/`disputed`/`amended`; N traded counts those
whose `volume_fp` was above zero in any sweep. A market counts as
disputed or amended if any sweep saw it in that status. Time in a status runs from the first sweep that saw it to the
first later sweep that did not, so it is accurate to ± one poll interval, and a status shorter than the interval can be
missed. Result changed = more than one distinct non-empty `result` across sweeps and the final settled record. The
Kalshi API exposes no status history, so none of this exists before the first sweep.

Invariant checks run in `build.py`; their output is kept in `data/anomalies.parquet` and nothing is dropped from the
tables above.
