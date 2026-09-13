# Is `updated_time` a trace of post-settlement changes?

Question from phase0 §7: 15.6% of historical non-MVE markets have `updated_time` more than 1 h after `settlement_ts`. Could it recover part of Metric 3 historically?

Checked 2026-09-13 on data already fetched (no new requests): 8,000 historical non-MVE markets (phase0 sample) and 10,000 rows in `data/kalshi.duckdb` (5,000 historical + 5,000 live).

## Observations [T]

- **Late updates happen in batches.** Of 1,246 late markets in the phase0 sample, 1,221 have `updated_time` inside **4 minutes** (2026-07-13 21:31–21:35 UTC), while their `settlement_ts` spans **31 hours** (2026-07-12 13:42 → 2026-07-13 20:22). A second small batch: 20 markets at 2026-07-20 19:25.
- The DuckDB historical rows show the same minutes: 21:31 (451), 21:34 (75), 21:35 (68), 2026-07-20 19:25 (20).
- **Live settled markets: 0 of 5,000** have `updated_time` more than 1 h after `settlement_ts` (max 2,114 s).
- Late markets are 98% Sports, mostly tennis match series (`KXITFWMATCH`, `KXITFMATCH`, `KXATPCHALLENGERMATCH`, `KXWTASETWINNER`), with timers 60/180/300 s. `result` mix is normal (no 666, yes 560, scalar 20).

## Conclusion

- [A] `updated_time` after settlement is a **bulk metadata write**. It does not reflect per-market status changes. The main batch falls just before the historical cutoff (2026-07-14), which suggests an archival or maintenance job [A-b].
- It **cannot** be used as a historical dispute/amendment signal. Metric 3 stays forward-only via `poll.py` (phase0 §8.2).
- Honest gap [P.3]: the API gives no way to prove that none of these batched markets was amended. A market amended before the batch would look the same.
- The 20-market batch on 2026-07-20 is the only candidate not explained by the cutoff. It is left as is, with no follow-up unless Metric 3 numbers make it relevant [P.8].
