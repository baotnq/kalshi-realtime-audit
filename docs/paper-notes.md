# Notes on the Kalshi calibration paper

Kagan, N. & Baiocchi, R. *Calibration in Prediction Markets: Theory and Evidence.* Kalshi Research working paper, August 2026.
`https://kalshi.com/research/kalshi-research-calibration.pdf` (33 pages). Read 2026-09-15 from a browser download; the URL blocks non-browser clients. The PDF is not stored in this repo.

Only statements quoted or paraphrased from the paper below are [T]. How they relate to this project is [A].

## What the paper says [T]

- **Sample** (abstract, §4.3.1): 2,243,741 resolved markets, 2021 → mid-2026, eleven categories. The categories are Climate, Commodities, Crypto, Economics, Elections, Entertainment, Finance, Mentions, Politics, Sports and Tech & Science. Exotics ("Combos") are excluded from the pooled results (§4.1.1).
- **Clock** (§4.4): every horizon is measured from "the timestamp at which Kalshi formally closes a market … an administrative clock, rather than a real-world one."
- **Sports** (§4.4): "Sports market closing times often lag the games' end by a few minutes." Re-anchored with "a proprietary dataset containing to-the-second game-end timestamps."
- **Elections** (§4.4): "Election market closing times can lag by days or weeks." Re-anchored to election day. Markets "were contractually, until November 2025, unable to be resolved earlier than the date of vote certification, or, in some cases, inauguration."
- **Settlement delay** (§4.3.1): "Election markets are prone to significant settlement delay, making their Brier scores biased downward."
- **Open question** (§7): the re-anchoring "was done only for some categories viewed to likely be most affected and for which matching time data was available. Whether the same exercise would move the calibration picture for other categories with a settlement lag remains an open question."
- **Data access** (§2.1): "Kalshi publishes its trade data through a public API at no cost."
- The paper does **not** explicitly invite third parties to reproduce it. §6 describes its open questions as "an agenda for future research building on the dataset and measurement framework developed in this paper."

## How this project relates [A]

| Paper | This project (`out/numbers.md`) |
|---|---|
| 11 categories (paper naming) | 19 API series categories (e.g. `Financials` ≈ Finance, `Climate and Weather` ≈ Climate, `Science and Technology` ≈ Tech & Science) |
| 2,243,741 resolved markets, Exotics excluded | 15,280,713 settled non-combo markets. Counts are not comparable: different definitions and cutoff dates, and the paper's selection rules are not public |
| Event time vs close time, Sports + Elections only, proprietary event times | `close_time → settlement_ts` and `expected_expiration_time → settlement_ts`, every category, public fields only |
| §7 open question: which other categories have a settlement lag | Lag tables per category identify where the lag is large enough to matter. They do **not** provide true event times |
| Elections rule change, November 2025 | Bonus table: Elections close→settle p50 is 30m both before (N=894) and after (N=7,904) 2025-11-01. The paper's lag is between the event and `close_time`, which this table does not measure |

Not claimed: that this project reproduces the paper's calibration results or its event-time re-anchoring.
