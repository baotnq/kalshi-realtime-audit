"""Compute the three numbers per category → out/*.csv, out/*.png, out/numbers.md.

Metrics 1–2 read data/kalshi.duckdb, a historical batch built from data/raw/.
Metric 3 reads the poll log under data/raw/poll/. It is forward-only: the API has
no status history (docs/phase0.md §5).
Every table carries N. numbers.md states what was measured, without interpretation.
"""
import csv
import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from build import latest_series, series_ticker_for  # noqa: E402
from kalshi_api import read_pages, utcnow  # noqa: E402

DB = Path("data/kalshi.duckdb")
POLL = Path("data/raw/poll")
CRAWL_STATE = Path("data/state/crawl.json")
OUT = Path("out")
POLL_INTERVAL_S = 300
RULE_CHANGE = "2025-11-01"

SURFACE, INK, INK_2, GRID, BAR = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6"


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def fmt_s(s) -> str:
    if s is None:
        return "–"
    a = abs(s)
    v = f"{a:.0f}s" if a < 120 else f"{a / 60:.0f}m" if a < 7200 else f"{a / 3600:.1f}h" if a < 172800 else f"{a / 86400:.1f}d"
    return ("-" if s < 0 else "") + v


def write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["category"])
        w.writeheader()
        w.writerows(rows)


def md_table(rows: list[dict], cols: list[tuple[str, str, callable]]) -> str:
    head = "| " + " | ".join(c[1] for c in cols) + " |\n|" + "---|" * len(cols) + "\n"
    return head + "".join("| " + " | ".join(c[2](r[c[0]]) for c in cols) + " |\n" for r in rows)


def bar_chart(path: Path, labels: list[str], values: list[float], tip: list[str], title: str, xlabel: str,
              log: bool = False) -> None:
    fig, ax = plt.subplots(figsize=(9, 0.42 * len(labels) + 1.4), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    y = range(len(labels))
    ax.barh(y, values, height=0.55, color=BAR, edgecolor=SURFACE, linewidth=2)
    ax.set_yticks(list(y), labels, color=INK, fontsize=9)
    ax.invert_yaxis()
    if log:
        ax.set_xscale("log")
    for i, (v, t) in enumerate(zip(values, tip)):
        ax.annotate(t, (v, i), xytext=(4, 0), textcoords="offset points", va="center", fontsize=8, color=INK_2)
    ax.set_title(title, loc="left", color=INK, fontsize=11)
    ax.set_xlabel(xlabel, color=INK_2, fontsize=9)
    ax.tick_params(axis="x", colors=INK_2, labelsize=8)
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.margins(x=0.25)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def lag(con) -> list[dict]:
    q = """
        SELECT coalesce(category, '(unknown series)') AS category, count(*) AS n,
               quantile_cont(epoch(settlement_ts - close_time), 0.5) AS close_to_settle_p50_s,
               quantile_cont(epoch(settlement_ts - close_time), 0.9) AS close_to_settle_p90_s,
               quantile_cont(epoch(settlement_ts - close_time), 0.99) AS close_to_settle_p99_s,
               quantile_cont(epoch(settlement_ts - expected_expiration_time), 0.5) AS expected_exp_to_settle_p50_s,
               quantile_cont(epoch(settlement_ts - expected_expiration_time), 0.9) AS expected_exp_to_settle_p90_s,
               avg(CAST(settlement_ts < expected_expiration_time AS INT)) AS share_settled_before_expected_exp
        FROM markets WHERE settlement_ts IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
    """
    return [dict(zip([d[0] for d in con.description], r)) for r in con.execute(q).fetchall()]


def elections_rule_change(con) -> list[dict]:
    q = f"""
        SELECT CASE WHEN close_time < TIMESTAMPTZ '{RULE_CHANGE}' THEN 'close before {RULE_CHANGE}'
                    ELSE 'close on/after {RULE_CHANGE}' END AS period,
               count(*) AS n,
               quantile_cont(epoch(settlement_ts - close_time), 0.5) AS close_to_settle_p50_s,
               quantile_cont(epoch(settlement_ts - close_time), 0.9) AS close_to_settle_p90_s,
               quantile_cont(epoch(settlement_ts - expected_expiration_time), 0.5) AS expected_exp_to_settle_p50_s
        FROM markets WHERE category = 'Elections' AND settlement_ts IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """
    return [dict(zip([d[0] for d in con.description], r)) for r in con.execute(q).fetchall()]


def nonbinary(con) -> list[dict]:
    q = """
        SELECT coalesce(category, '(unknown series)') AS category, count(*) AS n,
               count(*) FILTER (WHERE result = 'scalar' OR settlement_value_dollars NOT IN (0, 1)) AS n_nonbinary,
               count(*) FILTER (WHERE result = 'scalar') AS n_result_scalar,
               count(*) FILTER (WHERE result NOT IN ('yes', 'no', 'scalar') OR result IS NULL) AS n_result_other,
               n_nonbinary / n AS share_nonbinary,
               sum(volume) AS volume_total,
               coalesce(sum(volume) FILTER (WHERE result = 'scalar' OR settlement_value_dollars NOT IN (0, 1)), 0)
                   AS volume_nonbinary,
               volume_nonbinary / nullif(volume_total, 0) AS volume_share_nonbinary
        FROM markets GROUP BY 1 ORDER BY n DESC
    """
    return [dict(zip([d[0] for d in con.description], r)) for r in con.execute(q).fetchall()]


def poll_coverage() -> dict:
    sweeps, damaged = [], 0
    if (POLL / "sweeps.jsonl").exists():
        for line in open(POLL / "sweeps.jsonl"):
            try:
                sweeps.append(json.loads(line))
            except json.JSONDecodeError:  # a line cut by a crash; poll.py terminates it on restart
                damaged += 1
    ok = [s for s in sweeps if s["error"] is None]
    if not ok:
        return {"sweeps_ok": 0, "sweeps_failed": len(sweeps), "sweeps_damaged": damaged}
    starts = sorted(ts(s["started_at"]) for s in ok)
    gaps = [(b - a).total_seconds() for a, b in zip(starts, starts[1:]) if (b - a).total_seconds() > 2 * POLL_INTERVAL_S]
    return {"sweeps_ok": len(ok), "sweeps_failed": len(sweeps) - len(ok), "sweeps_damaged": damaged,
            "first": ok[0]["started_at"],
            "last": ok[-1]["finished_at"], "span_days": (ts(ok[-1]["finished_at"]) - starts[0]).total_seconds() / 86400,
            "gaps": len(gaps), "gap_hours": sum(gaps) / 3600}


def dispute(con, series: dict) -> list[dict]:
    obs = defaultdict(list)  # ticker -> [(observed_at, status, result)], status None = left the closed set
    # A sweep interrupted by a crash is repeated on restart, so an observation can appear twice; sets below absorb it.
    for path in sorted(POLL.glob("changes-*.jsonl.gz")):
        for r in read_pages(path):
            m = r["market"]
            obs[r["ticker"]].append((r["observed_at"], m and m.get("status"), m and m.get("result")))
    final = dict(con.execute("SELECT ticker, result FROM markets").fetchall())

    per = defaultdict(lambda: {"n_observed": 0, "n_disputed": 0, "n_amended": 0, "n_result_changed": 0,
                               "disputed_s": [], "amended_s": [], "n_still_in_review": 0})
    for ticker, events in obs.items():
        events = sorted(set(events))
        c = per[series.get(series_ticker_for(ticker, series)) or "(unknown series)"]
        c["n_observed"] += 1
        statuses = {s for _, s, _ in events}
        c["n_disputed"] += "disputed" in statuses
        c["n_amended"] += "amended" in statuses
        for state in ("disputed", "amended"):
            for i, (t0, s, _) in enumerate(events):
                if s != state or (i and events[i - 1][1] == state):
                    continue
                end = next((t for t, s2, _ in events[i + 1:] if s2 != state), None)
                if end is None:
                    c["n_still_in_review"] += 1
                else:
                    c[f"{state}_s"].append((ts(end) - ts(t0)).total_seconds())
        results = [r for _, _, r in events if r]
        if final.get(ticker):
            results.append(final[ticker])
        c["n_result_changed"] += len(set(results)) > 1

    rows = []
    for category, c in sorted(per.items(), key=lambda kv: -kv[1]["n_observed"]):
        rows.append({"category": category, "n_observed": c["n_observed"], "n_disputed": c["n_disputed"],
                     "n_amended": c["n_amended"], "n_result_changed": c["n_result_changed"],
                     "n_still_in_review": c["n_still_in_review"],
                     "disputed_median_s": statistics.median(c["disputed_s"]) if c["disputed_s"] else None,
                     "amended_median_s": statistics.median(c["amended_s"]) if c["amended_s"] else None})
    return rows


def main() -> None:
    OUT.mkdir(exist_ok=True)
    con = duckdb.connect(str(DB), read_only=True)
    series = latest_series()
    total, lo, hi, ingested = con.execute(
        "SELECT count(*), strftime(min(settlement_ts) AT TIME ZONE 'UTC', '%Y-%m-%d'), "
        "strftime(max(settlement_ts) AT TIME ZONE 'UTC', '%Y-%m-%d'), "
        "strftime(max(ingested_at) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%MZ') FROM markets").fetchone()
    crawl = json.loads(CRAWL_STATE.read_text()) if CRAWL_STATE.exists() else {}
    complete = all(crawl.get(k, {}).get("done") for k in ("historical", "live"))

    lag_rows, nb_rows, el_rows = lag(con), nonbinary(con), elections_rule_change(con)
    dsp_rows, cov = dispute(con, series), poll_coverage()
    write_csv(OUT / "lag_by_category.csv", lag_rows)
    write_csv(OUT / "nonbinary_by_category.csv", nb_rows)
    write_csv(OUT / "dispute_by_category.csv", dsp_rows)
    write_csv(OUT / "elections_lag_rule_change.csv", el_rows)

    bar_chart(OUT / "lag_by_category.png", [r["category"] for r in lag_rows],
              [max(r["close_to_settle_p50_s"], 1) for r in lag_rows],
              [f"p50 {fmt_s(r['close_to_settle_p50_s'])} · p90 {fmt_s(r['close_to_settle_p90_s'])} · N={r['n']:,}"
               for r in lag_rows],
              "Settlement lag: close_time → settlement_ts, median by category",
              "seconds (log scale)", log=True)
    bar_chart(OUT / "nonbinary_by_category.png", [r["category"] for r in nb_rows],
              [100 * r["share_nonbinary"] for r in nb_rows],
              [f"{100 * r['share_nonbinary']:.2f}% · {r['n_nonbinary']:,} of N={r['n']:,}" for r in nb_rows],
              "Non-binary settlement: share of settled markets", "% of markets")
    bar_chart(OUT / "dispute_by_category.png", [r["category"] for r in dsp_rows],
              [r["n_observed"] for r in dsp_rows],
              [f"disputed {r['n_disputed']} · amended {r['n_amended']} · N={r['n_observed']:,}" for r in dsp_rows],
              f"Dispute trail: closed markets observed by polling ({cov.get('span_days', 0):.1f} days)",
              "markets observed (N)")

    partial = "" if complete else (
        "> **Partial dataset.** The crawl had not finished when these numbers were generated "
        f"(historical done={crawl.get('historical', {}).get('done')}, live done={crawl.get('live', {}).get('done')}). "
        "Numbers will change.\n\n")
    events = crawl.get("events", [])
    if events:
        partial += "Crawl events (restarts, cutoff moves), from `data/state/crawl.json`:\n\n" + "".join(
            f"- {e['at'][:19]}Z: {e['event']}\n" for e in events) + "\n"
    md = f"""# Kalshi Realtime Audit — numbers

**Historical batch (Phases 1–2), not realtime.** Generated {utcnow()[:16]}Z from `data/kalshi.duckdb` (built {ingested}).
Source: Kalshi public REST API, no credentials. Assumptions: `docs/phase0.md`.

{partial}Dataset: **{total:,} settled markets**, settlement dates {lo} → {hi}. Multivariate combo markets (MVE) are **excluded**
at crawl time (`mve_filter=exclude`). On 2026-09-10 they settled at ≥ 60,000 per sampled hour versus 2,700–4,500 non-combo
(phase0 §6). Category = the market's series category (`event_ticker` prefix → `GET /series`).

## 1. Settlement lag

{md_table(lag_rows, [("category", "Category", str), ("n", "N", lambda v: f"{v:,}"),
                     ("close_to_settle_p50_s", "close→settle p50", fmt_s),
                     ("close_to_settle_p90_s", "p90", fmt_s), ("close_to_settle_p99_s", "p99", fmt_s),
                     ("expected_exp_to_settle_p50_s", "expected exp.→settle p50", fmt_s),
                     ("expected_exp_to_settle_p90_s", "p90", fmt_s),
                     ("share_settled_before_expected_exp", "settled before expected exp.", lambda v: f"{100 * v:.1f}%")])}
Measured: per market, `settlement_ts − close_time` and `settlement_ts − expected_expiration_time`, in seconds, over all
settled markets with a `settlement_ts`. Quantiles are continuous. Negative values mean settlement came before that
timestamp. `close_time` is the final value returned by the API, including early-close updates. Full precision:
`out/lag_by_category.csv`.

### Elections, before vs after {RULE_CHANGE}

{md_table(el_rows, [("period", "Period (by close_time)", str), ("n", "N", lambda v: f"{v:,}"),
                    ("close_to_settle_p50_s", "close→settle p50", fmt_s), ("close_to_settle_p90_s", "p90", fmt_s),
                    ("expected_exp_to_settle_p50_s", "expected exp.→settle p50", fmt_s)])}
Measured: the same lag for category Elections, split by whether `close_time` is before {RULE_CHANGE}.

## 2. Non-binary settlement

{md_table(nb_rows, [("category", "Category", str), ("n", "N", lambda v: f"{v:,}"),
                    ("n_nonbinary", "non-binary", lambda v: f"{v:,}"),
                    ("share_nonbinary", "share", lambda v: f"{100 * v:.2f}%"),
                    ("n_result_other", "result not yes/no/scalar", lambda v: f"{v:,}"),
                    ("volume_share_nonbinary", "share of volume", lambda v: "–" if v is None else f"{100 * v:.2f}%")])}
Measured: a market counts as non-binary when `result = scalar` or `settlement_value_dollars` ∉ {{0, 1}}. Volume is
`volume_fp` (contracts) summed per group. The "result not yes/no/scalar" column counts other or empty `result` values,
which are also counted in N. Full precision: `out/nonbinary_by_category.csv`.

## 3. Dispute trail (forward-only)

Poll coverage: {cov.get('sweeps_ok', 0)} successful sweeps, {cov.get('sweeps_failed', 0)} failed,
{cov.get('sweeps_damaged', 0)} log lines cut by a crash,
{cov.get('first', '–')} → {cov.get('last', '–')} ({cov.get('span_days', 0):.1f} days), {cov.get('gaps', 0)} gaps longer than
{2 * POLL_INTERVAL_S // 60} min totalling {cov.get('gap_hours', 0):.1f} h.

{md_table(dsp_rows, [("category", "Category", str), ("n_observed", "N observed", lambda v: f"{v:,}"),
                     ("n_disputed", "disputed", str), ("n_amended", "amended", str),
                     ("n_result_changed", "result changed", str), ("n_still_in_review", "still in review", str),
                     ("disputed_median_s", "median time disputed", fmt_s),
                     ("amended_median_s", "median time amended", fmt_s)])}
Measured: `GET /markets?status=closed&mve_filter=exclude` polled every {POLL_INTERVAL_S // 60} min since the first sweep.
N observed = distinct markets seen at least once in `closed`/`determined`/`disputed`/`amended`. A market counts as
disputed or amended if any sweep saw it in that status. Time in a status runs from the first sweep that saw it to the
first later sweep that did not, so it is accurate to ± one poll interval, and a status shorter than the interval can be
missed. Result changed = more than one distinct non-empty `result` across sweeps and the final settled record. The
Kalshi API exposes no status history, so none of this exists before the first sweep.
"""
    (OUT / "numbers.md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
