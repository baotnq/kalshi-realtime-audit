"""Build data/kalshi.duckdb (table `markets`) and data/anomalies.parquet from data/raw/.

This is a historical batch, not realtime. The build is derived and rebuilt from
scratch on each run; raw files are only read.

- One row per ticker. When a ticker appears more than once (live and historical,
  or a page re-read on resume), the most recently fetched copy wins.
- category = series.category, where series_ticker = the longest prefix of event_ticker
  that is a known series (docs/phase0.md §4).
- volume = volume_fp as a number (the API has no `volume` field, phase0 §3).
- record_hash = sha256(rules_primary || rules_secondary), with null read as ''.
- Invariant violations are written to anomalies and never dropped.
"""
import gzip
import hashlib
import json
import tempfile
from pathlib import Path

import duckdb

from kalshi_api import read_segments, utcnow

RAW = Path("data/raw")
damaged_segments: list[str] = []
DB = Path("data/kalshi.duckdb")
ANOMALIES = Path("data/anomalies.parquet")

FIELDS = [
    "ticker", "event_ticker", "market_type", "status", "created_time", "open_time", "close_time",
    "expected_expiration_time", "latest_expiration_time", "expiration_time", "settlement_ts",
    "settlement_timer_seconds", "result", "settlement_value_dollars", "early_close_condition",
    "can_close_early", "rules_primary", "rules_secondary", "volume_fp", "updated_time",
]


def latest_series() -> dict:
    pages = list(read_segments(RAW, "series"))
    if not pages:
        raise SystemExit("no series snapshot; run crawl.py first")
    return {s["ticker"]: s.get("category") for s in json.loads(pages[-1]["body"])["series"]}


def series_ticker_for(ticker: str, series: dict) -> str | None:
    """Longest '-'-separated prefix of an event or market ticker that is a known series.

    Series tickers can contain '-' (event KXMLBWINS-BOS-26 → series KXMLBWINS-BOS),
    so the first segment alone is not enough.
    """
    parts = ticker.split("-")
    for k in range(len(parts), 0, -1):
        candidate = "-".join(parts[:k])
        if candidate in series:
            return candidate
    return None


def flatten(tmp: Path, series: dict) -> int:
    n = 0
    with gzip.open(tmp, "wt", encoding="utf-8") as out:
        for source in ("historical", "live"):
            for page in read_segments(RAW, f"{source}_markets", on_truncated=lambda p: damaged_segments.append(str(p))):
                for m in json.loads(page["body"]).get("markets", []):
                    row = {f: m.get(f) for f in FIELDS}
                    rules = (m.get("rules_primary") or "") + (m.get("rules_secondary") or "")
                    row["record_hash"] = hashlib.sha256(rules.encode("utf-8")).hexdigest()
                    row["content_hash"] = hashlib.sha256(json.dumps(m, sort_keys=True).encode()).hexdigest()
                    row["series_ticker"] = series_ticker_for(m.get("event_ticker") or "", series)
                    row["source"] = source
                    row["fetched_at"] = page["fetched_at"]
                    row["page_sha256"] = page["sha256"]
                    out.write(json.dumps(row, ensure_ascii=False) + "\n")
                    n += 1
    return n


def main() -> None:
    ingested_at = utcnow()
    series = latest_series()
    DB.unlink(missing_ok=True)
    con = duckdb.connect(str(DB))

    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d) / "rows.jsonl.gz"
        n = flatten(tmp, series)
        print(f"flattened {n} raw market rows")
        columns = {f: "VARCHAR" for f in FIELDS + ["record_hash", "content_hash", "series_ticker", "source",
                                                   "fetched_at", "page_sha256"]}
        columns.update({"settlement_timer_seconds": "BIGINT", "can_close_early": "BOOLEAN"})
        con.execute("CREATE TEMP TABLE raw_rows AS SELECT * FROM read_json(?, format='newline_delimited', columns=?)",
                    [str(tmp), columns])

    # Damaged segment tails (crash during a write). The page was re-fetched on resume; reported, not dropped.
    con.execute("CREATE TEMP TABLE damaged(path VARCHAR)")
    if damaged_segments:  # executemany rejects an empty parameter list
        con.executemany("INSERT INTO damaged VALUES (?)", [[p] for p in damaged_segments])

    con.execute("CREATE TEMP TABLE series(series_ticker VARCHAR, category VARCHAR)")
    con.executemany("INSERT INTO series VALUES (?, ?)", list(series.items()))

    # Tickers seen with different content across copies: kept, reported.
    con.execute("""
        CREATE TEMP TABLE conflicts AS
        SELECT ticker, count(DISTINCT content_hash) AS versions, string_agg(DISTINCT source, ',') AS sources
        FROM raw_rows GROUP BY ticker HAVING count(DISTINCT content_hash) > 1
    """)

    con.execute("""
        CREATE TABLE markets AS
        WITH ranked AS (
            SELECT *, row_number() OVER (PARTITION BY ticker ORDER BY fetched_at DESC, source DESC) AS rn
            FROM raw_rows
        )
        SELECT
            r.ticker, r.event_ticker, r.series_ticker, s.category, r.market_type, r.status,
            CAST(r.created_time AS TIMESTAMPTZ) AS created_time,
            CAST(r.open_time AS TIMESTAMPTZ) AS open_time,
            CAST(r.close_time AS TIMESTAMPTZ) AS close_time,
            CAST(r.expected_expiration_time AS TIMESTAMPTZ) AS expected_expiration_time,
            CAST(r.latest_expiration_time AS TIMESTAMPTZ) AS latest_expiration_time,
            CAST(r.expiration_time AS TIMESTAMPTZ) AS expiration_time,
            CAST(r.settlement_ts AS TIMESTAMPTZ) AS settlement_ts,
            r.settlement_timer_seconds, r.result,
            CAST(r.settlement_value_dollars AS DECIMAL(10, 4)) AS settlement_value_dollars,
            r.early_close_condition, r.can_close_early, r.rules_primary, r.rules_secondary,
            CAST(r.volume_fp AS DOUBLE) AS volume,
            CAST(r.updated_time AS TIMESTAMPTZ) AS updated_time,
            r.record_hash, r.source, CAST(r.fetched_at AS TIMESTAMPTZ) AS fetched_at, r.page_sha256,
            CAST(? AS TIMESTAMPTZ) AS ingested_at
        FROM ranked r LEFT JOIN series s USING (series_ticker)
        WHERE r.rn = 1
    """, [ingested_at])

    con.execute("""
        CREATE TEMP TABLE anomalies AS
        SELECT ticker, 'created_close_settlement_order' AS check_name,
               format('created={} close={} settlement={}', created_time, close_time, settlement_ts) AS detail
        FROM markets
        WHERE settlement_ts IS NOT NULL AND NOT (created_time < close_time AND close_time <= settlement_ts)
        UNION ALL
        SELECT ticker, 'binary_result_nonbinary_value',
               format('result={} settlement_value_dollars={}', result, settlement_value_dollars)
        FROM markets
        WHERE result IN ('yes', 'no') AND settlement_value_dollars NOT IN (0, 1)
        UNION ALL
        SELECT ticker, 'missing_settlement_ts', format('status={} result={}', status, result)
        FROM markets WHERE settlement_ts IS NULL
        UNION ALL
        SELECT ticker, 'unknown_series', format('event_ticker={}', event_ticker)
        FROM markets WHERE category IS NULL
        UNION ALL
        SELECT ticker, 'duplicate_conflict', format('versions={} sources={}', versions, sources)
        FROM conflicts
        UNION ALL
        SELECT NULL, 'damaged_raw_segment_tail', format('path={}', path)
        FROM damaged
    """)
    con.execute(f"COPY (SELECT *, CAST(? AS TIMESTAMPTZ) AS ingested_at FROM anomalies ORDER BY check_name, ticker) "
                f"TO '{ANOMALIES}' (FORMAT parquet)", [ingested_at])

    total = con.execute("SELECT count(*) FROM markets").fetchone()[0]
    print(f"markets: {total} rows in {DB}")
    for source, c in con.execute("SELECT source, count(*) FROM markets GROUP BY 1 ORDER BY 1").fetchall():
        print(f"  source={source}: {c}")
    for check, c in con.execute("SELECT check_name, count(*) FROM anomalies GROUP BY 1 ORDER BY 1").fetchall():
        print(f"  anomaly {check}: {c}")
    print(f"anomalies written to {ANOMALIES}")
    con.close()


if __name__ == "__main__":
    main()
