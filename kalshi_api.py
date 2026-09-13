"""Unauthenticated client for Kalshi's public REST API, plus the raw page store.

Facts this relies on are in docs/phase0.md: no key needed, limit <= 1000,
cursor pagination, 429 without Retry-After, ~4 s per 1000-row page.
"""
import gzip
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE = "https://api.elections.kalshi.com/trade-api/v2"
HEADERS = {
    "User-Agent": "kalshi-realtime-audit/0.1",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
}
PAGE_LIMIT = 1000


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def fetch(path: str, params: dict | None = None, max_attempts: int = 10) -> tuple[str, bytes]:
    """GET a path and return (url, decoded body bytes).

    Retries 429, 5xx and network errors with exponential backoff (phase0 §1);
    other HTTP errors are raised immediately.
    """
    url = f"{BASE}/{path}"
    query = {k: v for k, v in (params or {}).items() if v is not None}
    if query:
        url += "?" + urllib.parse.urlencode(query)
    err: Exception | None = None
    for attempt in range(max_attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=120) as r:
                body = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                return url, body
        except urllib.error.HTTPError as e:
            if e.code != 429 and e.code < 500:
                raise
            err = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        time.sleep(min(60, 2**attempt))
    raise RuntimeError(f"giving up on {url}: {err}")


def paginate(path: str, params: dict, cursor: str | None = None, key: str = "markets"):
    """Yield (url, body, data, next_cursor) per page until the cursor runs out."""
    while True:
        url, body = fetch(path, {**params, "limit": PAGE_LIMIT, "cursor": cursor})
        data = json.loads(body)
        next_cursor = data.get("cursor") or None
        yield url, body, data, next_cursor
        if not next_cursor or not data.get(key):
            return
        cursor = next_cursor


def append_page(path: Path, url: str, body: bytes, fetched_at: str, **extra) -> None:
    """Append one response, verbatim, as one line in its own gzip member.

    One member per page means a crash can only truncate the last page, which
    read_pages() skips. The file is never rewritten.
    """
    record = {
        "fetched_at": fetched_at,
        "url": url,
        "sha256": hashlib.sha256(body).hexdigest(),
        **extra,
        "body": body.decode("utf-8"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "ab") as f:
        f.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))


def read_pages(path: Path):
    """Yield stored page records; stop quietly at a truncated tail."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)
    except (EOFError, gzip.BadGzipFile, json.JSONDecodeError):
        print(f"warning: truncated tail in {path}, remaining bytes ignored")


def write_json_atomic(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)
