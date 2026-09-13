"""Unauthenticated client for Kalshi's public REST API, plus the crash-safe raw store.

Facts this relies on are in docs/phase0.md: no key needed, limit <= 1000,
cursor pagination, 429 without Retry-After, ~2–4 s per 1000-row page.
"""
import gzip
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
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
    other HTTP errors are raised immediately as urllib.error.HTTPError.
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


# --- raw store -----------------------------------------------------------------
#
# A stream `stem` in `directory` is a sequence of segments:
#   <stem>.jsonl.gz, <stem>.0001.jsonl.gz, <stem>.0002.jsonl.gz, ...
# Each append is one JSON line in its own gzip member, so a crash can only damage the
# last member of the segment being written. A damaged segment is never appended to
# again (appending after a truncated member would hide everything written later);
# writing continues in a new segment, and old bytes are never modified.


def segments(directory: Path, stem: str) -> list[Path]:
    base = directory / f"{stem}.jsonl.gz"
    numbered = sorted(directory.glob(f"{stem}.[0-9][0-9][0-9][0-9].jsonl.gz"))
    return ([base] if base.exists() else []) + numbered


def gzip_intact(path: Path) -> bool:
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1 << 20):
                pass
        return True
    except (EOFError, gzip.BadGzipFile, zlib.error, OSError):
        return False


class SegmentWriter:
    """Append-only writer for one raw stream.

    `slot` is a dict the caller persists in its own state file, holding the active
    segment path and its size after the last committed write. On open, the active
    segment is reused only if its size equals the committed size. Without a
    committed size (older state), a full gzip check is used instead. Any mismatch
    (a crash during or right after a write) starts a new segment. The caller
    commits by saving its state after append().
    """

    def __init__(self, directory: Path, stem: str, slot: dict):
        self.slot = slot
        existing = segments(directory, stem)
        path = Path(slot["path"]) if slot.get("path") else (existing[-1] if existing else None)
        if path is not None and path.exists() and not self._clean(path):
            n = 1 + max([int(p.name.split(".")[-3]) for p in existing if p.name.count(".") == 3] or [0])
            print(f"warning: {path} has an uncommitted or truncated tail; continuing in segment {n:04d}",
                  flush=True)
            path = directory / f"{stem}.{n:04d}.jsonl.gz"
        self.path = path or directory / f"{stem}.jsonl.gz"
        slot["path"] = str(self.path)
        slot["size"] = self.path.stat().st_size if self.path.exists() else 0

    def _clean(self, path: Path) -> bool:
        if self.slot.get("path") == str(path) and "size" in self.slot:
            return path.stat().st_size == self.slot["size"]
        return gzip_intact(path)

    def append(self, obj: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(self.path, "ab") as f:
            f.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
        self.slot["size"] = self.path.stat().st_size


def append_page(writer: SegmentWriter, url: str, body: bytes, fetched_at: str, **extra) -> None:
    """Append one API response, verbatim, with its URL, fetch time and sha256."""
    writer.append({
        "fetched_at": fetched_at,
        "url": url,
        "sha256": hashlib.sha256(body).hexdigest(),
        **extra,
        "body": body.decode("utf-8"),
    })


def read_pages(path: Path, on_truncated=None):
    """Yield stored records from one segment; stop at a damaged tail and report it."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)
    except (EOFError, gzip.BadGzipFile, zlib.error, json.JSONDecodeError):
        print(f"warning: damaged tail in {path}; the rest of this segment is ignored", flush=True)
        if on_truncated:
            on_truncated(path)


def read_segments(directory: Path, stem: str, on_truncated=None):
    for path in segments(directory, stem):
        yield from read_pages(path, on_truncated)


def write_json_atomic(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)
