"""Restart-safety tests: crash mid-write, SIGKILL/SIGTERM resume, ignored cursor, cutoff move, poller restart.

Runs against a temp copy of the code and hits the live API with a few pages; never touches data/.
Usage: .venv/bin/python tests/restart_test.py
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PY = str(REPO / ".venv/bin/python")
T = Path(tempfile.mkdtemp(prefix="kra-restart-"))
for f in REPO.glob("*.py"):
    shutil.copy(f, T)
os.chdir(T)
sys.path.insert(0, str(T))
from kalshi_api import SegmentWriter, gzip_intact, read_segments, segments  # noqa: E402

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail else ""), flush=True)


def run(*args, timeout=900):
    p = subprocess.run([PY, *args], capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr


def state():
    return json.loads(Path("data/state/crawl.json").read_text())


def save_state(s):
    Path("data/state/crawl.json").write_text(json.dumps(s))


# (a) SegmentWriter: crash in the middle of a write
d = T / "unit"
slot = {}
w = SegmentWriter(d, "x", slot)
w.append({"i": 1}); w.append({"i": 2})
committed = dict(slot)                       # what the caller had saved to its state file
w.append({"i": 3})
p = Path(slot["path"])
p.write_bytes(p.read_bytes()[:committed["size"] + 12])   # 3rd member cut mid-write
w2 = SegmentWriter(d, "x", dict(committed))
w2.append({"i": 4})
damaged = []
got = [r["i"] for r in read_segments(d, "x", on_truncated=damaged.append)]
check("a1 damaged segment is not appended to; new segment created", w2.path.name == "x.0001.jsonl.gz", w2.path.name)
check("a2 all committed records + records after restart are readable", got == [1, 2, 4], got)
check("a3 damaged tail reported", len(damaged) == 1)
w3 = SegmentWriter(d, "x", {})               # legacy state without a committed size → full gzip check
check("a4 legacy state: intact last segment is reused", w3.path.name == "x.0001.jsonl.gz", w3.path.name)
check("a5 gzip_intact detects the cut file", not gzip_intact(d / "x.jsonl.gz"))

# (a') build on clean raw data: no damaged segment must not break the anomaly step
rc, out = run("crawl.py", "--max-pages", "1")
check("a6 crawl one page", rc == 0, out[-200:])
rc, out = run("build.py")
check("a7 build succeeds when no segment is damaged", rc == 0 and "damaged_raw_segment_tail" not in out, out[-300:])

# (b) crawl killed with SIGKILL, plus a cut tail, then resume and build
proc = subprocess.Popen([PY, "crawl.py", "--max-pages", "6"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
deadline = time.time() + 300
while time.time() < deadline:
    try:
        if state().get("historical", {}).get("pages", 0) >= 2:
            break
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    time.sleep(0.5)
proc.send_signal(signal.SIGKILL); proc.wait()
s = state()
hist_path = Path(s["segments"]["historical_markets"]["path"])
with open(hist_path, "ab") as f:
    f.write(b"\x1f\x8b\x08\x00garbage-partial-member")   # simulate power loss during the next write
rc, out = run("crawl.py", "--max-pages", "2")
s2 = state()
check("b1 resume after SIGKILL exits 0", rc == 0, out[-300:])
check("b2 resume continued in a new historical segment", Path(s2["segments"]["historical_markets"]["path"]) != hist_path,
      s2["segments"]["historical_markets"]["path"])
check("b3 cursor progressed (pages grew)", s2["historical"]["pages"] > s["historical"]["pages"],
      f"{s['historical']['pages']} -> {s2['historical']['pages']}")
rc, out = run("build.py")
check("b4 build succeeds", rc == 0, out[-300:])
n_pages = sum(1 for _ in read_segments(Path("data/raw"), "historical_markets")) + \
    sum(1 for _ in read_segments(Path("data/raw"), "live_markets"))
import re  # noqa: E402
m = re.search(r"damaged_raw_segment_tail: (\d+)", out)
check("b5 build reports the damaged tail(s)", m and int(m.group(1)) >= 1, out[-400:])
check("b6 build read every intact page (rows = pages x 1000, before dedupe)", f"flattened {n_pages * 1000} raw" in out,
      f"pages={n_pages}")

# (c) SIGTERM stops cleanly with intact files
proc = subprocess.Popen([PY, "crawl.py", "--max-pages", "50"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(12)
proc.send_signal(signal.SIGTERM)
out, _ = proc.communicate(timeout=300)
s3 = state()
intact = all(gzip_intact(Path(v["path"])) and Path(v["path"]).stat().st_size == v["size"]
             for v in s3["segments"].values())
check("c1 SIGTERM: stop message and clean exit", proc.returncode == 0 and "stop requested" in out, out[-300:])
check("c2 SIGTERM: every active segment intact and equal to committed size", intact)

# (d) unusable cursor: API returns first page with HTTP 200
s3["live"]["cursor"] = "garbage"; s3["historical"]["cursor"] = "garbage"
save_state(s3)
rc, out = run("crawl.py", "--max-pages", "1")
ev = [e["event"] for e in state().get("events", [])]
check("d1 ignored cursor detected and logged for both streams",
      sum("cursor ignored" in e for e in ev) == 2, ev)

# (e) cutoff moved since the crawl started → historical re-crawled
s4 = state(); s4["cutoff_ts"] -= 86400; save_state(s4)
rc, out = run("crawl.py", "--max-pages", "1")
s5 = state()
check("e1 cutoff move logged", any("cutoff moved" in e["event"] for e in s5["events"]))
check("e2 historical stream restarted", s5["historical"]["pages"] == 1, s5["historical"])

# (f) poller: cut changes segment and a cut sweeps.jsonl line, then restart; metrics runs
rc, out = run("poll.py", "--once")
check("f1 first sweep ok", rc == 0 and '"error": null' in out, out[-200:])
ps = json.loads(Path("data/state/poll_last.json").read_text())
day_stem = next(k for k in ps["segments"] if k.startswith("changes-"))
cp = Path(ps["segments"][day_stem]["path"])
with open(cp, "ab") as f:
    f.write(b"\x1f\x8b\x08\x00cut")
with open("data/raw/poll/sweeps.jsonl", "a") as f:
    f.write('{"sweep_id": "cut-mid-li')
rc, out = run("poll.py", "--once")
ps2 = json.loads(Path("data/state/poll_last.json").read_text())
check("f2 poller restart continues in a new changes segment", ps2["segments"][day_stem]["path"] != str(cp),
      ps2["segments"][day_stem]["path"])
lines = Path("data/raw/poll/sweeps.jsonl").read_text().splitlines()
check("f3 cut sweeps line terminated; newest sweep line is valid JSON", json.loads(lines[-1])["error"] is None)
rc, out = run("metrics.py")
check("f4 metrics runs and reports crawl events + cut log line",
      rc == 0 and "Crawl events" in out and "1 log lines cut by a crash" in out, out[-500:] if rc else "")

print(f"\n{sum(ok for _, ok in results)}/{len(results)} passed; temp dir {T}")
sys.exit(0 if all(ok for _, ok in results) else 1)
