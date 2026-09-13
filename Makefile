PY := .venv/bin/python

.PHONY: data numbers poll poll-once

$(PY): requirements.txt
	python3 -m venv .venv
	$(PY) -m pip install -q -r requirements.txt
	touch $(PY)

# Historical batch: crawl all settled non-MVE markets, then build data/kalshi.duckdb.
data: $(PY)
	$(PY) crawl.py
	$(PY) build.py

# Three numbers per category from data/kalshi.duckdb + the poll log → out/.
numbers: $(PY)
	$(PY) metrics.py

# Metric 3 collector; runs until stopped. Run on an always-on host.
poll: $(PY)
	$(PY) poll.py

poll-once: $(PY)
	$(PY) poll.py --once
