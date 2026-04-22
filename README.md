# Pass/Fail Check Pipeline

Historical US index CSVs are ingested, aligned on dates, and checked for large day-over-day (DoD) and week-over-week (WoW) moves. Breaches are written to `output/threshold_breaks.csv`. Thresholds and which checks run are controlled by `config.yaml`.

## Setup

Use a virtual environment (recommended; avoids PEP 668 “externally managed” errors on macOS):

```bash
cd /path/to/Pass:fail_check_pipeline
python3 -m venv /tmp/pass_fail_pipeline_venv
/tmp/pass_fail_pipeline_venv/bin/pip install -r requirements.txt
```

If the project path contains a colon (`:`), creating `.venv` inside the repo may fail; use a venv path outside the repo as above.

## Run

```bash
/tmp/pass_fail_pipeline_venv/bin/python main.py
```

Optional: pass a data directory (defaults to `./data`):

```bash
/tmp/pass_fail_pipeline_venv/bin/python main.py /path/to/csv_folder
```

Output: `output/threshold_breaks.csv`. A small append-only log line is written by `pipeline/output.py` (see that file for the log path).

## Configuration (`config.yaml`)

- `tickers`: which `{TICKER}.csv` files to load from the data folder.
- `thresholds`: optional per-ticker DoD threshold (decimal, e.g. `0.015` for 1.5%). Others use `default_threshold_dod` (default 1%).
- `default_threshold_wow`: WoW threshold (default 5%).
- `checks`: `DoD` / `WoW` booleans to turn each check on or off.
- `anomaly_warning_limit`: if any DoD move exceeds this magnitude, the pipeline returns `WARNING: Extreme Volatility` (see `main.py`).

## Tests

```bash
/tmp/pass_fail_pipeline_venv/bin/python -m unittest tests.test_suite -v
```

## Disclosure

Coding assistants (e.g. Cursor, ChatGPT) were permitted for this exercise; see comments in `tests/test_suite.py` and related notes in the repo.
