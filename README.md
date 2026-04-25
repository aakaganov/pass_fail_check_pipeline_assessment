# Pass/Fail Check Pipeline

Historical US index CSVs are ingested, aligned on dates, and checked for large day-over-day (DoD) and week-over-week (WoW) moves. Breaches are written to `output/threshold_breaks.csv`. Thresholds and which checks run are controlled by `config.yaml`.

## Assumptions/Explainations

For Week over week, the definition is based on five trading rows previous on the aligned calendar, not "same weekday last calendar week". 

Dates are alligned between files because given the context of all 5 of the data sets existing in the same context, which means when we are comparing trading dates it does not make sense for those trading dates to be non uniform when they all exist in the same context. This is done despite the fact that there is not comparisons across tickers, however it leaves room for future growth to compare between tickers. Additionally the way the code is structured, it is required that the observated dates are alligned

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

Data directory (defaults to `./data`):

```bash
/tmp/pass_fail_pipeline_venv/bin/python main.py --data-dir /path/to/csv_folder
# backward-compatible positional:
/tmp/pass_fail_pipeline_venv/bin/python main.py /path/to/csv_folder
```

Other flags (see `python main.py --help`): `--config`, `--output-dir`, `--csv-path`, `--log-path`. `--output-dir` and `--csv-path` are mutually exclusive.

**Exit codes** (for CI and scripts): `0` = success, `1` = reject or unexpected error, `2` = warning (e.g. extreme volatility).

Output: by default `output/threshold_breaks.csv` and an append-only log beside it (`pipeline/output.py`).

## Configuration (`config.yaml`)

After load, the file is validated (`pipeline/config_validate.py`): required non-empty `tickers`, numeric thresholds in `(0, 1]`, only known top-level keys, and `checks` limited to `DoD` / `WoW` with boolean values. Invalid config returns `REJECT: Invalid config: …` without running ingestion.

- `tickers`: which `{TICKER}.csv` files to load from the data folder.
- `thresholds`: optional per-ticker DoD threshold (decimal, e.g. `0.015` for 1.5%). Others use `default_threshold_dod` (default 1%).
- `thresholds_wow`: optional per-ticker WoW threshold (decimal). Others use `default_threshold_wow` (default 5%).
- `default_threshold_wow`: default WoW threshold when a ticker is not listed under `thresholds_wow`.
- `checks`: `DoD` / `WoW` booleans to turn each check on or off.
- `anomaly_warning_limit`: if any DoD move exceeds this magnitude, the pipeline returns `WARNING: Extreme Volatility` and the CLI exits with code `2` (see `main.py`).

## Tests

```bash
/tmp/pass_fail_pipeline_venv/bin/python -m unittest tests.test_suite -v
```

## Disclosure

Coding assistants, specificially cursor and gemini, were used in this project. Utilized for tasks including debugging, ensuring a complete coverage of test suites, and production of this README file.  
