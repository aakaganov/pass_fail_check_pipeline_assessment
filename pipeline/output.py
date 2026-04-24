"""Persist pipeline breach results and a minimal run log.

By default writes ``output/threshold_breaks.csv`` under the **repository root**
(the directory that contains ``main.py``). Optional ``output_dir`` /
``csv_path`` override that. Appends one line to the run log next to the CSV
(default ``output/mock_test_log.txt``).

``pipeline/`` is one level below the repo root, so the root is resolved from
``Path(__file__).resolve().parent.parent``.
"""

from datetime import datetime
from pathlib import Path

# Repo root = parent of ``pipeline/`` (this file lives in ``pipeline/``).
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_OUTPUT_DIR = _REPO_ROOT / "output"
_DEFAULT_CSV_PATH = _DEFAULT_OUTPUT_DIR / "threshold_breaks.csv"
_DEFAULT_LOG_PATH = _DEFAULT_OUTPUT_DIR / "mock_test_log.txt"


def run_output(breach_df, *, output_dir=None, csv_path=None, log_path=None):
    """Write breach_df to CSV and append a short audit log line.

    If ``csv_path`` is set, the CSV is written there (parents created). The log
    defaults to ``<csv_dir>/mock_test_log.txt`` unless ``log_path`` is given.

    If only ``output_dir`` is set, uses ``threshold_breaks.csv`` and
    ``mock_test_log.txt`` inside that directory.

    With all arguments omitted, uses ``output/`` under the repo root.
    """
    if csv_path is not None:
        csv_file = Path(csv_path)
        log_file = Path(log_path) if log_path is not None else csv_file.parent / "mock_test_log.txt"
    elif output_dir is not None:
        out = Path(output_dir)
        csv_file = out / "threshold_breaks.csv"
        log_file = Path(log_path) if log_path is not None else out / "mock_test_log.txt"
    else:
        csv_file = _DEFAULT_CSV_PATH
        log_file = Path(log_path) if log_path is not None else _DEFAULT_LOG_PATH

    csv_file.parent.mkdir(parents=True, exist_ok=True)
    breach_df.to_csv(csv_file, index=False)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as log:
        log.write(
            f"[{datetime.now()}] Pipeline complete. Breaches found: {len(breach_df)}\n"
        )

    return True