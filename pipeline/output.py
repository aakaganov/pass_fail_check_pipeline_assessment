"""Persist pipeline breach results and a minimal run log.

Writes ``output/threshold_breaks.csv`` under the **repository root** (the
directory that contains ``main.py``), not the shell's current working directory.
Appends one line to ``output/mock_test_log.txt`` in the same folder.

``pipeline/`` is one level below the repo root, so the root is resolved from
``Path(__file__).resolve().parent.parent``.
"""

from datetime import datetime
from pathlib import Path

# Repo root = parent of ``pipeline/`` (this file lives in ``pipeline/``).
_REPO_ROOT = Path(__file__).resolve().parent.parent
_OUTPUT_DIR = _REPO_ROOT / "output"
_CSV_PATH = _OUTPUT_DIR / "threshold_breaks.csv"
_LOG_PATH = _OUTPUT_DIR / "mock_test_log.txt"


def run_output(breach_df):
    """Write breach_df to CSV and append a short audit log line.

    Creates ``output/`` under the repo root if missing. Returns True when
    finished (callers may ignore the return value).
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    breach_df.to_csv(_CSV_PATH, index=False)

    with open(_LOG_PATH, "a", encoding="utf-8") as log:
        log.write(
            f"[{datetime.now()}] Pipeline complete. Breaches found: {len(breach_df)}\n"
        )

    return True