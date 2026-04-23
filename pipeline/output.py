"""Persist pipeline breach results and a minimal run log.

Writes ``./output/threshold_breaks.csv`` (relative to the process current working
directory) from the breach DataFrame. Appends one line to ``mock_test_log.txt``
in the cwd with a timestamp and breach count.

Paths are not anchored to __file__; run from the project root (or pass
absolute paths if this module is extended later).
"""

import os
from datetime import datetime


def run_output(breach_df):
    """Write breach_df to CSV and append a short audit log line.

    Creates ./output if missing. Returns True when finished (callers
    may ignore the return value).
    """
    # Write breach report to csv
    if not os.path.exists("./output"):
        os.makedirs("./output")
        
    output_path = "./output/threshold_breaks.csv"
    breach_df.to_csv(output_path, index=False)
    
    # Audit log
    with open("mock_test_log.txt", "a") as log:
        log.write(f"[{datetime.now()}] Pipeline complete. Breaches found: {len(breach_df)}\n")
        
    return True