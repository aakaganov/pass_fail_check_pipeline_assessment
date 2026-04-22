import os
from datetime import datetime

def run_output(breach_df):
    # Persistence
    if not os.path.exists("./output"):
        os.makedirs("./output")
        
    output_path = "./output/threshold_breaks.csv"
    breach_df.to_csv(output_path, index=False)
    
    # Audit log
    with open("mock_test_log.txt", "a") as log:
        log.write(f"[{datetime.now()}] Pipeline complete. Breaches found: {len(breach_df)}\n")
        
    return True