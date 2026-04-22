from pathlib import Path
import yaml
import sys
from pipeline.ingestion import run_ingestion
from pipeline.processing import run_processing
from pipeline.output import run_output

def run_pipeline(data_path="./data", enabled_checks = None):
    #setup paths/defaults
    if data_path is None:
        data_path = "./data/" # Production path
    if enabled_checks is None:
        enabled_checks = {'WoW' : True}
        
    # Make config loading independent of current working directory
    config_path = Path(__file__).resolve().parent / "config.yaml"

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        aligned_df, raw_dfs = run_ingestion(data_path, config["tickers"])
        breach_df = run_processing(aligned_df, config, enabled_checks)
        run_output(breach_df)
        return "SUCCESS", {
            "processed_data": raw_dfs,
            "breach_report": breach_df.to_dict("records"),
        }
    except FileNotFoundError:
        return "REJECT: Missing Files", None
    except ValueError as e:
        msg = str(e)
        # Normalize to exact strings expected by your tests
        if "Invalid Price" in msg:
            return "REJECT: Invalid Price", None
        if "Header mismatch" in msg:
            return "REJECT: Header mismatch", None
        # Avoid "REJECT: REJECT: ..."
        if msg.startswith("REJECT:"):
            return msg, None
        return f"REJECT: {msg}", None
    except Exception as e:
        msg = str(e)
        if "Extreme Volatility" in msg:
            return "WARNING: Extreme Volatility", None
        # Pass through preformatted statuses from lower layers
        if msg.startswith("REJECT:") or msg.startswith("WARNING:"):
            return msg, None
        return f"REJECT: {msg}", None
if __name__ == "__main__":
    cmd_path = sys.argv[1] if len(sys.argv) > 1 else None
    status, data = run_pipeline(data_path=cmd_path)
    print(status)
    if data:
        print(f"Breaches: {len(data['breach_report'])}")