"""CLI entrypoint for the index pass/fail pipeline.

Loads config.yaml next to this module (via __file__, not the shell's current
working directory), merges YAML checks with optional enabled_checks overrides,
then runs ingestion, processing, and CSV output.

run_pipeline returns (status, data). On success, data contains
processed_data and breach_report; on failure, data is None and
status is a stable REJECT: / WARNING: string.

When run as python main.py, an optional first argument is the path to the folder
holding {TICKER}.csv files (defaults to ./data / ./data/ when unset).
"""
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
        
    # Make config loading independent of current working directory
    config_path = Path(__file__).resolve().parent / "config.yaml"
    #solution found through debugging with cursor

    try:
        #open config file and load it to variable config for parsing
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) 
        #load checks from config file
        checks_cfg = config.get("checks") or {}
        merged_checks = {
            "DoD": bool(checks_cfg.get("DoD", True)),
            "WoW": bool(checks_cfg.get("WoW", True)),
        }
        #overrides merged_checks with enabled_checks if enabled_checks is not None
        if enabled_checks is not None:
            merged_checks.update(enabled_checks)
        #run ingestion
        aligned_df, raw_dfs = run_ingestion(data_path, config["tickers"])
        #run processing
        breach_df = run_processing(aligned_df, config, merged_checks)
        #run output 
        run_output(breach_df)
        #return success and data
        return "SUCCESS", {
            "processed_data": raw_dfs,
            "breach_report": breach_df.to_dict("records"),
        }
    #catch file not found error
    except FileNotFoundError:
        return "REJECT: Missing Files", None
    #catch value error
    except ValueError as e:
        msg = str(e)
        # Normalize to exact strings expected by tests
        if "Invalid Price" in msg:
            return "REJECT: Invalid Price", None
        if "Header mismatch" in msg:
            return "REJECT: Header mismatch", None
        # Avoid "REJECT: REJECT: ..."
        if msg.startswith("REJECT:"):
            return msg, None
        return f"REJECT: {msg}", None
    #catch general exception
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