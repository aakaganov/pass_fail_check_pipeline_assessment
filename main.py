import yaml
import sys
from pipeline.ingestion import run_ingestion
from pipeline.processing import run_processing
from pipeline.output import run_output

def run_pipeline(data_path=None, enabled_checks = None):
    #setup paths/defaults
    if data_path is None:
        data_path = "./data/" # Production path
    if enabled_checks is None:
        enabled_checks = {'WoW' : True}
    
    try:
        # Load config
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        #Ingestion
        aligned_df, raw_dfs = run_ingestion(data_path, config['tickers'])

        #Processing
        breach_df = run_processing(aligned_df, config, enabled_checks)

        #Output
        run_output(breach_df)

        print("pipeline complete")
        return "SUCCESS", {
            "processed_data": raw_dfs, 
            "breach_report": breach_df.to_dict('records')
        }
    except FileNotFoundError:
        return "REJECT: Missing Files", None
    except ValueError as e:
        return f"REJECT: {str(e)}", None
    except Exception as e:
        # Catch extreme volatility if raised as an exception, or other errors
        if "Extreme Volatility" in str(e):
            return "WARNING: Extreme Volatility", None
        return f"REJECT: {str(e)}", None

if __name__ == "__main__":
    # Check for sandbox argument from command line
    cmd_path = sys.argv[1] if len(sys.argv) > 1 else None
    run_pipeline(data_path=cmd_path)