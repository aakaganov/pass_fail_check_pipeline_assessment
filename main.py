"""CLI entrypoint for the index pass/fail pipeline.

Loads config (default ``config.yaml`` next to this file), merges YAML checks
with optional ``enabled_checks`` overrides, then runs ingestion, processing,
and CSV output.

``run_pipeline`` returns ``(status, data)``. On success, ``data`` contains
``processed_data`` and ``breach_report``; on failure, ``data`` is ``None`` and
``status`` is a stable ``REJECT:`` / ``WARNING:`` string.

CLI exit codes (for shells and CI): 0 success, 1 reject / error, 2 warning.
Use ``python main.py --help`` for flags.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from pipeline.ingestion import run_ingestion
from pipeline.output import run_output
from pipeline.processing import run_processing


def exit_code_for_status(status: str) -> int:
    """Map pipeline status to a process exit code for automation (CI, cron)."""
    if status == "SUCCESS":
        return 0
    if status.startswith("WARNING:"):
        return 2
    return 1


def run_pipeline(
    data_path="./data",
    enabled_checks=None,
    *,
    config_path=None,
    output_dir=None,
    csv_path=None,
    log_path=None,
):
    if data_path is None:
        data_path = "./data/"

    cfg = Path(config_path) if config_path is not None else Path(__file__).resolve().parent / "config.yaml"

    try:
        with open(cfg, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        checks_cfg = config.get("checks") or {}
        merged_checks = {
            "DoD": bool(checks_cfg.get("DoD", True)),
            "WoW": bool(checks_cfg.get("WoW", True)),
        }
        if enabled_checks is not None:
            merged_checks.update(enabled_checks)
        aligned_df, raw_dfs = run_ingestion(data_path, config["tickers"])
        breach_df = run_processing(aligned_df, config, merged_checks)
        run_output(
            breach_df,
            output_dir=output_dir,
            csv_path=csv_path,
            log_path=log_path,
        )
        return "SUCCESS", {
            "processed_data": raw_dfs,
            "breach_report": breach_df.to_dict("records"),
        }
    except FileNotFoundError:
        return "REJECT: Missing Files", None
    except ValueError as e:
        msg = str(e)
        if "Invalid Price" in msg:
            return "REJECT: Invalid Price", None
        if "Header mismatch" in msg:
            return "REJECT: Header mismatch", None
        if msg.startswith("REJECT:"):
            return msg, None
        return f"REJECT: {msg}", None
    except Exception as e:
        msg = str(e)
        if "Extreme Volatility" in msg:
            return "WARNING: Extreme Volatility", None
        if msg.startswith("REJECT:") or msg.startswith("WARNING:"):
            return msg, None
        return f"REJECT: {msg}", None


def _build_arg_parser():
    p = argparse.ArgumentParser(
        description="Ingest index CSVs, apply DoD/WoW threshold checks, write breach CSV.",
    )
    p.add_argument(
        "--data-dir",
        default=None,
        metavar="DIR",
        help="Folder containing {TICKER}.csv files (default: ./data).",
    )
    p.add_argument(
        "legacy_data_dir",
        nargs="?",
        default=None,
        metavar="DIR",
        help="Optional data folder (same as --data-dir); kept for backward compatibility.",
    )
    p.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help="YAML config path (default: config.yaml next to main.py).",
    )
    out = p.add_mutually_exclusive_group()
    out.add_argument(
        "--output-dir",
        default=None,
        metavar="DIR",
        help="Write threshold_breaks.csv and mock_test_log.txt under this directory.",
    )
    out.add_argument(
        "--csv-path",
        default=None,
        metavar="PATH",
        help="Write breaches CSV to this file (log defaults to same directory).",
    )
    p.add_argument(
        "--log-path",
        default=None,
        metavar="PATH",
        help="Append run log to this file (default: next to CSV per --output-dir/--csv-path).",
    )
    return p


def _main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    data_dir = args.data_dir or args.legacy_data_dir or "./data"
    status, data = run_pipeline(
        data_path=data_dir,
        config_path=args.config,
        output_dir=args.output_dir,
        csv_path=args.csv_path,
        log_path=args.log_path,
    )
    print(status)
    if data:
        print(f"Breaches: {len(data['breach_report'])}")
    return exit_code_for_status(status)


if __name__ == "__main__":
    sys.exit(_main())
