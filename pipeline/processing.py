"""Apply DoD/WoW threshold rules to aligned index prices.

Reads thresholds from config: thresholds (per-ticker DoD, optional),
thresholds_wow (per-ticker WoW, optional), default_threshold_dod,
default_threshold_wow, anomaly_warning_limit, and tickers.

DoD uses pct_change(periods=1) on aligned_df; WoW uses five rows
(pct_change(periods=5)). Breaches use strict |return| > limit after rounding
both sides to mitigate float noise from pct_change (so e.g. nominal 1% is not
flagged when the limit is 1%).
enabled_checks toggles DoD / WoW per run.

Returns a DataFrame of breach rows (fixed columns when empty). Raises
RuntimeError containing Extreme Volatility when |DoD| exceeds
anomaly_warning_limit (surfaced as a warning in main.py).
"""

import pandas as pd

# Stable schema when there are zero breaches (CSV + tests expect these columns).
_BREACH_COLUMNS = [
    "Ticker",
    "Date",
    "Current_Value",
    "Previous_Value",
    "Previous_Date",
    "Difference_Num",
    "Difference_Pct",
    "Check_Type",
    "Threshold_Applied",
    "Direction",
]

# Decimal places for comparing returns to YAML limits (float-safe strict >).
_RETURN_COMPARE_DECIMALS = 10


def _abs_return_strictly_exceeds(raw_pct, limit):
    """True if |raw_pct| is strictly greater than limit after rounding.

    pandas pct_change can be a few ULP away from the rational value;
    rounding aligns comparisons with the brief (e.g. exactly 1% vs a 1% cap).

    Previous use without function ended up flagging 1% changes when the limit was
    set to 1%,
    """
    if pd.isna(raw_pct):
        return False
    mag = round(abs(float(raw_pct)), _RETURN_COMPARE_DECIMALS)
    lim = round(float(limit), _RETURN_COMPARE_DECIMALS)
    return mag > lim


def run_processing(aligned_df, config, enabled_checks):
    """Scan aligned_df for DoD/WoW threshold breaches.

    aligned_df must include observation_date and one numeric column per
    ticker in config["tickers"]. enabled_checks is a dict with optional
    DoD / WoW booleans (defaults True if missing).
    """
    breach_list = []
    tickers = config.get("tickers", [])
    thresholds_dod = config.get("thresholds", {})
    thresholds_wow = config.get("thresholds_wow", {})
    # default anomaly warning limit is 0.20
    anomaly_limit = config.get("anomaly_warning_limit", 0.20)

    for ticker in tickers:
        dod = aligned_df[ticker].pct_change(periods=1)  # One trading day.
        wow = aligned_df[ticker].pct_change(periods=5)  # Five trading rows back.
        limit_dod = thresholds_dod.get(
            ticker, config.get("default_threshold_dod", 0.01)
        )
        limit_wow = thresholds_wow.get(
            ticker, config.get("default_threshold_wow", 0.05)
        )

        for i in range(1, len(aligned_df)):
            current_date = aligned_df.iloc[i]["observation_date"]
            current_price = aligned_df.iloc[i][ticker]

            val_dod = dod.iloc[i]
            # main.py returns WARNING: Extreme Volatility if the exception message contains this phrase.
            if _abs_return_strictly_exceeds(val_dod, anomaly_limit):
                raise RuntimeError(
                    f"Extreme Volatility: {ticker} on {current_date} "
                    f"(|DoD|={abs(float(val_dod)):.4f} > {anomaly_limit})"
                )
            # DoD flagging.
            if (
                enabled_checks.get("DoD", True)
                and pd.notna(val_dod)
                and _abs_return_strictly_exceeds(val_dod, limit_dod)
            ):
                breach_list.append(
                    format_breach(
                        ticker,
                        current_date,
                        current_price,
                        aligned_df.iloc[i - 1][ticker],
                        val_dod,
                        "DoD",
                        threshold_applied=limit_dod,
                        previous_date=aligned_df.iloc[i - 1]["observation_date"],
                    )
                )

            # WoW flagging.
            if enabled_checks.get("WoW", True) and i >= 5:
                val_wow = wow.iloc[i]
                if pd.notna(val_wow) and _abs_return_strictly_exceeds(
                    val_wow, limit_wow
                ):
                    breach_list.append(
                        format_breach(
                            ticker,
                            current_date,
                            current_price,
                            aligned_df.iloc[i - 5][ticker],
                            val_wow,
                            "WoW",
                            threshold_applied=limit_wow,
                            previous_date=aligned_df.iloc[i - 5]["observation_date"],
                        )
                    )

    if not breach_list:
        return pd.DataFrame(columns=_BREACH_COLUMNS)
    return pd.DataFrame(breach_list)


def _direction_from_return(pct_diff):
    x = float(pct_diff)
    if x > 0:
        return "up"
    if x < 0:
        return "down"
    return "flat"


def format_breach(
    ticker,
    date,
    current,
    previous,
    pct_diff,
    check_type,
    *,
    threshold_applied,
    previous_date,
):
    """Build one breach row dict for CSV / API consumption."""
    prev_ts = pd.Timestamp(previous_date)
    return {
        "Ticker": ticker,
        "Date": date.strftime("%Y-%m-%d"),
        "Current_Value": round(current, 2),
        "Previous_Value": round(previous, 2),
        "Previous_Date": prev_ts.strftime("%Y-%m-%d"),
        "Difference_Num": round(current - previous, 2),
        "Difference_Pct": round(pct_diff, 4),
        "Check_Type": check_type,
        "Threshold_Applied": round(float(threshold_applied), 6),
        "Direction": _direction_from_return(pct_diff),
    }
