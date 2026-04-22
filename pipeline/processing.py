import pandas as pd

# Stable schema when there are zero breaches (CSV + tests expect these columns).
_BREACH_COLUMNS = [
    "Ticker",
    "Date",
    "Current_Value",
    "Previous_Value",
    "Difference_Num",
    "Difference_Pct",
    "Check_Type",
]


def run_processing(aligned_df, config, enabled_checks):
    breach_list = []
    tickers = config.get("tickers", [])
    thresholds = config.get("thresholds", {})
    # Use config instead of a magic 0.2 so YAML stays the single source of truth.
    anomaly_limit = config.get("anomaly_warning_limit", 0.20)

    for ticker in tickers:
        dod = aligned_df[ticker].pct_change(periods=1)
        wow = aligned_df[ticker].pct_change(periods=5)
        limit_dod = thresholds.get(ticker, config.get("default_threshold_dod", 0.05))
        # WoW uses its own default from config (not DoD’s).
        limit_wow = config.get("default_threshold_wow", 0.05)

        for i in range(1, len(aligned_df)):
            current_date = aligned_df.iloc[i]["observation_date"]
            current_price = aligned_df.iloc[i][ticker]

            val_dod = dod.iloc[i]
            # main.py returns WARNING: Extreme Volatility if the exception message contains this phrase.
            if pd.notna(val_dod) and abs(val_dod) > anomaly_limit:
                raise RuntimeError(
                    f"Extreme Volatility: {ticker} on {current_date} "
                    f"(|DoD|={abs(float(val_dod)):.4f} > {anomaly_limit})"
                )

            if pd.notna(val_dod) and abs(val_dod) > limit_dod:
                breach_list.append(
                    format_breach(
                        ticker,
                        current_date,
                        current_price,
                        aligned_df.iloc[i - 1][ticker],
                        val_dod,
                        "DoD",
                    )
                )

            if enabled_checks.get("WoW", True) and i >= 5:
                val_wow = wow.iloc[i]
                if pd.notna(val_wow) and abs(val_wow) > limit_wow:
                    breach_list.append(
                        format_breach(
                            ticker,
                            current_date,
                            current_price,
                            aligned_df.iloc[i - 5][ticker],
                            val_wow,
                            "WoW",
                        )
                    )

    if not breach_list:
        return pd.DataFrame(columns=_BREACH_COLUMNS)
    return pd.DataFrame(breach_list)


def format_breach(ticker, date, current, previous, pct_diff, check_type):
    return {
        "Ticker": ticker,
        "Date": date.strftime("%Y-%m-%d"),
        "Current_Value": round(current, 2),
        "Previous_Value": round(previous, 2),
        "Difference_Num": round(current - previous, 2),
        "Difference_Pct": round(pct_diff, 4),
        "Check_Type": check_type,
    }
