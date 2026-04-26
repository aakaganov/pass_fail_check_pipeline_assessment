"""Load index CSVs and align them on shared observation dates.

For each ticker, reads {data_path}/{ticker}.csv, validates headers and dates,
deduplicates same-day rows (first row kept), coerces prices (drops blank prices,
rejects invalid placeholders and non-positive values), and stores a per-ticker
frame including daily_return for inspection.

Inner-joins all tickers on observation_date so aligned_df has one row per
date where every configured index has a close. Returns (aligned_df, data_frames)
where data_frames is ticker -> cleaned DataFrame.

Raises FileNotFoundError if a file is missing; ValueError for bad schema,
dates, or prices; RuntimeError with a WARNING: or REJECT: message when
no overlapping dates remain after the join.
"""

import os

import pandas as pd


def run_ingestion(data_path, tickers):
    data_frames = {}

    for ticker in tickers:
        file_path = os.path.join(data_path, f"{ticker}.csv")
        # If file does not exist, raise error.
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Ticker file not found: {file_path}")
        # Read CSV into DataFrame; skipinitialspace trims leading spaces in fields.
        df = pd.read_csv(file_path, skipinitialspace=True)

        # Header verification:
        # Allow either:
        # 1) observation_date,closing_price
        # 2) observation_date,<ticker>  (e.g. observation_date,SP500)
        if len(df.columns) != 2:
            raise ValueError(f"Header mismatch in {ticker}.csv")
        first_col, second_col = list(df.columns)
        if first_col != "observation_date":
            raise ValueError(f"Header mismatch in {ticker}.csv")
        if second_col == "closing_price":
            price_col = "closing_price"
        elif second_col == ticker:
            price_col = ticker
        else:
            raise ValueError(f"Header mismatch in {ticker}.csv")

        # If dataframe is empty, raise error.
        if df.empty:
            raise ValueError("empty file")
        # date cleaning, checks if dates are in the correct format
        raw_dates = df["observation_date"].astype(str).str.strip()
        if raw_dates.str.fullmatch(r"[A-Za-z ]+").any():
            raise ValueError("Random string in column")
        try:
            parsed_dates = pd.to_datetime(raw_dates, format="%Y-%m-%d", errors="raise")
        except Exception:
            raise ValueError("Date in wrong format")
        df["observation_date"] = parsed_dates
        df = df.drop_duplicates(subset=["observation_date"], keep="first").reset_index(
            drop=True
        )
        if not df["observation_date"].is_monotonic_increasing:
            raise ValueError("Date in wrong order")

        # Price cleaning, checks if prices are numeric and not empty, cleans commas
        raw_price_strings = df[price_col].astype(str).str.strip()
        placeholder_mask = raw_price_strings.str.lower().isin({"n/a", "NA", "N/A", "na", "."})
        if placeholder_mask.any():
            raise ValueError("Missing Price")

        cleaned_prices = raw_price_strings.str.replace(",", "", regex=False)
        df["closing_price"] = pd.to_numeric(cleaned_prices, errors="coerce")
        # Drop rows with NA prices.
        df = df.dropna(subset=["closing_price"]).copy()
        if df.empty:
            raise ValueError("empty file")
        if (df["closing_price"] <= 0).any():
            raise ValueError("Invalid Price")

        # Round prices to 2 decimal places.
        df["closing_price"] = df["closing_price"].round(2)
        #calculates daily returns
        df["daily_return"] = df["closing_price"].pct_change().fillna(0.0)

        data_frames[ticker] = df

    # Inner join after all tickers are loaded (outside the per-ticker loop).
    aligned_df = None
    for ticker, df in data_frames.items():
        # Merge only price columns; keep per-ticker daily_return in raw_dfs only.
        temp_df = df[["observation_date", "closing_price"]].rename(columns={"closing_price": ticker})
        if aligned_df is None:
            aligned_df = temp_df
        else:
            aligned_df = pd.merge(aligned_df, temp_df, on="observation_date", how="inner")

    if aligned_df is None or aligned_df.empty:
        # Distinguish broad date-range non-overlap from simple out-of-sync rows.
        if data_frames and all(len(df) >= 2 for df in data_frames.values()):
            raise RuntimeError("WARNING: Incomplete overlap")
        raise RuntimeError("REJECT: Files out of sync")

    return aligned_df, data_frames
