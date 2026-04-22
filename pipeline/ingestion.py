import os

import pandas as pd


def run_ingestion(data_path, tickers):
    data_frames = {}

    for ticker in tickers:
        file_path = os.path.join(data_path, f"{ticker}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Ticket file not found: {file_path}")

        df = pd.read_csv(file_path, skipinitialspace=True)

        # Header verification
        if list(df.columns) != ["observation_date", "closing_price"]:
            raise ValueError(f"Header mismatch in {ticker}.csv")

        if df.empty:
            raise ValueError("empty file")

        raw_dates = df["observation_date"].astype(str).str.strip()
        if raw_dates.str.fullmatch(r"[A-Za-z ]+").any():
            raise ValueError("Random string in column")
        try:
            parsed_dates = pd.to_datetime(raw_dates, format="%Y-%m-%d", errors="raise")
        except Exception:
            raise ValueError("Date in wrong format")
        if parsed_dates.duplicated().any():
            raise ValueError("Duplicate Dates")
        if not parsed_dates.is_monotonic_increasing:
            raise ValueError("Date in wrong order")
        df["observation_date"] = parsed_dates

        # Price cleaning:
        # - reject explicit non-numeric placeholders (N/A, .)
        # - allow blank cells by dropping those rows (holiday gaps)
        raw_price_strings = df["closing_price"].astype(str).str.strip()
        placeholder_mask = raw_price_strings.str.lower().isin({"n/a", "na", "."})
        if placeholder_mask.any():
            raise ValueError("Missing Price")

        cleaned_prices = raw_price_strings.str.replace(",", "", regex=False)
        df["closing_price"] = pd.to_numeric(cleaned_prices, errors="coerce")
        df = df.dropna(subset=["closing_price"]).copy()
        if df.empty:
            raise ValueError("empty file")
        if (df["closing_price"] <= 0).any():
            raise ValueError("Invalid Price")

        df["closing_price"] = df["closing_price"].round(2)
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
