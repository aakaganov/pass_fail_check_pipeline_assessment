import pandas as pd
import os

def run_ingestion(data_path, tickers):
    data_frames = {}

    for ticker in tickers:
        file_path = os.path.join(data_path, f"{tickers}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Ticket file not found: {file_path}")

            #Load with whitespace and handling
            df = pd.read_csv(file_path, skipinitialspace=True)

            # Header verification
            if list(df.columns) != ['observation_date', 'closing_price']:
                raise ValueError(f"Header mismatch in {ticker}.csv")
            
            # Date cleaning: Convert dates, handle duplicates
            df['observation_date'] = pd.to_datetime(df['observation_date'])
            df = df.sort_values('observation_date')
            df = df.drop_duplicates(subset = ['observation_date'], keep = 'first') #keep only first occurence for duplicate dates

            #Price cleaning: strip commas, cast to float, check for bounding errors
            if df['closing_price'].dtype == object:
                df['closing_price'] = df['closing_price'].str.replace(',','').astype(float)
            if (df['closing_price'] <= 0).any():
                raise ValueError(f"Invalid Price: nonpositive price found in {ticker}")

            data_frames[ticker] = df
        #inner join alignment 
        aligned_df = None
        for ticker, df in data_frames.items():
            temp_df = df.rename(columns={'closing_price': ticker})
            if aligned_df is None:
                aligned_df = temp_df
            else:
                aligned_df = pd.merge(aligned_df, temp_df, on='observation_date', how='inner')
                
        if aligned_df is None or aligned_df.empty:
            raise RuntimeError("REJECT: Files out of sync")
            
        return aligned_df, data_frames