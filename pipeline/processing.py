import pandas as pd

def run_processing(aligned_df, config, enabled_checks):
    breach_list = []
    tickers = config.get('tickers', [])
    thresholds = config.get('thresholds',{})

    for ticker in tickers:
        # Metric calculation
        dod = aligned_df[ticker].pct_change(periods = 1)
        wow = aligned_df[ticker].pct_change(periods = 5)
        #Custom/default threshold
        limit_dod = thresholds.get(ticker, config.get('default_threshold_dod', 0.05))
        limit_wow = config.get('default_threshold_dod', 0.05)

        for i in range(1, len(aligned_df)):
            current_date = aligned_df.iloc[i]['observation_date']
            current_price = aligned_df.iloc[i][ticker]

            #DoD flagging
            val_dod = dod.iloc[i]
            if abs(val_dod) > 0.2:
                # This could be logged or returned as a specific warning status
                print(f"WARNING: Extreme Volatility for {ticker} on {current_date}")
                
            if abs(val_dod) > limit_dod:
                breach_list.append(format_breach(ticker, current_date, current_price, aligned_df.iloc[i-1][ticker], val_dod, "DoD"))
            
            # WoW Flagging (if enabled and enough data exists)
            if enabled_checks.get('WoW', True) and i >= 5:
                val_wow = wow.iloc[i]
                if abs(val_wow) > limit_wow:
                    breach_list.append(format_breach(ticker, current_date, current_price, aligned_df.iloc[i-5][ticker], val_wow, "WoW"))
                    
    return pd.DataFrame(breach_list)

def format_breach(ticker, date, current, previous, pct_diff, check_type):
    return {
        'Ticker': ticker,
        'Date': date.strftime('%Y-%m-%d'),
        'Current_Value': round(current, 2),
        'Previous_Value': round(previous, 2),
        'Difference_Num': round(current - previous, 2),
        'Difference_Pct': round(pct_diff, 4),
        'Check_Type': check_type
    }
