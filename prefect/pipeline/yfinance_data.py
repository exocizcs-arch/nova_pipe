import yfinance as yf
import pandas as pd
import time


def get_historical_data(ticker, start_date, end_date, interval="1d"):
    """
    Fetch clean, adjusted historical data ready for ML features.
    """
    # Download the data
    df = yf.download(ticker, start=start_date, end=end_date, interval=interval, auto_adjust=False)

    # FIX: 'Adj Close' is removed when auto_adjust=True. 'Close' is already adjusted.
    # Also, handle the case where yf.download might return a MultiIndex column if downloading multiple tickers
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df = df[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']].dropna()

    # Add simple returns using the (already adjusted) 'Close'
    df['return_1d'] = df['Adj Close'].pct_change()

    return df


# Example: 5 years of daily AAPL data
data = get_historical_data("AAPL", "2019-01-01", "2024-01-01")
print(data.tail())