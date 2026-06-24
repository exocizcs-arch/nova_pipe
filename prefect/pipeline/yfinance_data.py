import time
import json
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

from storage import DataLakeStorage


class YFinanceExtractor:
    def __init__(self, lookback_years=5, config_path="/app/pipeline/tickers.json"):
        self.lookback_years = lookback_years
        self.config_path = config_path
        self.end_date = datetime.today().strftime('%Y-%m-%d')
        self.start_date = (datetime.today() - timedelta(days=self.lookback_years * 365)).strftime('%Y-%m-%d')

        self.storage = DataLakeStorage(base_path="/data/landing_zone")

    def fetch_single_ticker(self, ticker, asset_class, interval="1d"):
        """Fetches and cleans historical data for a single asset."""
        print(f"Fetching {asset_class} data for {ticker}...")

        ticker_obj = yf.Ticker(ticker)

        try:
            currency = ticker_obj.fast_info.get('currency', 'UNKNOWN')
        except Exception:
            currency = 'UNKNOWN'

        df = yf.download(ticker, start=self.start_date, end=self.end_date, interval=interval, auto_adjust=False)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        if df.empty:
            print(f"No data found for {ticker}")
            return None

        df = df[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']].dropna()
        df['return_1d'] = df['Adj Close'].pct_change()

        # Add metadata columns
        df['Ticker'] = ticker
        df['Asset_Class'] = asset_class
        df['Currency'] = currency

        df = df.reset_index()
        return df

    def run_extraction(self):
        """Reads the JSON config, runs the extraction loop, and saves the data."""
        # 1. Load the JSON config file
        try:
            with open(self.config_path, 'r') as file:
                config = json.load(file)
        except FileNotFoundError:
            print(f"❌ Config file not found at {self.config_path}")
            return

        all_dataframes = []

        for asset_class, ticker_list in config.items():
            print(f"\n--- Starting extraction for category: {asset_class.upper()} ---")

            for ticker in ticker_list:
                df = self.fetch_single_ticker(ticker, asset_class)
                if df is not None:
                    all_dataframes.append(df)

                time.sleep(1)

        if all_dataframes:
            print("\nMerging data...")
            final_df = pd.concat(all_dataframes, ignore_index=True)
            print(f"Successfully extracted {len(final_df)} total rows.")

            self.storage.save_to_parquet(df=final_df, filename="yfinance_assets.parquet")
        else:
            print("No data was retrieved. Pipeline aborted.")


if __name__ == "__main__":
    extractor = YFinanceExtractor(lookback_years=5)
    extractor.run_extraction()