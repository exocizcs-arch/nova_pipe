import time
import random
import logging
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

from storage import DataLakeStorage

logger = logging.getLogger(__name__)


class YFinanceExtractor:
    """Pulls historical bars from Yahoo Finance (unofficial endpoint).
    No Prefect logic here — this class only knows how to fetch and save.
    Retries/scheduling are handled by the orchestration flow.
    """

    def __init__(self, lookback_years=5, base_path="/data/landing_zone/yfinance"):
        self.lookback_years = lookback_years
        today = datetime.today()
        self.end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
        self.start_date = (today - timedelta(days=lookback_years * 365)).strftime('%Y-%m-%d')
        self.storage = DataLakeStorage(base_path=base_path)

    def fetch_single_ticker(self, ticker: str, asset_class: str, interval: str = "1d") -> pd.DataFrame | None:
        logger.info(f"[yfinance] Fetching {asset_class} data for {ticker}")

        try:
            df = yf.download(
                ticker, start=self.start_date, end=self.end_date,
                interval=interval, auto_adjust=False, progress=False
            )
        except Exception as e:
            logger.error(f"[yfinance] Fetch failed for {ticker}: {e}")
            raise

        if df.empty:
            logger.warning(f"[yfinance] No data returned for {ticker}")
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)

        df = df[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']].dropna()
        df['return_1d'] = df['Adj Close'].pct_change()

        try:
            currency = yf.Ticker(ticker).fast_info.get('currency', 'UNKNOWN')
        except Exception:
            currency = 'UNKNOWN'

        df['Ticker'] = ticker
        df['Asset_Class'] = asset_class
        df['Currency'] = currency
        df = df.reset_index()

        time.sleep(random.uniform(0.5, 1.5))
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/yfinance_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)