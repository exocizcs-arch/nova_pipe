import os
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

from storage import DataLakeStorage

logger = logging.getLogger(__name__)


class AlpacaExtractor:
    """Pulls historical daily bars from Alpaca's free IEX feed.
    Same shape as YFinanceExtractor so the orchestration flow can
    treat both sources uniformly.
    """

    def __init__(self, lookback_years=5, base_path="/app/data/general_data/landing_zone/alpaca"):
        self.lookback_years = lookback_years
        self.end_date = datetime.now(timezone.utc)
        self.start_date = self.end_date - timedelta(days=lookback_years * 365)
        self.storage = DataLakeStorage(base_path=base_path)

        self.client = StockHistoricalDataClient(
            os.environ["ALPACA_API_KEY"],
            os.environ["ALPACA_SECRET_KEY"],
        )

    def fetch_single_ticker(self, ticker: str, asset_class: str, timeframe=TimeFrame.Day) -> pd.DataFrame | None:
        logger.info(f"[alpaca] Fetching {asset_class} data for {ticker}")

        request = StockBarsRequest(
            symbol_or_symbols=[ticker],
            timeframe=timeframe,
            start=self.start_date,
            end=self.end_date,
            feed=DataFeed.IEX,
        )

        try:
            bars = self.client.get_stock_bars(request)
        except Exception as e:
            logger.error(f"[alpaca] Fetch failed for {ticker}: {e}")
            return None

        df = bars.df
        if df.empty:
            logger.warning(f"[alpaca] No data returned for {ticker}")
            return None

        df = df.reset_index()
        df['return_1d'] = df['close'].pct_change()
        df['Ticker'] = ticker
        df['Asset_Class'] = asset_class

        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/alpaca_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)