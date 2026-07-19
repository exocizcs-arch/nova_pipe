import time
import logging
import pandas as pd
from datetime import datetime
from pytrends.request import TrendReq

from storage import DataLakeStorage

logger = logging.getLogger(__name__)


class GoogleTrendsExtractor:
    """Pulls search-interest-over-time for a ticker/keyword via pytrends,
    an unofficial wrapper around Google Trends (there is no official
    public API). Free, no key — but fragile: Google actively
    rate-limits and occasionally CAPTCHAs this endpoint. Treat failures
    as routine, not exceptional, and don't schedule this tightly.
    """

    def __init__(self, lookback_years=1, base_path="/app/data/general_data/landing_zone/google_trends"):
        self.lookback_years = lookback_years
        self.storage = DataLakeStorage(base_path=base_path)
        self.pytrends = TrendReq(hl="en-US", tz=0)

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        logger.info(f"[google_trends] Fetching search interest for {ticker}")

        years = max(1, min(self.lookback_years, 5))
        timeframe = f"today {years}-y"

        try:
            self.pytrends.build_payload([ticker], timeframe=timeframe)
            df = self.pytrends.interest_over_time()
        except Exception as e:
            logger.error(f"[google_trends] Fetch failed for {ticker}: {e}")
            return None

        if df is None or df.empty:
            logger.warning(f"[google_trends] No data returned for {ticker}")
            return None

        df = df.reset_index().rename(columns={ticker: "search_interest"})
        df = df.drop(columns=["isPartial"], errors="ignore")
        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class

        time.sleep(2.0)  # no documented limit, but aggressive polling triggers CAPTCHAs
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/google_trends_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)