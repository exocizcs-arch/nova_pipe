import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

STOCKTWITS_URL = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"


class StockTwitsExtractor:
    """Pulls recent public messages + self-reported Bullish/Bearish tags
    from StockTwits. No API key needed for the public symbol stream, but
    it's rate-limited (~200 req/hour unauthenticated) and only returns
    the most recent ~30 messages per call — this is a sentiment
    snapshot, not a history backfill, so run it often rather than trying
    to pull deep history.
    """

    def __init__(self, lookback_years=None, base_path="/app/data/general_data/landing_zone/stocktwits"):
        self.storage = DataLakeStorage(base_path=base_path)

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        logger.info(f"[stocktwits] Fetching sentiment stream for {ticker}")

        try:
            resp = requests.get(STOCKTWITS_URL.format(ticker=ticker), timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[stocktwits] Fetch failed for {ticker}: {e}")
            return None

        messages = payload.get("messages", [])
        if not messages:
            logger.warning(f"[stocktwits] No messages returned for {ticker}")
            return None

        rows = []
        for msg in messages:
            sentiment = msg.get("entities", {}).get("sentiment")
            rows.append({
                "message_id": msg.get("id"),
                "created_at": msg.get("created_at"),
                "body": msg.get("body"),
                "sentiment": sentiment.get("basic") if sentiment else None,
                "user_followers": msg.get("user", {}).get("followers"),
            })

        df = pd.DataFrame(rows)
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class

        time.sleep(1.0)  # stay well under the unauthenticated 200 req/hour cap
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        # Timestamped filename (not just date) since this is a point-in-time
        # snapshot you'll likely pull multiple times per day.
        partition = f"{asset_class}/{ticker}/stocktwits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)