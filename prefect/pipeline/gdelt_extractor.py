import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

TICKER_TO_KEYWORD = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Google",
    "AMZN": "Amazon",
}


class GDELTExtractor:
    """Pulls a daily average-news-tone time series per company from the
    GDELT Project's Doc 2.0 API. Free, no key, no auth, no documented
    hard rate limit — the broadest-coverage free news-sentiment source
    here, though less finance-specific than Alpha Vantage's
    NEWS_SENTIMENT endpoint (GDELT scores general news tone, not
    ticker-relevance-weighted sentiment).
    """

    def __init__(self, lookback_years=1, base_path="/app/data/general_data/landing_zone/gdelt"):
        self.lookback_years = lookback_years
        self.storage = DataLakeStorage(base_path=base_path)

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        keyword = TICKER_TO_KEYWORD.get(ticker)
        if keyword is None:
            logger.warning(f"[gdelt] No keyword mapped for {ticker} — add it to TICKER_TO_KEYWORD")
            return None

        logger.info(f"[gdelt] Fetching news tone timeline for {ticker} ({keyword})")

        timespan = f"{min(self.lookback_years, 3) * 12}m"
        params = {
            "query": keyword,
            "mode": "timelinetone",
            "format": "json",
            "timespan": timespan,
        }

        try:
            resp = requests.get(GDELT_DOC_API_URL, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[gdelt] Fetch failed for {ticker}: {e}")
            return None

        timeline = payload.get("timeline", [])
        if not timeline or not timeline[0].get("data"):
            logger.warning(f"[gdelt] No timeline data returned for {ticker} ({keyword})")
            return None

        df = pd.DataFrame(timeline[0]["data"])
        if df.empty or "value" not in df.columns:
            logger.warning(f"[gdelt] Empty/malformed timeline for {ticker}")
            return None

        df = df.rename(columns={"value": "avg_tone"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["Ticker"] = ticker
        df["Keyword"] = keyword
        df["Asset_Class"] = asset_class

        time.sleep(0.5)  # no documented limit, but stay polite to a free public service
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/gdelt_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)