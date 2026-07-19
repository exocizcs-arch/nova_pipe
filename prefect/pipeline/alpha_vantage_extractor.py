import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


class AlphaVantageExtractor:
    """Pulls free company fundamentals (OVERVIEW endpoint: P/E, EPS,
    market cap, margins, etc.) from Alpha Vantage. This is a fundamentals
    source, not a price source — you already have price data from
    yfinance/Alpaca/Stooq. Free tier is capped at 25 requests/day and
    ~5 requests/minute, so this only makes sense on a slow (e.g. weekly)
    schedule across a handful of tickers. Requires a free API key.
    """

    def __init__(self, lookback_years=1, base_path="/app/data/general_data/landing_zone/alpha_vantage"):
        self.lookback_years = lookback_years  # unused — OVERVIEW is a point-in-time snapshot, kept for interface consistency
        self.storage = DataLakeStorage(base_path=base_path)

        self.api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "ALPHA_VANTAGE_API_KEY env var is required — get a free key at "
                "https://www.alphavantage.co/support/#api-key"
            )

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        logger.info(f"[alpha_vantage] Fetching fundamentals overview for {ticker}")

        params = {"function": "OVERVIEW", "symbol": ticker, "apikey": self.api_key}

        try:
            resp = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[alpha_vantage] Fetch failed for {ticker}: {e}")
            return None

        if not payload or "Symbol" not in payload:
            # Alpha Vantage returns {} or a "Note"/"Information" string when
            # the daily quota is exhausted, rather than an HTTP error.
            logger.warning(f"[alpha_vantage] No overview data for {ticker} — likely rate-limited: {payload}")
            return None

        df = pd.DataFrame([payload])
        df["snapshot_date"] = datetime.today().date()
        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class

        time.sleep(15)  # free tier: ~5 req/min — space calls out generously
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/alpha_vantage_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)