import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

FMP_RATIOS_URL = "https://financialmodelingprep.com/api/v3/ratios/{ticker}"


class FMPExtractor:
    """Pulls free-tier financial ratios (P/E, ROE, current ratio, debt
    ratios, etc.) from Financial Modeling Prep. Free tier: 250
    requests/day, best coverage on US large/mid-caps — check coverage
    for smaller tickers before relying on this. Requires a free API key.
    """

    def __init__(self, lookback_years=5, base_path="/app/data/general_data/landing_zone/fmp"):
        self.lookback_years = lookback_years
        self.storage = DataLakeStorage(base_path=base_path)

        self.api_key = os.environ.get("FMP_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "FMP_API_KEY env var is required — get a free key at "
                "https://site.financialmodelingprep.com/developer/docs"
            )

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        logger.info(f"[fmp] Fetching financial ratios for {ticker}")

        params = {"period": "annual", "limit": self.lookback_years, "apikey": self.api_key}

        try:
            resp = requests.get(FMP_RATIOS_URL.format(ticker=ticker), params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[fmp] Fetch failed for {ticker}: {e}")
            return None

        if not payload or not isinstance(payload, list):
            logger.warning(f"[fmp] No ratio data for {ticker} — response: {payload}")
            return None

        df = pd.DataFrame(payload)
        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class

        time.sleep(0.3)
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/fmp_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)