import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredExtractor:
    """Pulls macroeconomic time series from FRED (Federal Reserve Economic
    Data). Free, but requires a free API key from
    https://fred.stlouisfed.org/docs/api/api_key.html

    Same shape as the other extractors. Here "ticker" is a FRED series_id
    (e.g. 'DFF', 'CPIAUCSL', 'DGS10') rather than a stock symbol.
    """

    def __init__(self, lookback_years=10, base_path="/app/data/general_data/landing_zone/fred"):
        # Macro series are low-frequency (daily/monthly), so a long default
        # lookback is cheap and gives models more regime coverage (hikes,
        # cuts, recessions) than price history alone would.
        self.lookback_years = lookback_years
        today = datetime.today()
        self.start_date = today.replace(year=today.year - lookback_years).strftime("%Y-%m-%d")
        self.end_date = today.strftime("%Y-%m-%d")
        self.storage = DataLakeStorage(base_path=base_path)

        self.api_key = os.environ.get("FRED_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "FRED_API_KEY env var is required — get a free key at "
                "https://fred.stlouisfed.org/docs/api/api_key.html"
            )

    def fetch_single_ticker(self, ticker: str, asset_class: str = "macro") -> pd.DataFrame | None:
        series_id = ticker
        logger.info(f"[fred] Fetching series {series_id}")

        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": self.start_date,
            "observation_end": self.end_date,
        }

        try:
            resp = requests.get(FRED_OBSERVATIONS_URL, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[fred] Fetch failed for {series_id}: {e}")
            return None

        obs = payload.get("observations", [])
        if not obs:
            logger.warning(f"[fred] No observations returned for {series_id}")
            return None

        df = pd.DataFrame(obs)
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")  # FRED uses "." for missing values
        df = df.dropna(subset=["value"]).drop(columns=["realtime_start", "realtime_end"], errors="ignore")

        df["Ticker"] = series_id
        df["Asset_Class"] = asset_class

        time.sleep(0.2)  # FRED's documented soft limit is ~120 req/min
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/fred_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)