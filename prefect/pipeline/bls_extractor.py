import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


class BLSExtractor:
    """Pulls labor-market time series (unemployment, payrolls, wages)
    from the Bureau of Labor Statistics public API. Works without a key
    (25 queries/day, 1 series/query); a free registration key raises
    that to 500 queries/day and up to 50 series per call — set
    BLS_API_KEY if you register one.
    """

    def __init__(self, lookback_years=10, base_path="/app/data/general_data/landing_zone/bls"):
        self.lookback_years = lookback_years
        self.storage = DataLakeStorage(base_path=base_path)
        self.api_key = os.environ.get("BLS_API_KEY")  # optional

    def fetch_single_ticker(self, series_id: str, asset_class: str = "macro") -> pd.DataFrame | None:
        logger.info(f"[bls] Fetching series {series_id}")

        today = datetime.today()
        payload = {
            "seriesid": [series_id],
            "startyear": str(today.year - self.lookback_years),
            "endyear": str(today.year),
        }
        if self.api_key:
            payload["registrationkey"] = self.api_key

        try:
            resp = requests.post(BLS_URL, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"[bls] Fetch failed for {series_id}: {e}")
            return None

        if data.get("status") != "REQUEST_SUCCEEDED":
            logger.warning(f"[bls] Request not successful for {series_id}: {data.get('message')}")
            return None

        series_list = data.get("Results", {}).get("series", [])
        if not series_list or not series_list[0].get("data"):
            logger.warning(f"[bls] No observations returned for {series_id}")
            return None

        df = pd.DataFrame(series_list[0]["data"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["period_date"] = pd.to_datetime(
            df["year"] + "-" + df["period"].str.replace("M", "").str.zfill(2) + "-01",
            errors="coerce"
        )
        df = df.dropna(subset=["value"])

        df["Ticker"] = series_id
        df["Asset_Class"] = asset_class

        time.sleep(0.5)  # unregistered tier: 25 req/day — space calls out generously
        return df

    def save(self, df: pd.DataFrame, series_id: str, asset_class: str):
        partition = f"{asset_class}/{series_id}/bls_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)