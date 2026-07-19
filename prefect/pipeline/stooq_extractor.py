import time
import logging
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

STOOQ_URL = "https://stooq.com/q/d/l/"
# Stooq's download endpoint 404s requests without a browser-like User-Agent —
# this isn't a documented API, just what makes it stop rejecting the request.
STOOQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


class StooqExtractor:
    """Pulls free EOD history from Stooq — no API key, no published rate
    limit. Good as a backup/cross-check source for stocks and forex;
    crypto coverage is thinner. Same shape as the other extractors.
    """

    def __init__(self, lookback_years=5, base_path="/app/data/general_data/landing_zone/stooq"):
        self.lookback_years = lookback_years
        self.storage = DataLakeStorage(base_path=base_path)

    @staticmethod
    def _to_stooq_symbol(ticker: str, asset_class: str) -> str:
        """Stooq uses its own symbol conventions: US stocks get a '.us'
        suffix; forex/crypto pairs drop yfinance-style separators/suffixes."""
        if asset_class == "stocks":
            return f"{ticker.lower()}.us"
        if asset_class == "forex":
            base = ticker.replace("=X", "").lower()
            if base == "jpy":
                return "usdjpy"  # Stooq quotes USDJPY, not JPY=X
            return base
        if asset_class == "crypto":
            return ticker.replace("-", "").lower()
        return ticker.lower()

    def fetch_single_ticker(self, ticker: str, asset_class: str) -> pd.DataFrame | None:
        symbol = self._to_stooq_symbol(ticker, asset_class)
        logger.info(f"[stooq] Fetching {asset_class} data for {ticker} ({symbol})")

        try:
            resp = requests.get(STOOQ_URL, params={"s": symbol, "i": "d"}, headers=STOOQ_HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"[stooq] Fetch failed for {ticker}: {e}")
            return None

        text = resp.text.strip()
        if not text or text.lower().startswith("no data") or "<html" in text.lower():
            logger.warning(f"[stooq] No data returned for {ticker} ({symbol})")
            return None

        df = pd.read_csv(StringIO(text))
        if df.empty or "Close" not in df.columns:
            logger.warning(f"[stooq] Empty/malformed response for {ticker}")
            return None

        df["Date"] = pd.to_datetime(df["Date"])
        cutoff = datetime.today() - pd.Timedelta(days=self.lookback_years * 365)
        df = df[df["Date"] >= cutoff]

        df["return_1d"] = df["Close"].pct_change()
        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class

        time.sleep(0.3)  # be polite — no documented free-tier limit, but don't hammer it
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/stooq_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)