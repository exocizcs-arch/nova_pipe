import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/{id}/market_chart"

# yfinance-style tickers -> CoinGecko coin ids. Extend as you add coins.
TICKER_TO_COINGECKO_ID = {
    "BTC-USD": "bitcoin",
    "ETH-USD": "ethereum",
}


class CoinGeckoExtractor:
    """Pulls free daily price/market-cap/volume history from CoinGecko's
    public API. No key required on the free tier, but it's aggressively
    rate-limited (~10-30 calls/min) — sleeps between calls accordingly.
    Free tier also caps history at 365 days for daily granularity,
    regardless of lookback_years requested.
    """

    def __init__(self, lookback_years=5, base_path="/app/data/general_data/landing_zone/coingecko"):
        self.lookback_years = lookback_years
        self.storage = DataLakeStorage(base_path=base_path)

    def fetch_single_ticker(self, ticker: str, asset_class: str = "crypto") -> pd.DataFrame | None:
        coin_id = TICKER_TO_COINGECKO_ID.get(ticker)
        if coin_id is None:
            logger.warning(f"[coingecko] No CoinGecko id mapped for {ticker} — add it to TICKER_TO_COINGECKO_ID")
            return None

        logger.info(f"[coingecko] Fetching {asset_class} data for {ticker} ({coin_id})")

        days = min(self.lookback_years * 365, 365)  # free-tier cap
        params = {"vs_currency": "usd", "days": days, "interval": "daily"}

        try:
            resp = requests.get(COINGECKO_URL.format(id=coin_id), params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[coingecko] Fetch failed for {ticker}: {e}")
            return None

        prices = payload.get("prices", [])
        market_caps = payload.get("market_caps", [])
        volumes = payload.get("total_volumes", [])

        if not prices:
            logger.warning(f"[coingecko] No data returned for {ticker}")
            return None

        df = pd.DataFrame(prices, columns=["timestamp_ms", "price"])
        df["market_cap"] = [row[1] for row in market_caps] if len(market_caps) == len(prices) else None
        df["volume"] = [row[1] for row in volumes] if len(volumes) == len(prices) else None

        df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms").dt.normalize()
        df["return_1d"] = df["price"].pct_change()
        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class

        time.sleep(1.5)  # stay well clear of CoinGecko's free-tier rate limit
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/coingecko_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)