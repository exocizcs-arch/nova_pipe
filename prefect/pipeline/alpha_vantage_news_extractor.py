import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


class AlphaVantageNewsExtractor:
    """Pulls news + pre-computed sentiment scores per ticker from Alpha
    Vantage's NEWS_SENTIMENT endpoint. Separate class from
    AlphaVantageExtractor (which hits OVERVIEW) since it's a different
    endpoint and a different shaped response — kept as its own extractor
    so each maps to one clean parquet table, same as everything else.
    Same free-tier limits apply: 25 requests/day, ~5/min. Uses the same
    ALPHA_VANTAGE_API_KEY you already set up.
    """

    def __init__(self, lookback_years=None, base_path="/app/data/general_data/landing_zone/alpha_vantage_news"):
        self.storage = DataLakeStorage(base_path=base_path)

        self.api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "ALPHA_VANTAGE_API_KEY env var is required — get a free key at "
                "https://www.alphavantage.co/support/#api-key"
            )

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        logger.info(f"[alpha_vantage_news] Fetching news sentiment for {ticker}")

        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "apikey": self.api_key,
            "limit": 50,
        }

        try:
            resp = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[alpha_vantage_news] Fetch failed for {ticker}: {e}")
            return None

        feed = payload.get("feed")
        if not feed:
            # Alpha Vantage returns {} or a "Note"/"Information" string when
            # rate-limited, same failure mode as the OVERVIEW endpoint.
            logger.warning(f"[alpha_vantage_news] No news feed for {ticker} — likely rate-limited: {payload}")
            return None

        rows = []
        for article in feed:
            # Each article scores relevance/sentiment per ticker mentioned —
            # pull out just the score for the ticker we asked about.
            ticker_scores = {
                ts["ticker"]: ts
                for ts in article.get("ticker_sentiment", [])
            }
            match = ticker_scores.get(ticker, {})

            rows.append({
                "title": article.get("title"),
                "url": article.get("url"),
                "time_published": article.get("time_published"),
                "source": article.get("source"),
                "overall_sentiment_score": article.get("overall_sentiment_score"),
                "overall_sentiment_label": article.get("overall_sentiment_label"),
                "ticker_relevance_score": match.get("relevance_score"),
                "ticker_sentiment_score": match.get("ticker_sentiment_score"),
                "ticker_sentiment_label": match.get("ticker_sentiment_label"),
            })

        df = pd.DataFrame(rows)
        df["time_published"] = pd.to_datetime(df["time_published"], format="%Y%m%dT%H%M%S", errors="coerce")
        for col in ["overall_sentiment_score", "ticker_relevance_score", "ticker_sentiment_score"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class

        time.sleep(15)  # same free-tier pacing as the OVERVIEW extractor
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/alpha_vantage_news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)