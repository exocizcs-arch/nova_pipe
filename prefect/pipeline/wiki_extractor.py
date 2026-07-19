import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

WIKI_PAGEVIEWS_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "en.wikipedia/all-access/all-agents/{article}/daily/{start}/{end}"
)

# Ticker -> Wikipedia article title. Extend as you add tickers to config.json.
TICKER_TO_WIKI_ARTICLE = {
    "AAPL": "Apple_Inc.",
    "MSFT": "Microsoft",
    "GOOGL": "Google",
    "AMZN": "Amazon_(company)",
}


class WikipediaPageviewsExtractor:
    """Pulls daily Wikipedia article pageviews as a public-attention proxy
    for a ticker — a stable, official, keyless replacement for Google
    Trends. Unlike pytrends (which reverse-engineers an undocumented
    Google endpoint that breaks on and off for years across many
    projects), this hits Wikimedia's official REST API directly.
    Requires only a descriptive User-Agent, same policy as SEC EDGAR.
    """

    def __init__(self, lookback_years=5, base_path="/app/data/general_data/landing_zone/wikipedia_pageviews"):
        self.lookback_years = lookback_years
        today = datetime.today()
        self.start_date = (today - timedelta(days=lookback_years * 365)).strftime("%Y%m%d")
        self.end_date = today.strftime("%Y%m%d")
        self.storage = DataLakeStorage(base_path=base_path)

        # Wikimedia's policy mirrors SEC EDGAR's: no key required, but
        # blocks requests without a descriptive User-Agent identifying
        # the caller. Reuse the same env var so you don't need a second one.
        import os
        user_agent = os.environ.get("SEC_EDGAR_USER_AGENT")
        if not user_agent:
            raise RuntimeError(
                "SEC_EDGAR_USER_AGENT env var is required (reused here for "
                "Wikimedia's User-Agent policy too) — set it to something "
                "like 'nova_pipe choonhong@example.com'."
            )
        self.headers = {"User-Agent": user_agent}

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        article = TICKER_TO_WIKI_ARTICLE.get(ticker)
        if article is None:
            logger.warning(f"[wikipedia_pageviews] No article mapped for {ticker} — add it to TICKER_TO_WIKI_ARTICLE")
            return None

        logger.info(f"[wikipedia_pageviews] Fetching pageviews for {ticker} ({article})")

        # Wikimedia's own API examples use literal, unencoded parentheses in
        # article titles (e.g. 'Python_(programming_language)') — encoding
        # them as %28/%29 makes their router 404. Only encode characters
        # that are genuinely unsafe in a URL path; leave ()!'*, etc. as-is.
        url = WIKI_PAGEVIEWS_URL.format(
            article=quote(article, safe="()!*'"), start=self.start_date, end=self.end_date
        )

        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error(f"[wikipedia_pageviews] Fetch failed for {ticker}: {e}")
            return None

        items = payload.get("items", [])
        if not items:
            logger.warning(f"[wikipedia_pageviews] No data returned for {ticker} ({article})")
            return None

        df = pd.DataFrame(items)
        df["date"] = pd.to_datetime(df["timestamp"].str[:8], format="%Y%m%d")
        df = df[["date", "views"]]
        df["Ticker"] = ticker
        df["Wiki_Article"] = article
        df["Asset_Class"] = asset_class

        time.sleep(0.3)  # polite pacing, no documented hard limit for this endpoint
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/wikipedia_pageviews_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)