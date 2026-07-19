import logging
import feedparser
import pandas as pd
from datetime import datetime

from storage import DataLakeStorage

logger = logging.getLogger(__name__)


class RSSExtractor:
    """Pulls headlines from financial news RSS feeds. Free, no key, no
    auth. Unlike the ticker-based extractors, the first argument here is
    a feed URL, not a ticker — kept as the same fetch_single_ticker/save
    shape so the orchestration flow can treat it uniformly with every
    other source.
    """

    def __init__(self, lookback_years=None, base_path="/app/data/general_data/landing_zone/rss"):
        self.storage = DataLakeStorage(base_path=base_path)

    def fetch_single_ticker(self, feed_url: str, asset_class: str = "news") -> pd.DataFrame | None:
        logger.info(f"[rss] Fetching feed {feed_url}")

        try:
            parsed = feedparser.parse(feed_url)
        except Exception as e:
            logger.error(f"[rss] Fetch failed for {feed_url}: {e}")
            return None

        entries = parsed.entries
        if not entries:
            logger.warning(f"[rss] No entries returned for {feed_url} (feed URL may have changed)")
            return None

        rows = [{
            "title": e.get("title"),
            "link": e.get("link"),
            "published": e.get("published"),
            "summary": e.get("summary"),
        } for e in entries]

        df = pd.DataFrame(rows)
        df["published"] = pd.to_datetime(df["published"], errors="coerce")
        df["Feed_Url"] = feed_url
        df["Asset_Class"] = asset_class

        return df

    def save(self, df: pd.DataFrame, feed_name: str, asset_class: str):
        partition = f"{asset_class}/{feed_name}/rss_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)