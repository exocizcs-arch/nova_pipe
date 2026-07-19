import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

from storage import DataLakeStorage

logger = logging.getLogger(__name__)

SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"


class EdgarExtractor:
    """Pulls insider-trading (Form 4) and material-event (8-K) filing
    metadata from SEC EDGAR. Free, public domain, no API key — but SEC
    requires a descriptive User-Agent identifying who's calling, or it
    will start returning 403s.

    Same shape as YFinanceExtractor / AlpacaExtractor so the orchestration
    flow can treat every source uniformly: fetch_single_ticker() -> df,
    save(df, ticker, asset_class) -> parquet in the landing zone.
    """

    _ticker_cik_map = None  # class-level cache: one lookup per process, not per ticker

    def __init__(self, lookback_years=1, base_path="/app/data/general_data/landing_zone/edgar"):
        # Intentionally short default lookback: unlike price history, what
        # matters here is recent insider activity, not 5 years of filings.
        self.lookback_years = lookback_years
        self.start_date = datetime.now(timezone.utc) - timedelta(days=lookback_years * 365)
        self.storage = DataLakeStorage(base_path=base_path)

        user_agent = os.environ.get("SEC_EDGAR_USER_AGENT")
        if not user_agent:
            raise RuntimeError(
                "SEC_EDGAR_USER_AGENT env var is required, e.g. "
                "'nova_pipe choonhong@example.com' — SEC blocks requests "
                "without a descriptive User-Agent."
            )
        self.headers = {"User-Agent": user_agent}

    def _load_ticker_cik_map(self) -> dict:
        """SEC indexes filings by CIK, not ticker, so we need this lookup
        once per run. Cached at the class level since it's ~8000 rows and
        identical for every ticker in the same process."""
        if EdgarExtractor._ticker_cik_map is not None:
            return EdgarExtractor._ticker_cik_map

        resp = requests.get(SEC_TICKER_MAP_URL, headers=self.headers, timeout=15)
        resp.raise_for_status()
        raw = resp.json()

        mapping = {row["ticker"].upper(): str(row["cik_str"]).zfill(10) for row in raw.values()}
        EdgarExtractor._ticker_cik_map = mapping
        return mapping

    def _get_cik(self, ticker: str) -> str | None:
        cik = self._load_ticker_cik_map().get(ticker.upper())
        if cik is None:
            logger.warning(f"[edgar] No CIK found for {ticker}")
        return cik

    def fetch_single_ticker(self, ticker: str, asset_class: str = "stocks") -> pd.DataFrame | None:
        logger.info(f"[edgar] Fetching insider/material-event filings for {ticker}")

        cik = self._get_cik(ticker)
        if cik is None:
            return None

        try:
            resp = requests.get(SEC_SUBMISSIONS_URL.format(cik=cik), headers=self.headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"[edgar] Submissions fetch failed for {ticker}: {e}")
            return None

        recent = data.get("filings", {}).get("recent", {})
        if not recent or "form" not in recent:
            logger.warning(f"[edgar] No filings found for {ticker}")
            return None

        df = pd.DataFrame(recent)
        df["filingDate"] = pd.to_datetime(df["filingDate"])
        df = df[df["filingDate"] >= self.start_date.replace(tzinfo=None)]

        # Form 4 = insider buy/sell transactions, 8-K = material corporate
        # events. Highest-signal, lowest-noise filing types for trading use.
        df = df[df["form"].isin(["4", "8-K"])].copy()
        if df.empty:
            logger.info(f"[edgar] No Form 4 / 8-K filings in lookback window for {ticker}")
            return None

        df["Ticker"] = ticker
        df["Asset_Class"] = asset_class
        df["cik"] = cik
        df["filing_url"] = df["accessionNumber"].apply(
            lambda acc: (
                f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
                f"{acc.replace('-', '')}/{acc}-index.htm"
            )
        )

        keep_cols = [
            "Ticker", "Asset_Class", "cik", "form", "filingDate",
            "reportDate", "accessionNumber", "primaryDocument", "filing_url",
        ]
        df = df[[c for c in keep_cols if c in df.columns]].reset_index(drop=True)

        time.sleep(0.15)  # stay well under SEC's documented 10 req/sec fair-use limit
        return df

    def save(self, df: pd.DataFrame, ticker: str, asset_class: str):
        partition = f"{asset_class}/{ticker}/edgar_{datetime.today().strftime('%Y%m%d')}.parquet"
        self.storage.save_to_parquet(df=df, filename=partition)