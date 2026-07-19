import json
import os
import sys

from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger

from stooq_extractor import StooqExtractor
from coingecko_extractor import CoinGeckoExtractor
from alpha_vantage_extractor import AlphaVantageExtractor
from fmp_extractor import FMPExtractor
from stocktwits_extractor import StockTwitsExtractor
from alpha_vantage_news_extractor import AlphaVantageNewsExtractor
from gdelt_extractor import GDELTExtractor
from rss_extractor import RSSExtractor
from google_trends_extractor import GoogleTrendsExtractor
from bls_extractor import BLSExtractor

DEFAULT_CONFIG = str(Path(__file__).resolve().parent.parent / "config.json")

# Ticker-list keys only — mirrors PRICE_ASSET_CLASSES in prefect_flow.py so
# stooq_flow doesn't mistake fred_series/bls_series/rss_feeds (dicts, not
# ticker lists) for an asset class.
PRICE_ASSET_CLASSES = ["stocks", "forex", "crypto"]


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Tasks — one per source, same retry/error pattern as prefect_flow.py
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=10, persist_result=True)
def run_stooq_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = StooqExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[stooq] Saved {len(df)} rows for {ticker}")
    return df is not None


@task(retries=3, retry_delay_seconds=30, persist_result=True)
def run_coingecko_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = CoinGeckoExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[coingecko] Saved {len(df)} rows for {ticker}")
    return df is not None


@task(retries=2, retry_delay_seconds=60, persist_result=True)
def run_alpha_vantage_ticker(ticker: str, asset_class: str):
    logger = get_run_logger()
    extractor = AlphaVantageExtractor()
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[alpha_vantage] Saved overview for {ticker}")
    return df is not None


@task(retries=3, retry_delay_seconds=15, persist_result=True)
def run_fmp_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = FMPExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[fmp] Saved {len(df)} rows for {ticker}")
    return df is not None


@task(retries=3, retry_delay_seconds=10, persist_result=True)
def run_stocktwits_ticker(ticker: str, asset_class: str):
    logger = get_run_logger()
    extractor = StockTwitsExtractor()
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[stocktwits] Saved {len(df)} messages for {ticker}")
    return df is not None


@task(retries=2, retry_delay_seconds=60, persist_result=True)
def run_alpha_vantage_news_ticker(ticker: str, asset_class: str):
    logger = get_run_logger()
    extractor = AlphaVantageNewsExtractor()
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[alpha_vantage_news] Saved {len(df)} articles for {ticker}")
    return df is not None


@task(retries=2, retry_delay_seconds=30, persist_result=True)
def run_gdelt_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = GDELTExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[gdelt] Saved {len(df)} rows for {ticker}")
    return df is not None


@task(retries=2, retry_delay_seconds=10, persist_result=True)
def run_rss_feed(feed_name: str, feed_url: str, asset_class: str):
    logger = get_run_logger()
    extractor = RSSExtractor()
    df = extractor.fetch_single_ticker(feed_url, asset_class)
    if df is not None:
        extractor.save(df, feed_name, asset_class)
        logger.info(f"[rss] Saved {len(df)} entries for {feed_name}")
    return df is not None


@task(retries=2, retry_delay_seconds=30, persist_result=True)
def run_google_trends_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = GoogleTrendsExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[google_trends] Saved {len(df)} rows for {ticker}")
    return df is not None


@task(retries=3, retry_delay_seconds=20, persist_result=True)
def run_bls_series(series_id: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = BLSExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(series_id, asset_class)
    if df is not None:
        extractor.save(df, series_id, asset_class)
        logger.info(f"[bls] Saved {len(df)} observations for {series_id}")
    return df is not None


# ---------------------------------------------------------------------------
# Flows — one per source, all reading from the single unified config.json
# (ticker lists, fred_series, bls_series, and rss_feeds all live there)
# ---------------------------------------------------------------------------

@flow(name="stooq-extraction-flow", log_prints=True)
def stooq_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 5):
    config = load_config(config_path)
    results = [
        run_stooq_ticker(ticker, asset_class, lookback_years)
        for asset_class in PRICE_ASSET_CLASSES
        for ticker in config.get(asset_class, [])
    ]
    print(f"stooq flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="coingecko-extraction-flow", log_prints=True)
def coingecko_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 5):
    config = load_config(config_path)
    results = [
        run_coingecko_ticker(ticker, "crypto", lookback_years)
        for ticker in config.get("crypto", [])
    ]
    print(f"coingecko flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="alpha-vantage-extraction-flow", log_prints=True)
def alpha_vantage_flow(config_path: str = DEFAULT_CONFIG):
    config = load_config(config_path)
    # 25 req/day free tier — only ever run this against "stocks", not
    # crypto/forex, and expect to only cover a handful of tickers per day.
    results = [
        run_alpha_vantage_ticker(ticker, "stocks")
        for ticker in config.get("stocks", [])
    ]
    print(f"alpha_vantage flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="fmp-extraction-flow", log_prints=True)
def fmp_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 5):
    config = load_config(config_path)
    results = [
        run_fmp_ticker(ticker, "stocks", lookback_years)
        for ticker in config.get("stocks", [])
    ]
    print(f"fmp flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="stocktwits-extraction-flow", log_prints=True)
def stocktwits_flow(config_path: str = DEFAULT_CONFIG):
    config = load_config(config_path)
    results = [
        run_stocktwits_ticker(ticker, "stocks")
        for ticker in config.get("stocks", [])
    ]
    print(f"stocktwits flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="alpha-vantage-news-extraction-flow", log_prints=True)
def alpha_vantage_news_flow(config_path: str = DEFAULT_CONFIG):
    config = load_config(config_path)
    # Same 25 req/day budget as the OVERVIEW flow — between the two, that's
    # roughly 12-13 tickers/day total if both run daily, so keep your
    # stocks list short or stagger which flow runs which day.
    results = [
        run_alpha_vantage_news_ticker(ticker, "stocks")
        for ticker in config.get("stocks", [])
    ]
    print(f"alpha_vantage_news flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="gdelt-extraction-flow", log_prints=True)
def gdelt_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 1):
    config = load_config(config_path)
    results = [
        run_gdelt_ticker(ticker, "stocks", lookback_years)
        for ticker in config.get("stocks", [])
    ]
    print(f"gdelt flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="rss-extraction-flow", log_prints=True)
def rss_flow(config_path: str = DEFAULT_CONFIG):
    config = load_config(config_path)
    rss_config = config.get("rss_feeds", {})
    results = [
        run_rss_feed(feed_name, feed_url, asset_class)
        for asset_class, feeds in rss_config.items()
        for feed_name, feed_url in feeds.items()
    ]
    print(f"rss flow complete: {sum(results)}/{len(results)} feeds succeeded")


@flow(name="google-trends-extraction-flow", log_prints=True)
def google_trends_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 1):
    config = load_config(config_path)
    results = [
        run_google_trends_ticker(ticker, "stocks", lookback_years)
        for ticker in config.get("stocks", [])
    ]
    print(f"google_trends flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="bls-extraction-flow", log_prints=True)
def bls_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 10):
    config = load_config(config_path)
    bls_config = config.get("bls_series", {})
    results = [
        run_bls_series(series_id, asset_class, lookback_years)
        for asset_class, series_ids in bls_config.items()
        for series_id in series_ids
    ]
    print(f"bls flow complete: {sum(results)}/{len(results)} series succeeded")


# ---------------------------------------------------------------------------
# Parent flow — each sub-flow is wrapped in its own try/except so one
# rate-limited or misconfigured source (e.g. missing FMP_API_KEY) never
# blocks the rest of the run.
# ---------------------------------------------------------------------------

@flow(name="alt-data-extraction-parent", log_prints=True)
def alt_data_extraction_parent(price_lookback_years: int = 5, macro_lookback_years: int = 10,
                                trends_lookback_years: int = 1):
    logger = get_run_logger()

    sub_flows = [
        ("stooq", lambda: stooq_flow(lookback_years=price_lookback_years)),
        ("coingecko", lambda: coingecko_flow(lookback_years=price_lookback_years)),
        ("alpha_vantage", alpha_vantage_flow),
        ("fmp", lambda: fmp_flow(lookback_years=price_lookback_years)),
        ("stocktwits", stocktwits_flow),
        ("alpha_vantage_news", alpha_vantage_news_flow),
        ("gdelt", lambda: gdelt_flow(lookback_years=trends_lookback_years)),
        ("rss", rss_flow),
        ("google_trends", lambda: google_trends_flow(lookback_years=trends_lookback_years)),
        ("bls", lambda: bls_flow(lookback_years=macro_lookback_years)),
    ]

    for name, run in sub_flows:
        try:
            logger.info(f"Attempting {name} extraction...")
            run()
        except Exception as e:
            logger.warning(f"{name} extraction skipped or failed: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--deploy":
        print("Registering alt-data deployment with Prefect...")
        alt_data_extraction_parent.from_source(
            source="/app/pipeline/flows",
            entrypoint="alt_data_flow.py:alt_data_extraction_parent"
        ).deploy(
            name="daily-alt-data-sync",
            work_pool_name="default-agent-pool",
            cron="0 7 * * *",  # once a day, after the fundamentals/macro sync
            build=False,
            push=False,
            parameters={"price_lookback_years": 5, "macro_lookback_years": 10, "trends_lookback_years": 1}
        )
    else:
        print("Running alt-data pipeline manually...")
        alt_data_extraction_parent(price_lookback_years=5, macro_lookback_years=10, trends_lookback_years=1)