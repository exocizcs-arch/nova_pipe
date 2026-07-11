import json
import random
import os
from datetime import timedelta

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# 1. FIXED IMPORTS: The files are in the same directory
from yfinance_extractor import YFinanceExtractor
from alpaca_extractor import AlpacaExtractor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "tickers.json")

def load_config(path: str) -> dict:
    with open(path, 'r') as f:
        return json.load(f)

@task(
    retries=4,
    retry_delay_seconds=lambda attempt: min(60, (2 ** attempt) + random.uniform(0, 1)),
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=6),
    persist_result=True
)
def run_yfinance_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = YFinanceExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[yfinance] Saved {len(df)} rows for {ticker}")
    return df is not None


@task(
    retries=3,
    retry_delay_seconds=10,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=6),
    persist_result=True
)
def run_alpaca_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = AlpacaExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[alpaca] Saved {len(df)} rows for {ticker}")
    return df is not None


@flow(name="yfinance-extraction-flow", log_prints=True)
def yfinance_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 5):
    config = load_config(config_path)
    results = []
    for asset_class, tickers in config.items():
        for ticker in tickers:
            results.append(run_yfinance_ticker(ticker, asset_class, lookback_years))
    print(f"yfinance flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="alpaca-extraction-flow", log_prints=True)
def alpaca_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 5):
    config = load_config(config_path)
    results = []
    for asset_class, tickers in config.items():
        for ticker in tickers:
            results.append(run_alpaca_ticker(ticker, asset_class, lookback_years))
    print(f"alpaca flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="market-data-extraction-parent", log_prints=True)
def run_all_sources(lookback_years: int = 5):
    """Parent flow — this is what you schedule in Prefect."""
    yfinance_flow(lookback_years=lookback_years)
    alpaca_flow(lookback_years=lookback_years)


if __name__ == "__main__":
    run_all_sources(lookback_years=5)