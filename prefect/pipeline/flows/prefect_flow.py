import json
import random
import os
import sys
import duckdb
import subprocess

from pathlib import Path
from datetime import timedelta
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

from yfinance_extractor import YFinanceExtractor
from alpaca_extractor import AlpacaExtractor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = str(Path(__file__).resolve().parent.parent / "tickers.json")

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


@task(name="Run dbt Transformations", log_prints=True)
def run_dbt_models():
    print("Starting dbt transformation...")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd="/app/dbt_project",
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception("dbt run failed! Check logs.")

    print("dbt transformation complete!")


@task(name="Sync to Hostinger MySQL", log_prints=True)
def push_to_hostinger():
    print("Reading transformed data from DuckDB...")

    duckdb_path = "/app/data/general_data/analytics.duckdb"
    con = duckdb.connect(duckdb_path, read_only=True)
    clean_df = con.execute("SELECT * FROM stg_combined_stocks").df()
    con.close()

    db_host = os.getenv("HOSTINGER_DB_HOST")
    db_user = os.getenv("HOSTINGER_DB_USER")
    db_pass = os.getenv("HOSTINGER_DB_PASS")
    db_name = os.getenv("HOSTINGER_DB_NAME")

    mysql_url = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
    engine = create_engine(mysql_url)

    print(f"Pushing {len(clean_df)} rows to Hostinger MySQL database...")

    clean_df.to_sql(
        name="daily_stock_prices",
        con=engine,
        if_exists="replace",
        index=False
    )

    print("Successfully synced to Hostinger!")

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
        if asset_class != "stocks":
            print(f"[alpaca] Skipping asset class: {asset_class}")
            continue

        for ticker in tickers:
            results.append(run_alpaca_ticker(ticker, asset_class, lookback_years))
    print(f"alpaca flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="market-data-extraction-parent", log_prints=True)
def run_all_sources(lookback_years: int = 5):
    """Parent flow — this is what you schedule in Prefect."""
    yfinance_flow(lookback_years=lookback_years)
    alpaca_flow(lookback_years=lookback_years)


@flow(name="Master ELT Pipeline")
def master_elt_flow(lookback_years: int = 5):
    logger = get_run_logger()

    successful_pulls = 0

    try:
        logger.info("Attempting Alpaca extraction...")
        run_alpaca_extraction(lookback_years)
        successful_pulls += 1
    except Exception as e:
        if "429" in str(e) or "rate limit" in str(e).lower():
            logger.warning("Alpaca rate limit hit! Skipping dynamically for this run.")
        else:
            logger.error(f"Alpaca failed due to an unexpected error: {e}")

    try:
        logger.info("Attempting YFinance extraction...")
        run_yfinance_extraction(lookback_years)
        successful_pulls += 1
    except Exception as e:
        if "429" in str(e) or "too many requests" in str(e).lower():
            logger.warning("YFinance rate limit hit! Skipping dynamically for this run.")
        else:
            logger.warning(f"YFinance skipped or failed: {e}")

    if successful_pulls > 0:
        logger.info("Proceeding with transformation and sync for successful data streams.")
        run_dbt_transformations()
        push_to_hostinger()
    else:
        logger.warning("All data sources skipped or rate-limited this turn. Nothing new to process.")

if __name__ == "__main__":
    master_elt_flow.from_source(
        source="/app/pipeline/flows",
        entrypoint="prefect_flow.py:master_elt_flow"
    ).deploy(
        name="dynamic-10min-sync",
        work_pool_name="default-agent-pool",
        interval=600,
        build=False,
        push=False,
        parameters={"lookback_years": 5}
    )
