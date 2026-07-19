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
from edgar_extractor import EdgarExtractor
from fred_extractor import FredExtractor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = str(Path(__file__).resolve().parent.parent / "config.json")

# Everything (tickers, FRED series, BLS series, RSS feeds) now lives in one
# config.json. PRICE_ASSET_CLASSES exists so price flows only ever iterate
# the ticker-list keys and never mistake fred_series/bls_series/rss_feeds
# (which are dicts, not ticker lists) for an asset class.
PRICE_ASSET_CLASSES = ["stocks", "forex", "crypto"]


def load_config(path: str) -> dict:
    with open(path, 'r') as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# Price data tasks (unchanged)
# ---------------------------------------------------------------------------

@task(
    retries=4,
    retry_delay_seconds=lambda attempt: min(60, (2 ** attempt) + random.uniform(0, 1)),
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


# ---------------------------------------------------------------------------
# New: fundamentals / macro tasks
# ---------------------------------------------------------------------------

@task(
    retries=3,
    retry_delay_seconds=15,
    persist_result=True
)
def run_edgar_ticker(ticker: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = EdgarExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(ticker, asset_class)
    if df is not None:
        extractor.save(df, ticker, asset_class)
        logger.info(f"[edgar] Saved {len(df)} filings for {ticker}")
    return df is not None


@task(
    retries=3,
    retry_delay_seconds=15,
    persist_result=True
)
def run_fred_series(series_id: str, asset_class: str, lookback_years: int):
    logger = get_run_logger()
    extractor = FredExtractor(lookback_years=lookback_years)
    df = extractor.fetch_single_ticker(series_id, asset_class)
    if df is not None:
        extractor.save(df, series_id, asset_class)
        logger.info(f"[fred] Saved {len(df)} observations for {series_id}")
    return df is not None


# ---------------------------------------------------------------------------
# dbt / sync tasks (unchanged)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Price flows (unchanged) — 10-min cadence
# ---------------------------------------------------------------------------

@flow(name="yfinance-extraction-flow", log_prints=True)
def yfinance_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 5):
    config = load_config(config_path)
    results = []
    for asset_class in PRICE_ASSET_CLASSES:
        for ticker in config.get(asset_class, []):
            results.append(run_yfinance_ticker(ticker, asset_class, lookback_years))
    print(f"yfinance flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="alpaca-extraction-flow", log_prints=True)
def alpaca_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 5):
    config = load_config(config_path)
    # Alpaca's free IEX feed only covers US equities.
    results = [
        run_alpaca_ticker(ticker, "stocks", lookback_years)
        for ticker in config.get("stocks", [])
    ]
    print(f"alpaca flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="market-data-extraction-parent", log_prints=True)
def run_all_sources(lookback_years: int = 5):
    """Parent flow — this is what you schedule in Prefect."""
    yfinance_flow(lookback_years=lookback_years)
    alpaca_flow(lookback_years=lookback_years)


# ---------------------------------------------------------------------------
# New: fundamentals / macro flows — deliberately separate from the 10-min
# price sync. Insider filings and macro releases don't change minute to
# minute, so hammering these on the same schedule as price data just burns
# your rate-limit budget for nothing.
# ---------------------------------------------------------------------------

@flow(name="edgar-extraction-flow", log_prints=True)
def edgar_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 1):
    config = load_config(config_path)
    results = []
    # Insider filings only make sense for equities — reuse the same
    # "stocks" ticker list your price extractors already use.
    for ticker in config.get("stocks", []):
        results.append(run_edgar_ticker(ticker, "stocks", lookback_years))
    print(f"edgar flow complete: {sum(results)}/{len(results)} tickers succeeded")


@flow(name="fred-extraction-flow", log_prints=True)
def fred_flow(config_path: str = DEFAULT_CONFIG, lookback_years: int = 10):
    config = load_config(config_path)
    fred_config = config.get("fred_series", {})
    results = []
    for asset_class, series_ids in fred_config.items():
        for series_id in series_ids:
            results.append(run_fred_series(series_id, asset_class, lookback_years))
    print(f"fred flow complete: {sum(results)}/{len(results)} series succeeded")


@flow(name="fundamentals-and-macro-parent", log_prints=True)
def fundamentals_and_macro_flow(price_lookback_years: int = 1, macro_lookback_years: int = 10):
    """Parent flow for the daily/low-frequency sources. Schedule this
    separately from run_all_sources — e.g. once a day rather than every
    10 minutes."""
    logger = get_run_logger()

    try:
        edgar_flow(lookback_years=price_lookback_years)
    except Exception as e:
        logger.warning(f"EDGAR extraction skipped or failed: {e}")

    try:
        fred_flow(lookback_years=macro_lookback_years)
    except Exception as e:
        logger.warning(f"FRED extraction skipped or failed: {e}")

    # Reuse the same dbt + Hostinger sync tasks — add new staging models for
    # edgar/fred sources in dbt_project/models before relying on this in prod.
    run_dbt_models()


# ---------------------------------------------------------------------------
# Master flow (price sources unchanged; fundamentals/macro kept independent)
# ---------------------------------------------------------------------------

@flow(name="Master ELT Pipeline")
def master_elt_flow(lookback_years: int = 5):
    logger = get_run_logger()

    successful_pulls = 0

    try:
        logger.info("Attempting Alpaca extraction...")
        alpaca_flow(lookback_years=lookback_years)
        successful_pulls += 1
    except Exception as e:
        if "429" in str(e) or "rate limit" in str(e).lower():
            logger.warning("Alpaca rate limit hit! Skipping dynamically for this run.")
        else:
            logger.error(f"Alpaca failed due to an unexpected error: {e}")

    try:
        logger.info("Attempting YFinance extraction...")
        yfinance_flow(lookback_years=lookback_years)
        successful_pulls += 1
    except Exception as e:
        if "429" in str(e) or "too many requests" in str(e).lower():
            logger.warning("YFinance rate limit hit! Skipping dynamically for this run.")
        else:
            logger.warning(f"YFinance skipped or failed: {e}")

    if successful_pulls > 0:
        logger.info("Proceeding with transformation and sync for successful data streams.")
        run_dbt_models()
        push_to_hostinger()
    else:
        logger.warning("All data sources skipped or rate-limited this turn. Nothing new to process.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--deploy":
        print("Registering deployments with Prefect...")

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

        fundamentals_and_macro_flow.from_source(
            source="/app/pipeline/flows",
            entrypoint="prefect_flow.py:fundamentals_and_macro_flow"
        ).deploy(
            name="daily-fundamentals-macro-sync",
            work_pool_name="default-agent-pool",
            cron="0 6 * * *",  # once a day, 06:00 UTC — well after market data has landed
            build=False,
            push=False,
            parameters={"price_lookback_years": 1, "macro_lookback_years": 10}
        )
    else:
        print("Running pipeline manually...")
        master_elt_flow(lookback_years=5)
        fundamentals_and_macro_flow(price_lookback_years=1, macro_lookback_years=10)