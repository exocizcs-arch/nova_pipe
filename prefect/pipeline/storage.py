import os
import tempfile
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DataLakeStorage:
    """
    A generic storage handler for writing data to the Data Lake.
    Can be imported and used by any future extraction script.
    """

    # UPDATE: Changed the default base_path to match the Docker volume mount
    def __init__(self, base_path="/app/data/general_data/landing_zone"):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    def save_to_parquet(self, df: pd.DataFrame, filename: str) -> bool:
        """
        Saves a pandas DataFrame to a Parquet file, creating any
        nested partition directories (e.g. 'equities/AAPL/file.parquet').
        Writes atomically via a temp file + rename so a crash mid-write
        never leaves a corrupt file at the destination path.

        Raises on failure rather than swallowing the exception, so
        callers (e.g. Prefect tasks) can retry.
        """
        if df is None or df.empty:
            logger.warning("No data provided to save — skipping.")
            return False

        file_path = os.path.join(self.base_path, filename)
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)

        fd, tmp_path = tempfile.mkstemp(suffix=".parquet.tmp", dir=dir_path)
        os.close(fd)

        try:
            df.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, file_path)
            logger.info(f"Data successfully saved to {file_path} ({len(df)} rows)")
            return True
        except Exception as e:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            logger.error(f"Failed to save Parquet file at {file_path}: {e}")
            raise