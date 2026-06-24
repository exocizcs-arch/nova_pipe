import os
import pandas as pd


class DataLakeStorage:
    """
    A generic storage handler for writing data to the Data Lake.
    Can be imported and used by any future extraction script.
    """

    def __init__(self, base_path="/data/landing_zone"):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    def save_to_parquet(self, df: pd.DataFrame, filename: str):
        """
        Saves a pandas DataFrame to a Parquet file.
        """
        if df is None or df.empty:
            print("No data provided to save.")
            return False

        file_path = os.path.join(self.base_path, filename)

        try:
            # index=False prevents pandas from saving the DataFrame index as an unnamed column
            df.to_parquet(file_path, index=False)
            print(f"Data successfully saved to {file_path}")
            return True
        except Exception as e:
            print(f"Failed to save Parquet file: {e}")
            return False