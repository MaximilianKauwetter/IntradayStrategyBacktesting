import io
import os
from datetime import datetime, date

import pandas as pd
import psutil
from google.cloud import storage
from matplotlib import ticker

from Backtesting.AggBacktestResult import AggBacktestResult
from Backtesting.BacktestResult import BacktestResult
from . import BaseDataStore
from ..DataDownloader import DataDownloader


class BucketDataStore(BaseDataStore):
    AUTHENTICATOR_FILE_PATH: str = f"{os.path.dirname(__file__)}/../../google_cloud_authentication.json"

    def __init__(self, ticker: str, num_threads: int = 1):
        super().__init__(ticker, num_threads)
        if self._has_authentication_json():
            client = storage.Client.from_service_account_json(BucketDataStore.AUTHENTICATOR_FILE_PATH)
        else:
            try:
                client = storage.Client()
            except Exception as _:
                print("No google_cloud_authentication.json could be found and automatic authentication is also not possible, Please use a different DataStore type")
                exit()
        self.bucket = client.get_bucket("mkauwetter-datascience-bucket")
        self.data_blob = self.bucket.blob(f"Data/{ticker}.csv")

    @staticmethod
    def _has_authentication_json() -> bool:
        return os.path.exists(BucketDataStore.AUTHENTICATOR_FILE_PATH)

    def ts_file_exists(self) -> bool:
        return self.data_blob.exists()

    def download_ts(self) -> pd.DataFrame:
        start_time = datetime.now()
        print(f"Start blob download at <{start_time}>")
        df = pd.read_csv(
            io.BytesIO(self.data_blob.download_as_bytes()),
            sep=DataDownloader.SEPARATOR,
            parse_dates=["date"],
            index_col="date",
        )
        end_time = datetime.now()
        print(f"End blob download at <{end_time}> within <{end_time - start_time}>")
        return df

    def upload_ts(self, df: pd.DataFrame) -> None:
        start_time = datetime.now()
        print(f"Start blob upload at <{start_time}>")

        print(f"Memory before upload: {psutil.Process().memory_info().rss/1000000000:,.2f} GB")
        csv = df.to_csv(index=True, sep=DataDownloader.SEPARATOR)
        self.data_blob.upload_from_string(csv, "text/csv", timeout=None)

        end_time = datetime.now()
        print(f"End blob upload at <{end_time}> within <{end_time - start_time}>")

    def download_backtest(self, start_date: date, end_date: date, strategy_name: str) -> dict[str, pd.DataFrame]:
        name = f"{self.ticker}-{start_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{end_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{strategy_name}"
        backtest_ts_blob = self.bucket.blob(f"Backtest/{name}|ts.csv")
        if not backtest_ts_blob.exists():
            raise FileNotFoundError(f"{name} do not exist")
        start_time = datetime.now()
        print(f"Start backtest blobs download at <{start_time}>")
        df_ts = pd.read_csv(
            io.BytesIO(backtest_ts_blob.download_as_bytes()),
            sep=DataDownloader.SEPARATOR,
            parse_dates=["date"],
            index_col="date",
        )
        end_time = datetime.now()
        print(f"End backtest blob downloads at <{end_time}> within <{end_time - start_time}>")
        return dict(ts=df_ts)

    def upload_backtest(self, backtest_result: BacktestResult) -> None:
        name = (
            f"{self.ticker}-{backtest_result.start_at.strftime(DataDownloader.DATE_FILE_FORMAT)}-{backtest_result.end_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{backtest_result.strategy_name}"
        )
        backtest_info_blob = self.bucket.blob(f"Backtest/{name}-info.csv")
        backtest_daily_blob = self.bucket.blob(f"Backtest/{name}-daily.csv")
        backtest_ts_blob = self.bucket.blob(f"Backtest/{name}-ts.csv")
        start_time = datetime.now()
        print(f"Start backtest blobs upload at <{start_time}>")
        backtest_info_blob.upload_from_string(backtest_result.to_info_df().to_csv(index=True, sep=DataDownloader.SEPARATOR), "text/csv", timeout=None)
        backtest_daily_blob.upload_from_string(backtest_result.to_daily().to_csv(index=True, sep=DataDownloader.SEPARATOR), "text/csv", timeout=None)
        backtest_ts_blob.upload_from_string(backtest_result.to_ts_df().to_csv(index=True, sep=DataDownloader.SEPARATOR), "text/csv", timeout=None)
        end_time = datetime.now()
        print(f"End backtest blob upload at <{end_time}> within <{end_time - start_time}>")

    def upload_agg_backtest(self, agg_backtest_result: AggBacktestResult) -> None:
        pass
