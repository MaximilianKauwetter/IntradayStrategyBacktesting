import io
import os
import threading
import time
from datetime import datetime, date

import numpy as np
import pandas as pd
import psutil
from google.cloud import storage
from matplotlib import ticker

from Backtesting.BacktestResult import BacktestResult
from . import BaseDataStore
from ..DataDownloader import DataDownloader


class SplitBucketDataStore(BaseDataStore):
    AUTHENTICATOR_FILE_PATH: str = f"{os.path.dirname(__file__)}/../../google_cloud_authentication.json"

    def __init__(self, ticker: str, num_threads: int = 1):
        super().__init__(ticker, num_threads)
        if self._has_authentication_json():
            client = storage.Client.from_service_account_json(SplitBucketDataStore.AUTHENTICATOR_FILE_PATH)
        else:
            try:
                client = storage.Client()
            except Exception as _:
                print("No google_cloud_authentication.json could be found and automatic authentication is also not possible, Please use a different DataStore type")
                exit()
        self.bucket = client.get_bucket("mkauwetter-datascience-bucket")

    @staticmethod
    def _has_authentication_json() -> bool:
        return os.path.exists(SplitBucketDataStore.AUTHENTICATOR_FILE_PATH)

    def ts_file_exists(self) -> bool:
        return 0 < len(list(self.bucket.list_blobs(prefix=f"Data/{self.ticker}/")))

    def download_dfs(self, blob_names, final):
        for blob_name in blob_names:
            blob = self.bucket.blob(blob_name)
            final[blob_name] = pd.read_csv(
                io.BytesIO(blob.download_as_bytes()),
                sep=DataDownloader.SEPARATOR,
                parse_dates=["date"],
                index_col="date",
            )

    def download_ts(self) -> pd.DataFrame:
        start_time = datetime.now()
        print(f"Start blob download at <{start_time}>")
        blob_names = [blob.name for blob in self.bucket.list_blobs(prefix=f"Data/{self.ticker}/")]
        total_todo = len(blob_names)
        final: dict = dict.fromkeys(blob_names, None)
        threads = []

        for arr in np.array_split(blob_names, min(self.num_threads, len(blob_names))):
            thread = threading.Thread(target=self.download_dfs, args=(arr, final))
            thread.start()
            threads.append(thread)
        active_threads = 1
        while 0 < active_threads:
            active_threads = sum([thread.is_alive() for thread in threads])
            empty = sum([df is None for df in final.values()])
            print(f"Downloading at <{datetime.now()}> done {total_todo-empty} downloads, {empty} todo , {active_threads} active threads")
            time.sleep(5)

        df = pd.concat(objs=final.values(), axis="index")
        df.sort_index(inplace=True)
        end_time = datetime.now()
        print(f"End blob download at <{end_time}> within <{end_time - start_time}>")
        return df

    def upload_dfs(self, to_upload: dict[str, pd.DataFrame], tracker: dict[str, bool] = None):
        if tracker is None:
            tracker = {}
        for year, df in to_upload.items():
            blob = self.bucket.blob(f"Data/{self.ticker}/{year}.csv")
            blob.upload_from_string(df.to_csv(index=True, sep=DataDownloader.SEPARATOR), content_type="text/csv")
            tracker[year] = True

    def upload_ts(self, df: pd.DataFrame) -> None:
        start_time = datetime.now()
        print(f"Start split blob upload at <{start_time}>")

        groups = df.groupby(df.index.year)
        split = {str(group): groups.get_group(group) for group in groups.groups}
        tracker: dict[str, bool] = dict.fromkeys(split.keys(), False)
        total_todo = len(split)

        threads = []
        for arr in np.array_split(list(split.keys()), self.num_threads):
            thread = threading.Thread(target=self.upload_dfs, args=({k: split[k] for k in arr}, tracker))
            thread.start()
            threads.append(thread)
        active_threads = 1
        while 0 < active_threads:
            active_threads = sum([thread.is_alive() for thread in threads])
            done = sum(tracker.values())
            print(f"Uploading at <{datetime.now()}> done {done} uploads, {total_todo-done} todo , {active_threads} active threads")
            time.sleep(5)
        print(f"\nMemory before upload: {psutil.Process().memory_info().rss/1000000000:.2f} GB")
        end_time = datetime.now()
        print(f"End blob upload at <{end_time}> within <{end_time - start_time}>")

    def download_backtest(self, start_date: date, end_date: date, strategy_name: str) -> pd.DataFrame:
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
        return df_ts

    def upload_backtest(self, backtest_result: BacktestResult) -> None:
        name = (
            f"{self.ticker}-{backtest_result.start_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{backtest_result.end_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{backtest_result.strategy_name}"
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
