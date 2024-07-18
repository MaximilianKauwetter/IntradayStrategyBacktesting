import os
from datetime import datetime, date

import pandas as pd

from Backtesting.BacktestResult import BacktestResult
from . import BaseDataStore
from ..DataDownloader import DataDownloader


class LocalDataStore(BaseDataStore):
    AGG_DATA_FOLDER: str = f"{os.path.dirname(__file__)}/../../Data/AggregatedCSVs"
    BACKTEST_FOLDER: str = f"{os.path.dirname(__file__)}/../../Data/Backtest"

    def __init__(self, ticker: str, num_threads: int = 1):
        super().__init__(ticker, num_threads=num_threads)
        self.file_path_ts = f"{LocalDataStore.AGG_DATA_FOLDER}/{self.ticker}_ts.csv"

    def ts_file_exists(self) -> bool:
        return os.path.exists(self.file_path_ts)

    def download_ts(self) -> pd.DataFrame:
        start_time = datetime.now()
        print(f"Start local download at <{start_time}>")
        df = pd.read_csv(
            filepath_or_buffer=self.file_path_ts,
            sep=DataDownloader.SEPARATOR,
            parse_dates=["date"],
            index_col="date",
        )
        end_time = datetime.now()
        print(f"End local download at <{end_time}> within <{end_time - start_time}>")
        return df

    def upload_ts(self, df: pd.DataFrame) -> None:
        start_time = datetime.now()
        print(f"Start local upload at <{start_time}>")
        df.to_csv(path_or_buf=self.file_path_ts, index=True, sep=DataDownloader.SEPARATOR)
        end_time = datetime.now()
        print(f"End local upload at <{end_time}> within <{end_time - start_time}>")

    def download_backtest(self, start_date: date, end_date: date, strategy_name: str) -> pd.DataFrame:
        name = f"{self.ticker}-{start_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{end_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{strategy_name}"
        start_time = datetime.now()
        print(f"Start backtest local download at <{start_time}>")
        df_ts = pd.read_csv(
            filepath_or_buffer=f"{LocalDataStore.BACKTEST_FOLDER}/{name}|ts.csv",
            sep=DataDownloader.SEPARATOR,
            parse_dates=["date"],
            index_col="date",
        )
        end_time = datetime.now()
        print(f"End backtest local downloads at <{end_time}> within <{end_time - start_time}>")
        return df_ts

    def upload_backtest(self, backtest_result: BacktestResult) -> None:
        name = (
            f"{self.ticker}-{backtest_result.start_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{backtest_result.end_date.strftime(DataDownloader.DATE_FILE_FORMAT)}-{backtest_result.strategy_name}"
        )
        start_time = datetime.now()
        print(f"Start backtest local upload at <{start_time}>")
        backtest_result.to_info_df().to_csv(path_or_buf=f"{LocalDataStore.BACKTEST_FOLDER}/{name}-info.csv", index=True, sep=DataDownloader.SEPARATOR)
        backtest_result.to_daily().to_csv(path_or_buf=f"{LocalDataStore.BACKTEST_FOLDER}/{name}-daily.csv", index=True, sep=DataDownloader.SEPARATOR)
        backtest_result.to_ts_df().to_csv(path_or_buf=f"{LocalDataStore.BACKTEST_FOLDER}/{name}-ts.csv", index=True, sep=DataDownloader.SEPARATOR)
        end_time = datetime.now()
        print(f"End backtest local upload at <{end_time}> within <{end_time - start_time}>")
