from abc import ABC, abstractmethod
from datetime import date, datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from ..DataDownloader import DataDownloader
from ..DataFile import DataFile
from Backtesting.BacktestResult import BacktestResult


class BaseDataStore(ABC):
    def __init__(self, ticker: str, num_threads: int = 1):
        self.ticker = ticker
        self.num_threads = num_threads

    @abstractmethod
    def ts_file_exists(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def download_ts(self) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def upload_ts(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    @abstractmethod
    def download_backtest(self, start_date, end_date, strategy_name) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def upload_backtest(self, backtest_result: BacktestResult) -> None:
        raise NotImplementedError

    def create_datafile(self, start_date: date = None, end_date: date = None) -> DataFile:
        if self.ts_file_exists():
            df_ts = self.download_ts()
            print(f"Stored Ts has {len(df_ts)} rows")
            dates = df_ts.index
            df_first_date, df_last_date = dates[0].date(), dates[-1].date()
            if start_date < df_first_date or df_last_date < end_date:
                # build new bigger AggregateDF and store
                df_ts = DataDownloader.download_data(
                    ticker=self.ticker,
                    start_date=start_date,
                    end_date=end_date,
                    df=df_ts,
                    num_threads=self.num_threads,
                )
                self.upload_ts(df=df_ts)
            start_date, end_date = pd.to_datetime((start_date, end_date + relativedelta(days=1, microseconds=-1)))
            df_ts = df_ts.loc[(start_date <= df_ts.index) & (df_ts.index <= end_date)]
        else:
            df_ts = DataDownloader.download_data(
                ticker=self.ticker,
                start_date=start_date,
                end_date=end_date,
                num_threads=self.num_threads,
            )
            self.upload_ts(df=df_ts)
        print(f"Datafile has been created with {len(df_ts)} number of rows starting at <{df_ts.index[0]}> and ending at {df_ts.index[-1]}")
        df_ts.dropna(how="any", axis="rows", inplace=True)
        return DataFile(ticker=self.ticker, df_ts=df_ts)

    def create_backtest_result(self, ticker: str, start_date: date, end_date: date, strategy_name: str) -> BacktestResult:
        df_ts = self.download_backtest(start_date=start_date, end_date=end_date, strategy_name=strategy_name)
        return BacktestResult(
            ticker=ticker,
            weights=df_ts.loc["weights"],
            performance_rel=df_ts.loc["rel_performance"],
            strategy_name=strategy_name,
        )
