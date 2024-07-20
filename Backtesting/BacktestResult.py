import io
import time
from datetime import date, datetime

import pandas as pd

from DataDownload.DataFile import DataFile
import matplotlib.pyplot as plt


class BacktestResult:
    def __init__(
        self,
        ticker: str,
        df_ts: None | pd.DataFrame = None,
        weights: None | pd.Series = None,
        performance_rel: None | pd.Series = None,
        strategy_name: str = "NoDefStrat",
        df_daily: pd.DataFrame = None,
        df_info: pd.DataFrame = None,
    ):
        self.ticker: str = ticker
        self.strategy_name: str = strategy_name

        # ts
        if df_ts is not None:
            self.weights: pd.Series = df_ts["weights"]
            self.performance_rel: pd.Series = df_ts["performance_rel"]
        elif performance_rel is not None:
            self.weights: pd.Series = weights if weights is not None else pd.Series()
            self.performance_rel: pd.Series = performance_rel
        else:
            self.weights: pd.Series = pd.Series()
            self.performance_rel = pd.Series()

        # daily
        if not self.performance_rel.empty:
            grouped_last = self.performance_rel.groupby(self.performance_rel.index.date).last()
            self.daily_performance = grouped_last.divide(grouped_last.shift(1)).add(-1)
            self.daily_performance.iloc[0] = grouped_last.iloc[0] - 1
            positive_days = self.daily_performance[self.daily_performance.gt(0)]
            neutral_days = self.daily_performance[self.daily_performance.eq(0)]
            negative_days = self.daily_performance[self.daily_performance.lt(0)]

            # info
            self.start_at: date = self.performance_rel.index[0].date()
            self.end_date: date = self.performance_rel.index[-1].date()
            self.num_days: int = len(self.daily_performance)
            self.number_neutral_days = len(neutral_days)
            self.total_return = self.performance_rel.iloc[-1] - 1
            self.min_daily_return = self.daily_performance.min()
            self.average_daily_return = self.daily_performance.mean()
            self.max_daily_return = self.daily_performance.max()

            self.number_positive_days = len(positive_days)
            self.average_positive_days_return = positive_days.mean()
            self.number_negative_days = len(negative_days)
            self.average_negative_days_return = negative_days.mean()

            ret = self.performance_rel.pct_change()
            self.var = ret.var()
            self.std_dev = ret.std()
        else:
            # Developed by Maximilian Kauwetter
            self.daily_performance = df_daily.loc[:, "daily_performance"]

            # info
            self.start_at: date = df_info.loc["start_date", strategy_name]
            self.end_date: date = df_info.loc["end_date", strategy_name]
            self.num_days: int = df_info.loc["num_days", strategy_name]
            self.number_neutral_days = df_info.loc["number_neutral_days", strategy_name]
            self.total_return = df_info.loc["total_return", strategy_name]
            self.min_daily_return = df_info.loc["min_daily_return", strategy_name]
            self.average_daily_return = df_info.loc["average_daily_return", strategy_name]
            self.max_daily_return = df_info.loc["max_daily_return", strategy_name]

            self.number_positive_days = df_info.loc["number_positive_days", strategy_name]
            self.average_positive_days_return = df_info.loc["average_positive_days_return", strategy_name]
            self.number_negative_days = df_info.loc["number_negative_days", strategy_name]
            self.average_negative_days_return = df_info.loc["average_negative_days_return", strategy_name]

            self.var = df_info.loc["var", strategy_name]
            self.std_dev = df_info.loc["std_dev", strategy_name]

    def to_ts_df(self) -> pd.DataFrame:
        df = pd.concat([self.weights, self.performance_rel], axis="columns", ignore_index=False).rename(columns={0: "weights", 1: "performance_rel"})
        df.index.name = "date"
        return df

    def to_daily(self) -> pd.DataFrame:
        df = self.daily_performance.to_frame("daily_performance")
        df.index.name = "date"
        return df

    def to_info_df(self) -> pd.DataFrame:
        return pd.Series(
            dict(
                start_date=self.start_at,
                end_date=self.end_date,
                num_days=self.num_days,
                number_neutral_days=self.number_neutral_days,
                total_return=self.total_return,
                min_daily_return=self.min_daily_return,
                average_daily_return=self.average_daily_return,
                max_daily_return=self.max_daily_return,
                number_positive_days=self.number_positive_days,
                average_positive_days_return=self.average_positive_days_return,
                number_negative_days=self.number_negative_days,
                average_negative_days_return=self.average_negative_days_return,
                var=self.var,
                std_dev=self.std_dev,
            )
        ).to_frame(self.strategy_name)

    def plot(self):
        start_time = datetime.now()
        print(f"Plot AggBacktest at <{start_time}>")
        print("<>")

        bytes = io.BytesIO()
        plt.show(bytes, format="png")

    @staticmethod
    def from_data_file(data_file: DataFile, start_date: date | datetime = None, end_date: date | datetime = None) -> "BacktestResult":
        print(f"Create BacktestResult from DataFile for:{data_file.ticker}")
        start: int = int(data_file.index.get_indexer(pd.Index([start_date]), method="bfill")[0])
        end: int = int(data_file.index.get_indexer(pd.Index([end_date]), method="ffill")[0])
        ts = data_file.mid.iloc[start : end + 1]
        performance_rel = ts / ts.iloc[0]
        return BacktestResult(ticker=data_file.ticker, performance_rel=performance_rel, strategy_name=f"Benchmark:{data_file.ticker}")
