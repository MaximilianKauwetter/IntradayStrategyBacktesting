import io
from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt

from .BacktestResult import BacktestResult
from DataDownload.DataFile import DataFile


class AggBacktestResult:
    def __init__(self, backtest_results: list[BacktestResult], initial_invest: float = 100, data_file: DataFile = None, with_ts: bool = True):
        backtest_results = [br for br in backtest_results if br is not None]
        if len(backtest_results) == 0:
            return

        first_dates = [res.start_at for res in backtest_results]
        self.first_date = first_dates[0]
        last_dates = [res.end_date for res in backtest_results]
        self.last_date = last_dates[-1]
        if any([self.first_date != f for f in first_dates]):
            raise ValueError(first_dates)
        if any([self.last_date != l for l in last_dates]):
            raise ValueError(last_dates)

        self.initial_invest = initial_invest

        if data_file is not None:
            self.ticker = data_file.ticker
            backtest_results.append(BacktestResult.from_data_file(data_file, start_date=self.first_date, end_date=self.last_date))
        else:
            self.ticker = backtest_results[0].ticker

        self.strat_names = sorted([res.strategy_name for res in backtest_results])
        self.name = "_".join(self.strat_names)
        self.info = pd.concat([res.to_info_df() for res in backtest_results], axis="columns").transpose()
        self.daily = pd.concat([res.to_daily() for res in backtest_results], axis="columns")
        if with_ts:
            self.performance = pd.concat([self.initial_invest * res.performance_rel.rename(res.strategy_name, inplace=False) for res in backtest_results], axis="columns")
        else:
            self.performance = pd.Series()

    @property
    def plot(self) -> io.BytesIO:
        start_time = datetime.now()
        print(f"Plot AggBacktest at <{start_time}>")
        plt.figure(figsize=(15, 6))

        for strategy_name in self.strat_names:
            plt.plot(self.performance[strategy_name], label=strategy_name)
        plt.legend(loc="upper left", bbox_to_anchor=(0, 1))
        plt.ylabel("Performance")

        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format="png", dpi=500)
        img_bytes.seek(0)

        return img_bytes
