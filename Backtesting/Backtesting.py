import sys
from datetime import datetime, date

import matplotlib.pyplot as plt
import pandas as pd

from DataDownload.DataFile import DataFile
from Backtesting.Strategies.BaseStrategy import BaseStrategy
from Backtesting.BacktestResult import BacktestResult
from DataDownload.DataStore import BaseDataStore


class Backtesting:
    def __init__(self, security: DataFile, strategy: BaseStrategy, initial_investment: float = 100, start_at: datetime | int = 0, threads: int = 1, iterative: bool = False):
        self.security: DataFile = security
        self.strategy: BaseStrategy = strategy
        self.initial_investment: float = initial_investment
        if isinstance(start_at, (date, datetime)):
            start_at: int = int(self.security.index.get_indexer(pd.Index([start_at]), method="bfill")[0])
        if isinstance(start_at, int):
            self.start_at = start_at
        else:
            raise ValueError(f"start_at must be an integer or datetime object, but was {type(start_at)}")
        self.threads = threads
        self.iterative = iterative
        self._weights = None
        self._performance_rel = None
        self._performance = None
        self._result = None

    @property
    def weights(self) -> pd.Series:
        if self._weights is None:
            start_time = datetime.now()
            print(f"Start Backtesting at <{start_time}> ", end="")
            if self.iterative:
                print("iterative")
                ticks_to_eval = self.security.index[self.start_at :].to_list()
                total_num = len(ticks_to_eval)
                print(f"Evaluate {total_num:,} ticks from <{ticks_to_eval[0]}> to <{ticks_to_eval[-1]}>")
                weights: [datetime, None | float] = {}
                print(f"Start Calculation at <{datetime.now()}>")
                for i, dt in enumerate(ticks_to_eval):
                    weights[dt] = self.strategy.get_weight(self.security, dt)
                    if i % 1000 == 0:
                        sys.stdout.write("\r")
                        print(f"Calculating [{100 * (i / total_num):.2f}%][{datetime.now() - start_time}])", end="")
                print(f"Finished calculation in {datetime.now() - start_time}")
                self._weights = pd.Series(data=weights, dtype=float).sort_index()
            else:
                print("non iterative")
                self._weights = self.strategy.get_weights(self.security).iloc[self.start_at :].astype(float)
                self._weights.name = None
            self._weights.ffill(axis="index", inplace=True)
            self._weights.fillna(0, inplace=True)
            end_time = datetime.now()
            print(f"Finished backtesting at <{end_time}>")
            print(f"Calculation duration: <{end_time - start_time}>")
        return self._weights

    def plot_weights(self):
        print("<plot weights>")
        plt.figure(figsize=(15, 6))
        plt.ylim(-0.1, 1.1)
        plt.plot(self.weights, label="weights", color="red")
        plt.fill_between(x=self.weights.index, y1=self.weights, alpha=0.3, color="red")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        plt.ylabel("weight")
        plt.show()

    @property
    def performance_rel(self) -> pd.Series:
        if self._performance_rel is None:
            weighted_return = self.security.pct_returns.multiply(self.weights.shift(1, fill_value=0)).iloc[self.start_at :]
            perf_raw = weighted_return.add(1).cumprod()
            new_invest = self.weights.diff().clip(lower=0)
            new_invest.iloc[0] = self.weights.iloc[0]
            sell_factor = (new_invest.multiply(-self.security.spread.divide(self.security.ask).iloc[self.start_at :])).add(1).cumprod()
            self._performance_rel = perf_raw * sell_factor
        return self._performance_rel

    @property
    def performance(self):
        if self._performance is None:
            self._performance = self.initial_investment * self.performance_rel
        return self._performance

    @property
    def result(self) -> BacktestResult:
        if self._result is None:
            self._result = BacktestResult(
                ticker=self.security.ticker,
                df_ts=None,
                weights=self.weights,
                performance_rel=self.performance_rel,
                strategy_name=self.strategy.__class__.__name__,
            )
        return self._result

    def push_result_to_data_store(self, data_store: BaseDataStore):
        data_store.upload_backtest(self.result)

    def plot_performance(self, plot_security: bool = False):
        print("<plot performance>")
        plt.figure(figsize=(15, 6))
        plt.plot(self.performance, label="performance strategy", color="red")
        if plot_security:
            plt.plot(self.initial_investment * self.security.mid / self.security.mid.iloc[0], label="performance security", color="blue")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        plt.ylabel("performance")
        plt.show()
