import sys
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd

from Backtesting.DataFile import DataFile
from Backtesting.Strategies import BaseStrategy


class Temp:
    def __init__(self):
        self.count = 0

    def inc(self):
        self.count += 1


class Backtesting:
    def __init__(self, security: DataFile, strategy: BaseStrategy, initial_investment: float = 100, start_at: datetime | int = 0, threads: int = 1):
        self.security: DataFile = security
        self.strategy: BaseStrategy = strategy
        self.initial_investment: float = initial_investment
        if isinstance(start_at, int):
            start_at = self.security.index[start_at]
        if isinstance(start_at, datetime):
            self.start_at: datetime = start_at
        else:
            raise ValueError("start_at must be an integer or datetime object")
        self.threads = threads
        self._weights = None
        self._performance_rel = None
        self._performance = None

    @property
    def weights(self) -> pd.Series:
        if self._weights is None:
            start_time = datetime.now()
            print(f"Start Backtesting at <{start_time}>")
            total_num = len(self.security.index)
            print(f"Evaluate {total_num:,} ticks")
            weights: [datetime, None | float] = {}
            print(f"Start Calculation at <{datetime.now()}>")
            for i, dt in enumerate(self.security.index.to_list()):
                weights[dt] = self.strategy.get_weight(self.security, dt)
                if i % 100 == 0:
                    sys.stdout.write("\r")
                    print(f"Calculating [{100 * (i / total_num):.2f}%][{datetime.now() - start_time}])", end="")
            print(f"Finished calculation in {datetime.now() - start_time}")
            self._weights = pd.Series(data=weights).sort_index()
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
            weighted_return = self.security.pct_returns * self.weights.shift(1, fill_value=0)
            perf_raw = weighted_return.add(1).cumprod()
            new_invest = self.weights.diff().clip(lower=0)
            new_invest.iloc[0] = self.weights.iloc[0]
            sell_factor = (new_invest * -self.security.spread / self.security.ask).add(1).cumprod()
            self._performance_rel = perf_raw * sell_factor
        return self._performance_rel

    @property
    def performance(self):
        if self._performance is None:
            self._performance = self.initial_investment * self.performance_rel
        return self._performance

    def plot_performance(self, plot_security: bool = False):
        print("<plot performance>")
        plt.figure(figsize=(15, 6))
        plt.plot(self.performance, label="performance strategy", color="red")
        if plot_security:
            plt.plot(self.initial_investment * self.security.mid / self.security.mid.iloc[0], label="performance security", color="blue")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        plt.ylabel("performance")
        plt.show()

    def to_csv(self):
        pass
