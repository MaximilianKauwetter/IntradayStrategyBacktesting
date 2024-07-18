from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt

from . import MomentumIndicator, Indication
from DataDownload.DataFile import DataFile


class StochasticOscillatorIndicator(MomentumIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, lower_threshold=20, upper_threshold=80):
        super().__init__()
        self.min_period = min_period
        self.min_period_ticks = min_period_ticks
        if lower_threshold < 0 or upper_threshold < lower_threshold or 100 < upper_threshold:
            raise ValueError()
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold

    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        last_closing_price = security.mid.loc[end_date]
        period: pd.Series = security.mid.iloc[start_index:end_index]
        lowest_price = period.min()
        highest_price = period.max()
        if highest_price == lowest_price:
            return Indication.HOLD
        soi = 100 * (last_closing_price - lowest_price) / (highest_price - lowest_price)
        self.cache[end_date] = soi
        if self.upper_threshold <= soi:
            return Indication.SELL
        elif soi <= self.lower_threshold:
            return Indication.BUY
        else:
            return Indication.HOLD

    def plot_soi(self):
        soi = pd.Series(self.cache).sort_index()
        plt.figure(figsize=(15, 6))
        plt.plot(soi, label="soi", color="red")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        plt.ylabel("soi")
        plt.show()
