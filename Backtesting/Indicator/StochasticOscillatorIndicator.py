from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt
from numba import jit, float64

from . import MomentumIndicator, Indication
from DataDownload.DataFile import DataFile


class StochasticOscillatorIndicator(MomentumIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, lower=20, upper=80, threads: int = 1):
        super().__init__()
        self.min_period = min_period
        self.timedelta_min_period = timedelta(days=min_period.days, hours=min_period.hours, minutes=min_period.minutes, seconds=min_period.seconds, microseconds=min_period.microseconds)
        self.min_period_ticks = min_period_ticks
        if lower < 0 or upper < lower or 100 < upper:
            raise ValueError()
        self.lower = lower
        self.upper = upper
        self.threads = threads

    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        period: pd.Series = security.mid.iloc[start_index:end_index]
        soi = _func(period.to_numpy())
        self.date_cache[end_date] = soi
        self.idx_cache[end_index] = soi

        if self.upper <= soi:
            return Indication.SELL
        elif soi <= self.lower:
            return Indication.BUY
        else:
            return Indication.HOLD

    def series_indication(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate StochasticOscillatorIndicator at <{start_time}>")

        rolling = security.mid.rolling(window=self.timedelta_min_period)
        selector = rolling.count().lt(self.min_period_ticks)
        soi_rolling = rolling.apply(lambda period: _func(period.to_numpy()))
        soi_rolling_periods = security.mid.rolling(window=self.min_period_ticks, min_periods=1).apply(lambda period: _func(period.to_numpy()))
        soi_rolling.loc[selector] = soi_rolling_periods.loc[selector]
        soi_rolling = soi_rolling.apply(lambda x: Indication.SELL if x <= self.lower else Indication.BUY if self.upper <= x else Indication.HOLD)

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date-start_time}")
        return soi_rolling

    def plot_soi(self):
        soi = pd.Series(self.date_cache).sort_index()
        plt.figure(figsize=(15, 6))
        plt.plot(soi, label="soi", color="red")
        plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        plt.ylabel("soi")
        plt.show()


@jit((float64[:],), nopython=True, nogil=True)
def _func(period: np.ndarray) -> float64:
    if len(period) == 0:
        return 50
    last = period[-1]
    low = period.min()
    hig = period.max()
    if period.min() == period.max():
        return 50
    return 100 * (last - low) / (hig - low)
