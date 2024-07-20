from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from numba import jit, float64, int64

from DataDownload.DataFile import DataFile
from . import VolatilityIndicator


class SimpleAverageTrueRangeIndicator(VolatilityIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, n: int = 14, threads: int = 1):
        super().__init__()
        self.min_period = min_period
        self.timedelta_min_period = timedelta(days=min_period.days, hours=min_period.hours, minutes=min_period.minutes, seconds=min_period.seconds, microseconds=min_period.microseconds)
        self.min_period_ticks = min_period_ticks
        self.n = n
        self.threads = threads

    def indication(self, security: DataFile, end_date: datetime) -> None | float:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        if start_index == end_index:
            return None
        period: pd.Series[float] = security.mid.iloc[start_index:end_index]
        split = np.array_split(period, min(end_index - start_index, self.n))
        trs = np.array([max(arr.max(), arr.flat[0]) - min(arr.min(), arr.flat[0]) for arr in split])
        atr = trs.mean()
        self.date_cache[end_date] = atr
        self.idx_cache[end_index] = atr
        return atr

    def series_indication(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate SimpleAverageTrueRangeIndicator at <{start_time}>")

        rolling = security.mid.rolling(window=self.timedelta_min_period)
        selector = rolling.count().lt(self.min_period_ticks)
        rolling_atr_period = security.mid.rolling(window=self.min_period_ticks, min_periods=1).apply(lambda x: _func(x.to_numpy(), self.n))
        rolling_atr = rolling.apply(lambda x: _func(x.to_numpy(), self.n))
        rolling_atr.loc[selector] = rolling_atr_period.loc[selector]

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date-start_time}")
        return rolling_atr


@jit((float64[:], int64), nopython=True, nogil=True)
def _func(period: np.ndarray, n: int64) -> float64:
    n = min(n, period.shape[0])
    x = np.empty(n, dtype=np.float64)
    arr = np.array_split(period, n)
    for i in range(n):
        ar = arr[i]
        x[i] = max(ar.max(), ar.flat[0]) - min(ar.min(), ar.flat[0])
    return x.mean()
