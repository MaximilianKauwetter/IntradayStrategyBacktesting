from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from .BaseIndicators import TrendIndicator
from numba import jit, float64, int64


class ExponentialMovingAverageIndicator(TrendIndicator):
    def __init__(
        self,
        min_period: relativedelta = relativedelta(),
        min_period_ticks: int = 0,
        threads: int = 1,
    ):
        super().__init__()
        self.min_period = min_period
        self.timedelta_min_period = timedelta(days=min_period.days, hours=min_period.hours, minutes=min_period.minutes, seconds=min_period.seconds, microseconds=min_period.microseconds)
        self.min_period_ticks = min_period_ticks
        self.threads = threads

    def indication(self, security: DataFile, end_date: datetime) -> float:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        period: pd.Series = security.mid.iloc[start_index:end_index]
        ema = period.ewm(span=end_index - start_index, adjust=False).mean().iloc[-1]
        self.date_cache[end_date] = ema
        self.idx_cache[end_index] = ema
        return ema

    def series_indication(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate ExponentialMovingAverageIndicator at <{start_time}>")

        rolling = security.mid.rolling(window=self.timedelta_min_period)
        selector = rolling.count().lt(self.min_period_ticks)
        ema: pd.Series = rolling.apply(lambda x: _ewma(x.to_numpy(), x.count()))
        ema_period = security.mid.ewm(span=self.min_period_ticks, adjust=False).mean()
        ema.loc[selector] = ema_period.loc[selector]

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date-start_time}")
        self.date_cache = ema.to_dict()
        return ema


@jit((float64[:], int64), nopython=True, nogil=True)
def _ewma(arr_in, window) -> float64:
    n = arr_in.shape[0]
    ewma = np.empty(n, dtype=float64)
    alpha = 2 / float(window + 1)
    w = 1
    ewma_old = arr_in[0]
    ewma[0] = ewma_old
    for i in range(1, n):
        w += (1 - alpha) ** i
        ewma_old = ewma_old * (1 - alpha) + arr_in[i]
        ewma[i] = ewma_old / w
    return ewma.mean()
