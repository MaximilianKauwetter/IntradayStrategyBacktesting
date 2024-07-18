from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from . import BaseIndicators, Indication


class SimpleAverageTrueRange(BaseIndicators):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, n: int = 14):
        super().__init__()
        self.min_period = min_period
        self.min_period_ticks = min_period_ticks
        self.n = n

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
        return atr
