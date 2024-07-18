from datetime import datetime
from typing import Callable

import pandas as pd
from dateutil.relativedelta import relativedelta

from . import TrendIndicator
from DataDownload.DataFile import DataFile


class SimpleMovingAverageIndicator(TrendIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0):
        super().__init__()
        self.min_period = min_period
        self.min_period_ticks = min_period_ticks

    def indication(self, security: DataFile, end_date: datetime) -> float:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        period: pd.Series = security.mid.iloc[start_index:end_index]
        avg = period.mean()
        self.cache[end_date] = avg
        return avg
