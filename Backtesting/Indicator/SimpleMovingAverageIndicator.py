from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt

from . import TrendIndicator, Indication
from ..DataFile import DataFile


class SimpleMovingAverageIndicator(TrendIndicator):
    def __init__(self, period_ticks: int):
        super().__init__()
        self.period_ticks = period_ticks

    def indication(self, security: DataFile, end_date: datetime) -> float:
        end_index = security.index.get_loc(end_date) + 1
        start_index = max(0, end_index - self.period_ticks)
        period: pd.Series = security.mid.iloc[start_index:end_index]
        avg = period.mean()
        self.cache[end_date] = avg
        return avg
