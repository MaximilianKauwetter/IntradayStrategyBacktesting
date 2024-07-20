from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

from .BaseIndicators import TrendIndicator
from DataDownload.DataFile import DataFile


class SimpleMovingAverageIndicator(TrendIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0):
        super().__init__()
        self.min_period = min_period
        self.timedelta_min_period = timedelta(days=min_period.days, hours=min_period.hours, minutes=min_period.minutes, seconds=min_period.seconds, microseconds=min_period.microseconds)
        self.min_period_ticks = min_period_ticks

    def indication(self, security: DataFile, end_date: datetime) -> float:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        period: pd.Series = security.mid.iloc[start_index:end_index]
        avg = period.mean()
        self.date_cache[end_date] = avg
        self.idx_cache[end_index] = avg
        return avg

    def series_indication(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate SimpleMovingAverageIndicator at <{start_time}>")

        rolling = security.mid.rolling(window=self.timedelta_min_period)
        selector = rolling.count().lt(self.min_period_ticks)
        rolling = rolling.mean()
        rolling_periods = security.mid.rolling(window=self.min_period_ticks, min_periods=1).mean()
        rolling.loc[selector] = rolling_periods.loc[selector]
        self.date_cache = rolling.to_dict()
        self.idx_cache = rolling.reset_index().to_dict()

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date-start_time}")
        return rolling
