from datetime import datetime, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

from Backtesting.Indicator import TrendIndicator, Indication
from DataDownload.DataFile import DataFile
from multiprocesspandas import applyparallel


class RelativeStrengthIndexIndicator(TrendIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, lower: float = 30, upper: float = 70, threads: int = 1):
        super().__init__()
        self.min_period = min_period
        self.timedelta_min_period = timedelta(days=min_period.days, hours=min_period.hours, minutes=min_period.minutes, seconds=min_period.seconds, microseconds=min_period.microseconds)
        self.min_period_ticks = min_period_ticks
        self.lower = lower
        self.upper = upper
        self.threads = threads

    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        period: pd.Series = security.mid.iloc[start_index:end_index]
        diff = period.diff()
        avg_gain = diff.clip(lower=0).mean()
        avg_loss = -diff.clip(upper=0).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        self.date_cache[end_date] = rsi
        self.idx_cache[end_index] = rsi
        if rsi <= self.lower:
            return Indication.BUY
        elif self.upper <= rsi:
            return Indication.SELL
        else:
            return Indication.HOLD

    def series_indication(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate RelativeStrengthIndexIndicator at <{start_time}>")

        diff = security.mid.diff()
        gain = diff.clip(lower=0)
        loss = -diff.clip(upper=0)
        avg_loss_rolling = loss.rolling(window=self.timedelta_min_period)
        selector = avg_loss_rolling.count().lt(self.min_period_ticks)
        avg_gain_periods = gain.rolling(window=self.min_period_ticks, min_periods=1).mean()
        avg_loss_periods = loss.rolling(window=self.min_period_ticks, min_periods=1).mean()
        avg_gain: pd.Series = gain.rolling(window=self.timedelta_min_period).mean()
        avg_loss: pd.Series = avg_loss_rolling.mean()
        avg_gain.loc[selector] = avg_gain_periods.loc[selector]
        avg_loss.loc[selector] = avg_loss_periods.loc[selector]
        rs = avg_gain.divide(avg_loss)
        rsi = rs.apply(lambda x: 100 - (100 / (x + 1)))
        self.date_cache = rsi
        rsi = rsi.apply(lambda x: Indication.SELL if x <= self.lower else Indication.BUY if self.upper <= x else Indication.HOLD)

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date-start_time}")
        return rsi
