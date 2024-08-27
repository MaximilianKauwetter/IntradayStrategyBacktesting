from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from numba import jit, float64

from . import VolatilityIndicator, Indication
from .SimpleMovingAverageIndicator import SimpleMovingAverageIndicator
from DataDownload.DataFile import DataFile


class BollingerBandsIndicator(VolatilityIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, standard_deviations: float = 2, threads: int = 1):
        super().__init__()
        self.min_period = min_period
        self.timedelta_min_period = timedelta(days=min_period.days, hours=min_period.hours, minutes=min_period.minutes, seconds=min_period.seconds, microseconds=min_period.microseconds)
        self.min_period_ticks = min_period_ticks
        self.sma = SimpleMovingAverageIndicator(min_period=min_period, min_period_ticks=min_period_ticks)
        self.standard_deviations = standard_deviations
        self.threads = threads

    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        period: pd.Series = security.mid.iloc[start_index:end_index]
        last_price = period.iloc[-1]
        sma = period.mean()
        std_dev = period.pct_change().std()
        upper_band = sma + self.standard_deviations * std_dev
        lower_band = sma - self.standard_deviations * std_dev
        self.date_cache[end_date] = {"lower_band": lower_band, "sma": sma, "upper_band": upper_band}
        self.idx_cache[end_index] = {"lower_band": lower_band, "sma": sma, "upper_band": upper_band}
        if upper_band <= last_price:
            return Indication.SELL
        elif last_price <= lower_band:
            return Indication.BUY
        else:
            return Indication.HOLD

    def series_indication(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate BollingerBandsIndicator at <{start_time}>")

        sma: pd.Series = self.sma.series_indication(security)
        pct_change = security.mid.pct_change()
        std_period_rolling = pct_change.rolling(window=self.min_period_ticks, min_periods=1)
        selector = std_period_rolling.count().lt(self.min_period_ticks)
        std_period = std_period_rolling.apply(lambda x: _func(x.to_numpy()))
        std = pct_change.rolling(window=self.timedelta_min_period).apply(lambda x: _func(x.to_numpy()))
        std.loc[selector] = std_period.loc[selector]

        df = security.mid.to_frame("price")
        df["lower_band"] = sma.subtract(std.multiply(self.standard_deviations))
        df["upper_band"] = sma.add(std.multiply(self.standard_deviations))
        bb = df.apply(lambda x: Indication.SELL if x.price <= x.lower_band else Indication.BUY if x.upper_band <= x.price else Indication.HOLD, axis="columns")

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date-start_time}")
        return bb

    def ts(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.date_cache, orient="index")


@jit((float64[:],), nopython=True, nogil=True)
def _func(period: np.ndarray) -> float64:
    return period.std()
