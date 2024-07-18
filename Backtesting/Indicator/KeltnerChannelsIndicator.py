from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from . import VolatilityIndicator, Indication
from DataDownload.DataFile import DataFile


class KeltnerChannelsIndicator(VolatilityIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, n: int = 14, times_art: float = 2):
        super().__init__()
        self.min_period = min_period
        self.min_period_ticks = min_period_ticks
        self.n = n
        self.times_art = times_art

    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        end_index = security.index.get_loc(end_date) + 1
        start_index = security.index.get_indexer(pd.DatetimeIndex([end_date - self.min_period]), method="backfill")[0]
        start_index = max(0, min(start_index, end_index - self.min_period_ticks))
        period: pd.Series = security.mid.iloc[start_index:end_index]
        ema = period.ewm(span=end_index - start_index, adjust=False).mean()[-1]
        split = np.array_split(period, min(end_index - start_index, self.n))
        trs = np.array([max(arr.max(), arr.flat[0]) - min(arr.min(), arr.flat[0]) for arr in split])
        atr = trs.mean()

        lower_band = ema - self.times_art * atr
        upper_band = ema + self.times_art * atr
        if upper_band <= ema:
            return Indication.SELL
        elif ema <= lower_band:
            return Indication.BUY
        else:
            return Indication.HOLD
