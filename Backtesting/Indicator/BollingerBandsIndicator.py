from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from . import VolatilityIndicator, Indication, SimpleMovingAverageIndicator
from DataDownload.DataFile import DataFile


class BollingerBandsIndicator(VolatilityIndicator):
    def __init__(self, min_period: relativedelta = relativedelta(), min_period_ticks: int = 0, standard_deviations: float = 2):
        super().__init__()
        self.min_period = min_period
        self.min_period_ticks = min_period_ticks
        self.standard_deviations = standard_deviations
        self.sma = SimpleMovingAverageIndicator(min_period=self.min_period, min_period_ticks=self.min_period_ticks)

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
        self.cache[end_date] = {"lower_band": lower_band, "sma": sma, "upper_band": upper_band}
        if upper_band <= last_price:
            return Indication.SELL
        elif last_price <= lower_band:
            return Indication.BUY
        else:
            return Indication.HOLD

    def ts(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.cache, orient="index")
