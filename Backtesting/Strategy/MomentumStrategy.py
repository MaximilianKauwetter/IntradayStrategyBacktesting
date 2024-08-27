from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from . import BaseStrategy
from ..Indicator import StochasticOscillatorIndicator, Indication


class MomentumStrategy(BaseStrategy):
    def __init__(self, invest: float = 1.0, threads: int = 1):
        self.invest = invest
        min_period = relativedelta(hours=1)
        min_period_ticks = 200

        self.so_indicator = StochasticOscillatorIndicator(min_period=min_period, min_period_ticks=min_period_ticks, lower=20, upper=80, threads=threads)

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        soi = self.so_indicator.indication(security, end_date)

        if soi == Indication.BUY:
            return self.invest
        elif soi == Indication.SELL:
            return 0
        else:
            return None

    def get_weights(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate MomentumStrategy at <{start_time}>")

        soi = self.so_indicator.series_indication(security)
        soi = soi.apply(lambda x: self.invest if x == Indication.BUY else 0 if x == Indication.SELL else None)

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date - start_time}")
        return soi
