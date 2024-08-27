from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from . import BaseStrategy
from ..Indicator import SimpleMovingAverageIndicator, ExponentialMovingAverageIndicator


class TrendStrategy(BaseStrategy):
    def __init__(self, invest: float = 1.0, threads: int = 1):
        self.invest = invest

        self.sma_1h = SimpleMovingAverageIndicator(min_period_ticks=200, min_period=relativedelta(hours=1))
        self.sma_6h = SimpleMovingAverageIndicator(min_period_ticks=1200, min_period=relativedelta(hours=6))
        self.ema_1h = ExponentialMovingAverageIndicator(min_period_ticks=200, min_period=relativedelta(hours=1), threads=threads)

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        sma_1 = self.sma_1h.indication(security, end_date)
        sma_6 = self.sma_6h.indication(security, end_date)
        ema_1 = self.ema_1h.indication(security, end_date)
        last_price = security.mid.loc[end_date]
        if sma_1 < sma_6 and last_price < ema_1:
            sma = 0
        elif sma_6 < sma_1 and ema_1 < last_price:
            sma = self.invest
        else:
            sma = None

        return sma

    def get_weights(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate TrendStrategy at <{start_time}>")

        df = security.mid.to_frame("price")
        df["sma_1"] = self.sma_1h.series_indication(security)
        df["sma_6"] = self.sma_6h.series_indication(security)
        df["ema_1"] = self.ema_1h.series_indication(security)

        trend_strat = df.apply(
            lambda x: 0 if x.sma_1 < x.sma_6 and x.price < x.ema_1 else self.invest if x.sma_6 < x.sma_1 and x.ema_1 < x.price else None,
            axis="columns",
        )

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date - start_time}")
        return trend_strat
