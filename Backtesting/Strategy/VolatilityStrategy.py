from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from . import BaseStrategy
from ..Indicator import SimpleAverageTrueRangeIndicator, BollingerBandsIndicator, KeltnerChannelsIndicator, Indication


class VolatilityStrategy(BaseStrategy):
    def __init__(self, invest: float = 1.0, threads: int = 1):
        self.invest = invest
        min_period = relativedelta(hours=1)
        min_period_ticks = 200

        self.atr_1h = SimpleAverageTrueRangeIndicator(min_period=min_period, min_period_ticks=min_period_ticks, threads=threads)
        self.atr_6h = SimpleAverageTrueRangeIndicator(min_period=6 * min_period, min_period_ticks=6 * min_period_ticks, threads=threads)
        self.bb = BollingerBandsIndicator(min_period=min_period, min_period_ticks=min_period_ticks, threads=threads)
        self.kc = KeltnerChannelsIndicator(min_period=min_period, min_period_ticks=min_period_ticks, threads=threads)

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        atr_1h = self.atr_1h.indication(security=security, end_date=end_date)
        atr_6h = self.atr_1h.indication(security=security, end_date=end_date)
        bb = self.bb.indication(security=security, end_date=end_date)
        kc = self.kc.indication(security=security, end_date=end_date)

        count_sell = [bb, kc].count(Indication.SELL)
        count_buy = [bb, kc].count(Indication.BUY)
        if 2 <= count_buy and 0 == count_sell and atr_6h < atr_1h:
            return self.invest
        elif 0 == count_buy and 2 <= count_sell and atr_6h < atr_1h:
            return 0
        else:
            return None

    def get_weights(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate VolatilityStrategy at <{start_time}>")

        df = self.atr_1h.series_indication(security=security).to_frame("atr_1h")
        df["atr_6h"] = self.atr_1h.series_indication(security=security)
        df["bb"] = self.bb.series_indication(security=security)
        df["kc"] = self.kc.series_indication(security=security, atr=df["atr_1h"])

        vol = df.apply(
            lambda x: self.invest
            if 1 <= [x.bb, x.kc].count(Indication.BUY) and 0 == [x.bb, x.kc].count(Indication.SELL) and x.atr_6h < x.atr_1h
            else 0
            if 0 == [x.bb, x.kc].count(Indication.BUY) and 1 <= [x.bb, x.kc].count(Indication.SELL) and x.atr_6h < x.atr_1h
            else None,
            axis="columns",
        )

        end_date = datetime.now()
        print(f"End calculation at <{end_date}> within {end_date - start_time}")
        return vol
