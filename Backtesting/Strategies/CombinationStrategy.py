from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from . import BaseStrategy
from ..Indicator import BollingerBandsIndicator, RelativeStrengthIndexIndicator, StochasticOscillatorIndicator, Indication


class CombinationStrategy(BaseStrategy):
    def __init__(self, invest: float = 1.0, threads: int = 1):
        self.invest = invest
        min_period = relativedelta(hours=2)
        min_period_ticks = 400
        self.bb = BollingerBandsIndicator(min_period=min_period, min_period_ticks=min_period_ticks, threads=threads)
        self.rsi = RelativeStrengthIndexIndicator(min_period=min_period, min_period_ticks=min_period_ticks, lower=30, upper=70, threads=threads)
        self.so = StochasticOscillatorIndicator(min_period=min_period, min_period_ticks=min_period_ticks, lower=20, upper=80, threads=threads)

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        bb = self.bb.indication(security=security, end_date=end_date)
        rsi = self.rsi.indication(security=security, end_date=end_date)
        so = self.so.indication(security=security, end_date=end_date)
        count_sell = [bb, rsi, so].count(Indication.SELL)
        count_buy = [bb, rsi, so].count(Indication.BUY)
        if 3 <= count_buy and count_sell == 0:
            return self.invest
        elif 2 <= count_sell and count_buy == 0:
            return 0
        else:
            return None

    def get_weights(self, security: DataFile) -> pd.Series:
        start_time = datetime.now()
        print(f"Calculate CombinationStrategy at <{start_time}>")

        df = self.bb.series_indication(security=security).to_frame("bb")
        df["rsi"] = self.rsi.series_indication(security=security)
        df["so"] = self.so.series_indication(security=security)
        combine_strat = df.apply(
            lambda x: self.invest
            if 2 <= x.to_list().count(Indication.BUY) and x.to_list().count(Indication.SELL) == 0
            else 0
            if 3 <= x.to_list().count(Indication.SELL) and x.to_list().count(Indication.BUY) == 0
            else None,
            axis="columns",
        )

        end_date = datetime.now()
        print(f"End CombinationStrategy calculation at <{end_date}> within {end_date - start_time}")
        return combine_strat
