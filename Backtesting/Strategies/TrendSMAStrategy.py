from datetime import datetime

from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from . import BaseStrategy
from Backtesting.Indicator import SimpleMovingAverageIndicator


class TrendSMAStrategy(BaseStrategy):
    def __init__(self, invest: float = 1.0):
        self.invest = invest
        self.sma_1h = SimpleMovingAverageIndicator(min_period_ticks=200, min_period=relativedelta(hours=1))
        self.sma_6h = SimpleMovingAverageIndicator(min_period_ticks=1200, min_period=relativedelta(hours=6))

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        sma_1 = self.sma_1h.indication(security, end_date)
        sma_6 = self.sma_6h.indication(security, end_date)
        if sma_1 < sma_6:
            return 0
        elif sma_6 < sma_1:
            return self.invest
        else:
            return None
