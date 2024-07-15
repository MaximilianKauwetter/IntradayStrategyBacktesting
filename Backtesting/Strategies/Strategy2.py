from datetime import datetime

from Backtesting.DataFile import DataFile
from . import BaseStrategy
from Backtesting.Indicator import SimpleMovingAverageIndicator


class Strategy2(BaseStrategy):
    def __init__(self, invest: float = 1.0):
        self.invest = invest
        self.sma_10000 = SimpleMovingAverageIndicator(10000)
        self.sma_50000 = SimpleMovingAverageIndicator(50000)

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        sma_10 = self.sma_10000.indication(security, end_date)
        sma_50 = self.sma_50000.indication(security, end_date)
        if sma_10 < sma_50:
            return 0
        elif sma_50 < sma_10:
            return self.invest
        else:
            return None
