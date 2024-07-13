from datetime import datetime

from Backtesting.DataFile import DataFile
from . import BaseStrategy


class UpperLowerStrategy(BaseStrategy):
    def __init__(self, low: float, high: float, invest: float = 1.0):
        self.low = low
        self.high = high
        self.invest = invest

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        if security.ask.loc[end_date] < self.low:
            return self.invest
        elif self.high < security.bid.loc[end_date]:
            return 0
        else:
            return None
