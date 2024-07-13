from datetime import datetime

from Backtesting.DataFile import DataFile
from . import BaseStrategy


class Strategy2(BaseStrategy):
    def __init__(self):
        pass

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        return None
