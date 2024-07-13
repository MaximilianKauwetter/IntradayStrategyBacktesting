from datetime import datetime

from Backtesting.DataFile import DataFile
from . import BaseStrategy


class MomentumStrategy(BaseStrategy):
    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        return None
