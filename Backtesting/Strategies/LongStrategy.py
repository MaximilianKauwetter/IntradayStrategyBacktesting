from datetime import datetime

from DataDownload.DataFile import DataFile
from . import BaseStrategy


class LongStrategy(BaseStrategy):
    def __init__(self, invest: float = 1.0):
        self.invest = invest

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        return self.invest
