from datetime import datetime

from DataDownload.DataFile import DataFile
from . import BaseStrategy


class VolatilityStrategy(BaseStrategy):
    def __init__(self):
        pass

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        return None
