from abc import ABC, abstractmethod
from datetime import datetime

from Backtesting.DataFile import DataFile


class BaseStrategy(ABC):
    @abstractmethod
    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        raise NotImplementedError
