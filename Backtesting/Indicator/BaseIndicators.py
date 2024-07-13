from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum

from Backtesting.DataFile import DataFile


class Indication(Enum):
    SELL = -1
    HOLD = 0
    BUY = 1


class BaseIndicator(ABC):
    @abstractmethod
    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        raise NotImplementedError


class VolatilityIndicator(BaseIndicator):
    pass


class TrendIndicator(BaseIndicator):
    pass


class MomentumIndicator(BaseIndicator):
    pass


class VolumeIndicator(BaseIndicator):
    pass
