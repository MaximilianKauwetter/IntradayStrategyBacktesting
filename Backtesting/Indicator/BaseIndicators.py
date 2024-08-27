from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum

import pandas as pd

from DataDownload.DataFile import DataFile


class Indication(Enum):
    SELL = -1
    HOLD = 0
    BUY = 1

    def __radd__(self, other):
        if other == 0:
            return self.value
        else:
            return self.__add__(other)

    def __add__(self, other):
        if isinstance(other, Indication):
            return self.value + other.value
        else:
            return self.value + other

    @staticmethod
    def quota_sell(arr: list["Indication"]) -> float:
        return arr.count(Indication.SELL) / len(arr)

    @staticmethod
    def quota_hold(arr: list["Indication"]) -> float:
        return arr.count(Indication.HOLD) / len(arr)

    @staticmethod
    def quota_buy(arr: list["Indication"]) -> float:
        return arr.count(Indication.BUY) / len(arr)


class BaseIndicator(ABC):
    def __init__(self):
        self.date_cache: dict[datetime, any] = dict()
        self.idx_cache: dict[int, any] = dict()

    @abstractmethod
    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        raise NotImplementedError

    # @abstractmethod
    def series_indication(self, security: DataFile) -> pd.Series:
        raise NotImplementedError


class VolatilityIndicator(BaseIndicator):
    pass


class TrendIndicator(BaseIndicator):
    pass


class MomentumIndicator(BaseIndicator):
    pass


class VolumeIndicator(BaseIndicator):
    pass
