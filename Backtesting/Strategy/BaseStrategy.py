from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd

from DataDownload.DataFile import DataFile


class BaseStrategy(ABC):
    @abstractmethod
    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        raise NotImplementedError

    @abstractmethod
    def get_weights(self, security: DataFile) -> pd.Series:
        raise NotImplementedError
