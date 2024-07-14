from datetime import datetime

from . import VolatilityIndicator, Indication
from ..DataFile import DataFile


class KeltnerChannelsIndicator(VolatilityIndicator):
    def __init__(self):
        super().__init__()

    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        pass
