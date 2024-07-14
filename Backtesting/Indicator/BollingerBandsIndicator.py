from datetime import datetime

from . import VolatilityIndicator, Indication
from ..DataFile import DataFile


class BollingerBandsIndicator(VolatilityIndicator):
    def __init__(self, period_ticks: int, standard_deviations: float = 2):
        super().__init__()
        self.period_ticks = period_ticks
        self.standard_deviations = standard_deviations

    def indication(self, security: DataFile, end_date: datetime) -> Indication:
        pass
