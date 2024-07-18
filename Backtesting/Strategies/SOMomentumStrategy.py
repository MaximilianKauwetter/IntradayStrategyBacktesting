from datetime import datetime

from dateutil.relativedelta import relativedelta

from DataDownload.DataFile import DataFile
from . import BaseStrategy
from Backtesting.Indicator import StochasticOscillatorIndicator, Indication


class SOMomentumStrategy(BaseStrategy):
    def __init__(self, invest: float = 1.0):
        self.invest = invest
        self.mom_indicator = StochasticOscillatorIndicator(min_period=relativedelta(hours=1), min_period_ticks=200, lower_threshold=20, upper_threshold=80)

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        momentum = self.mom_indicator.indication(security, end_date)
        return {
            Indication.SELL: 0,
            Indication.HOLD: None,
            Indication.BUY: self.invest,
        }[momentum]
