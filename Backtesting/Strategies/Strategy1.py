from datetime import datetime

from Backtesting.DataFile import DataFile
from . import BaseStrategy
from Backtesting.Indicator import StochasticOscillatorIndicator, Indication


class Strategy1(BaseStrategy):
    def __init__(self, invest: float = 1.0, period_tickers=10000):
        self.invest = invest
        self.mom_indicator = StochasticOscillatorIndicator(period_tickers, 20, 80)

    def get_weight(self, security: DataFile, end_date: datetime) -> None | float:
        momentum = self.mom_indicator.indication(security, end_date)
        return {
            Indication.SELL: 0,
            Indication.HOLD: None,
            Indication.BUY: self.invest,
        }[momentum]
