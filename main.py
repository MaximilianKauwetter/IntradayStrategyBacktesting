import sys
from datetime import date, datetime
import time

import numpy as np
import pandas as pd

from Backtesting.DataFile import DataFile
from Backtesting.Backtesting import Backtesting
from Backtesting.Strategies import UpperLowerStrategy, Strategy1


if __name__ == "__main__":
    pd.set_option("display.expand_frame_repr", False)
    start_date = date(year=2024, month=1, day=10)
    end_date = date(year=2024, month=1, day=10)
    ticker = "BTCEUR"
    data = DataFile.from_csv(ticker=ticker, start_date=start_date, end_date=end_date)
    strat = Strategy1(period_tickers=50000)
    backtesting = Backtesting(security=data, strategy=strat, start_at=10000)
    backtesting.plot_weights()
    backtesting.plot_performance(plot_security=True)
    strat.mom_indicator.plot_soi()
