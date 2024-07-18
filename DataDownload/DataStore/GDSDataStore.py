import pandas as pd

from Backtesting.BacktestResult import BacktestResult
from . import BaseDataStore


class GDSDataStore(BaseDataStore):
    def ts_file_exists(self) -> bool:
        pass

    def download_ts(self) -> pd.DataFrame:
        pass

    def upload_ts(self, df: pd.DataFrame) -> None:
        pass

    def download_backtest(self, start_date, end_date, strategy_name) -> pd.DataFrame:
        pass

    def upload_backtest(self, backtest_result: BacktestResult) -> None:
        pass
