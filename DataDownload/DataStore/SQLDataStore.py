import pandas as pd

from . import BaseDataStore


class SQLDataStore(BaseDataStore):
    def ts_file_exists(self) -> bool:
        pass

    def download_ts(self) -> pd.DataFrame:
        pass

    def upload_ts(self, df: pd.DataFrame) -> None:
        pass
