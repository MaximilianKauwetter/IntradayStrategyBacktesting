import io
import os
from datetime import date, datetime

import matplotlib.pyplot as plt
import pandas as pd
from dateutil.relativedelta import relativedelta

from DataDownload.DataDownloader import DataDownloader
from google.cloud import storage


class DataFile:
    EARLIEST_DATE: date = date(year=2015, month=3, day=24)
    AGG_DATA_FOLDER: str = f"{os.path.dirname(__file__)}/../Data/AggregatedCSVs"

    def __init__(self, df_ts: pd.DataFrame):
        self._df: pd.DataFrame = df_ts
        self.index: pd.Index = df_ts.index
        self.ask: pd.Series = df_ts.loc[:, "ask"]
        self.mid: pd.Series = df_ts.loc[:, "mid"]
        self.bid: pd.Series = df_ts.loc[:, "bid"]
        self.spread: pd.Series = df_ts.loc[:, "spread"]
        self.pct_returns: pd.Series = df_ts.loc[:, "returns"]
        self.pct_sell_costs: pd.Series = df_ts.loc[:, "sell_costs"]
        self.start_date: datetime = df_ts.index[0]
        self.end_date: datetime = df_ts.index[-1]

    def strip(self, end_date: datetime) -> "DataFile":
        return DataFile(self._df.loc[self._df.index <= end_date])

    def plot_ts(self):
        print("<plot ts>")
        plt.figure(figsize=(15, 6))
        plt.plot(self.ask, label="ask", color="red")
        plt.plot(self.mid, label="mid", color="green")
        plt.plot(self.bid, label="bid", color="blue")
        plt.legend(loc="upper right", fontsize=12)
        plt.ylabel("Price")
        plt.show()

    def plot_normalized_ts(self):
        print("<plot ts>")
        plt.figure(figsize=(15, 6))
        first_mid = self.mid.iloc[0] / 100
        plt.plot(self.ask / first_mid, label="ask", color="red")
        plt.plot(self.mid / first_mid, label="mid", color="green")
        plt.plot(self.bid / first_mid, label="bid", color="blue")
        plt.legend(loc="upper right", fontsize=12)
        plt.ylabel("Price")
        plt.show()

    @staticmethod
    def upload_to_cloud(blob: storage.Blob, df: pd.DataFrame) -> None:
        start_time = datetime.now()
        print(f"Start upload at <{start_time}>")
        blob.upload_from_string(df.to_csv(index=True, sep=DataDownloader.SEPARATOR), "text/csv")
        end_time = datetime.now()
        print(f"End upload at <{end_time}> within <{end_time-start_time}>")

    @staticmethod
    def from_cloud(bucket: storage.Bucket, ticker: str, start_date: date = None, end_date: date = None) -> "DataFile":
        if start_date is None:
            start_date = DataFile.EARLIEST_DATE
        latest_full_date = date.today() - relativedelta(days=1)
        if end_date is None or latest_full_date < end_date:
            end_date = latest_full_date

        blob = bucket.blob(f"Data/{ticker}.csv")
        if blob.exists():
            df_ts = pd.read_csv(
                io.BytesIO(blob.download_as_bytes()),
                sep=DataDownloader.SEPARATOR,
                parse_dates=["date"],
                index_col="date",
            )
            dates = df_ts.index
            first_date, last_date = dates[0].date(), dates[-1].date()
            if start_date < first_date or last_date < end_date:
                # build new bigger AggregateDF and store
                print("Update AGGREGATE")
                first_date = min(first_date, start_date)
                last_date = max(last_date, end_date)
                df_ts = DataDownloader.update_data(df=df_ts, ticker=ticker, start_date=first_date, end_date=last_date)
                DataFile.upload_to_cloud(blob=blob, df=df_ts)
            start_date, end_date = pd.to_datetime((start_date, end_date + relativedelta(days=1, microseconds=-1)))
            df_ts = df_ts.loc[(start_date <= df_ts.index) & (df_ts.index <= end_date)]
        else:
            df_ts = DataDownloader.download_data(ticker=ticker, start_date=start_date, end_date=end_date)
            DataFile.upload_to_cloud(blob=blob, df=df_ts)
        df_ts.dropna(how="any", axis="rows", inplace=True)
        return DataFile(df_ts)

    @staticmethod
    def from_csv(ticker: str, start_date: date = None, end_date: date = None) -> "DataFile":
        if start_date is None:
            start_date = DataFile.EARLIEST_DATE
        latest_full_date = date.today() - relativedelta(days=1)
        if end_date is None or latest_full_date < end_date:
            end_date = latest_full_date

        file_path_ts = f"{DataFile.AGG_DATA_FOLDER}/{ticker}_ts.csv"
        if os.path.exists(file_path_ts):
            df_ts = pd.read_csv(
                filepath_or_buffer=file_path_ts,
                sep=DataDownloader.SEPARATOR,
                parse_dates=["date"],
                index_col="date",
            )
            dates = df_ts.index
            first_date, last_date = dates[0].date(), dates[-1].date()
            if start_date < first_date or last_date < end_date:
                # build new bigger AggregateDF and store
                print("Update AGGREGATE")
                first_date = min(first_date, start_date)
                last_date = max(last_date, end_date)
                df_ts = DataDownloader.update_data(df_ts, ticker=ticker, start_date=first_date, end_date=last_date)
                df_ts.to_csv(path_or_buf=file_path_ts, index=True, sep=DataDownloader.SEPARATOR)
            start_date, end_date = pd.to_datetime((start_date, end_date + relativedelta(days=1, microseconds=-1)))
            df_ts = df_ts.loc[(start_date <= df_ts.index) & (df_ts.index <= end_date)]
        else:
            print("write to AGGREGATE")
            df_ts = DataDownloader.download_data(ticker=ticker, start_date=start_date, end_date=end_date)
            df_ts.to_csv(path_or_buf=file_path_ts, index=True, sep=DataDownloader.SEPARATOR)
        df_ts.dropna(how="any", axis="rows", inplace=True)
        return DataFile(df_ts)
