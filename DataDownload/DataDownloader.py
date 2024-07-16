import lzma
import os
import struct
import sys
from datetime import datetime, date

import numpy as np
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta


class DataDownloader:
    FILE_FORMAT = "!IIIff"
    DATE_FORMAT = "%Y/%m/%d"
    DATE_FILE_FORMAT = "%Y_%m_%d"
    SEPARATOR: str = ";"

    @staticmethod
    def calc_columns(df: pd.DataFrame) -> pd.DataFrame:
        start_time = datetime.now()
        print(f"Start calculating columns at <{start_time}>")
        for_ret_calc = df.dropna(how="any", axis="rows").copy()
        for_ret_calc["returns"] = for_ret_calc["bid"].pct_change()
        for_ret_calc["sell_costs"] = -for_ret_calc["spread"] / for_ret_calc["ask"]
        df = df.join(for_ret_calc.loc[:, ["returns", "sell_costs"]], how="left")
        end_time = datetime.now()
        print(f"End calculating columns at <{end_time}> within <{end_time - start_time}>")
        return df

    @staticmethod
    def download_data(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        all_days = pd.date_range(start_date, end_date, freq="1d", inclusive="both").to_list()
        dfs = []
        number_days = len(all_days)
        print(f"Downloading data for {ticker}")
        for day_nr, day in enumerate(all_days):
            sys.stdout.write("\r")
            print(day, end="")
            day_df = DataDownloader.download_day(ticker=ticker, day=day, day_nr=day_nr, total_days=number_days)
            dfs.append(day_df)
        print("\nDownload done\nStart Calculations")
        df = pd.concat(dfs, axis="index")
        return DataDownloader.calc_columns(df)

    @staticmethod
    def update_data(df: pd.DataFrame, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        df = df.loc[:, ["ask", "bid", "ask_vol", "bid_vol", "mid", "spread"]]
        df_first_day = df.index[0].date()
        df_last_day = df.index[-1].date()
        days_before = pd.date_range(start_date, df_first_day, freq="1d", inclusive="left").to_list() if start_date != df_first_day else []
        days_after = pd.date_range(df_last_day, end_date, freq="1d", inclusive="right").to_list() if end_date != df_last_day else []
        number_days = len(days_before) + len(days_after)

        start_time = datetime.now()
        dfs = []
        print(f"Downloading additional data for {ticker} at <{start_time}>")
        for day_nr, day in enumerate(days_before):
            sys.stdout.write("\r")
            print(day, end="")
            dfs.append(DataDownloader.download_day(ticker=ticker, day=day, day_nr=day_nr, total_days=number_days))
        dfs.append(df)
        for day_nr, day in enumerate(days_after):
            sys.stdout.write("\r")
            print(day, end="")
            dfs.append(DataDownloader.download_day(ticker=ticker, day=day, day_nr=len(days_before) + day_nr, total_days=number_days))
        end_time = datetime.now()
        print(f"\nDownload done at <{end_time}> Time needed <{end_time-start_time}>")
        df = pd.concat(dfs, axis="index")
        return DataDownloader.calc_columns(df)

    @staticmethod
    def download_day(ticker: str, day: datetime, day_nr: int, total_days: int) -> pd.DataFrame:
        return pd.concat([DataDownloader.download_df(ticker, day + relativedelta(hour=i), perc=100 * (24 * day_nr + i) / (24 * total_days)) for i in range(24)], axis="index")

    @staticmethod
    def download_df(ticker: str, timestamp: datetime, perc: None | float = None) -> pd.DataFrame:
        sys.stdout.write("\r")
        print(f"Download: {ticker}/{timestamp}", end="")
        if perc is not None:
            print(f" [{perc:.2f}%]", end="")
        url = f"https://datafeed.dukascopy.com/datafeed/{ticker}/{timestamp.year}/{timestamp.month - 1:02d}/{timestamp.day:02d}/{timestamp.hour:02d}h_ticks.bi5"
        df = DataDownloader.download_bi5_to_df(url)
        df["date"] = df["date"].apply(lambda milsec: timestamp + relativedelta(microseconds=milsec * 1000))
        df.set_index("date", inplace=True)
        df["ask"] = df["ask"].apply(lambda quote: quote / 1000)
        df["bid"] = df["bid"].apply(lambda quote: quote / 1000)
        df["mid"] = (df["ask"] + df["bid"]) / 2
        df["spread"] = df["ask"] - df["bid"]
        if df.empty:
            df.loc[timestamp] = np.nan
        return df

    @staticmethod
    def download_bi5_to_df(url):
        data = []
        with requests.get(url, stream=True) as res:
            rawdata = res.content
            decomp = lzma.LZMADecompressor(lzma.FORMAT_AUTO, None, None)
            decompresseddata = decomp.decompress(rawdata)
            for i in range(0, int(len(decompresseddata) / 20)):
                data.append(struct.unpack(DataDownloader.FILE_FORMAT, decompresseddata[i * 20 : (i + 1) * 20]))
        return pd.DataFrame(data=data, columns=["date", "ask", "bid", "ask_vol", "bid_vol"])
