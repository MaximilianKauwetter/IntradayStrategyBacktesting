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
    RAW_DATA_FOLDER = f"{os.path.dirname(__file__)}/../Data/RawCSVs"
    FILE_FORMAT = "!IIIff"
    DATE_FORMAT = "%Y/%m/%d"
    DATE_FILE_FORMAT = "%Y_%m_%d"
    SEPARATOR: str = ";"

    @staticmethod
    def download_data(ticker: str, start_date: date, end_date: date) -> [pd.DataFrame, pd.DataFrame]:
        raw_dir_path = f"{DataDownloader.RAW_DATA_FOLDER}/{ticker}"
        if not os.path.isdir(raw_dir_path):
            os.makedirs(raw_dir_path)
        all_days = pd.date_range(start_date, end_date, freq="1d", inclusive="both").to_list()
        dfs = []
        print(f"Downloading data for {ticker}")
        number_days = len(all_days)
        for j, day in enumerate(all_days):
            sys.stdout.write("\r")
            print(day, end="")
            day_df_file = f"{DataDownloader.RAW_DATA_FOLDER}/{ticker}/{ticker}_{day.strftime(DataDownloader.DATE_FILE_FORMAT)}.csv"
            if os.path.exists(day_df_file):
                print(" in filesystem", end="")
                day_df = pd.read_csv(filepath_or_buffer=day_df_file, sep=DataDownloader.SEPARATOR, parse_dates=["date"], index_col="date")
            else:
                print(" download", end="")
                day_df = pd.concat([DataDownloader.download_df(ticker, day + relativedelta(hour=i), perc=100 * (24 * j + i) / (24 * number_days)) for i in range(24)], axis="index")
                day_df.to_csv(path_or_buf=day_df_file, index=True, sep=DataDownloader.SEPARATOR)
            dfs.append(day_df)
        print("\nDownload done\nStart Calculations")
        df = pd.concat(dfs, axis="index")
        # calc new columns
        for_ret_calc = df.dropna(how="any", axis="rows").copy()
        for_ret_calc["returns"] = for_ret_calc["bid"].pct_change()
        for_ret_calc["sell_costs"] = -for_ret_calc["spread"] / for_ret_calc["ask"]
        df = df.join(for_ret_calc.loc[:, ["returns", "sell_costs"]], how="left")
        return df

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
