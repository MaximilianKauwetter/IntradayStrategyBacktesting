import gc
import lzma
import struct
import threading
import time
from datetime import datetime, date

import numpy as np
import pandas as pd
import psutil
import requests
from dateutil.relativedelta import relativedelta


class DataDownloader:
    FILE_FORMAT = "!IIIff"
    DATE_FORMAT = "%Y/%m/%d"
    DATE_FILE_FORMAT = "%Y_%m_%d"
    SEPARATOR: str = ";"
    EARLIEST_DATE: date = date(year=2015, month=3, day=24)

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
    def download_data(ticker: str, start_date: date, end_date: date, df: None | pd.DataFrame = None, num_threads: int = 1, log_delta: float = 5) -> pd.DataFrame:
        print(f"Init downloading data from <{ticker}>")
        start_date = max(start_date, DataDownloader.EARLIEST_DATE)
        end_date = min(end_date, date.today() - relativedelta(days=1))

        if df is None or df.empty:
            days = pd.date_range(start_date, end_date, freq="1d", inclusive="both")
        else:
            df = df.loc[:, ["ask", "bid", "ask_vol", "bid_vol", "mid", "spread"]]
            df_first_day = df.index[0].date()
            df_last_day = df.index[-1].date()
            start_date = min(start_date, df_first_day)
            end_date = max(end_date, df_last_day)
            days = pd.date_range(start_date, end_date, freq="1d", inclusive="both")
            done_days = pd.date_range(df_first_day, df_last_day, freq="1d", inclusive="both")
            days = pd.to_datetime(np.setdiff1d(days, done_days))

        to_download = np.array([[day + relativedelta(hour=i) for i in range(24)] for day in days]).flatten()
        final_days: dict = dict.fromkeys(to_download, None)
        number_days = len(to_download)
        divided_dts = np.array_split(to_download, num_threads)

        start_time = datetime.now()
        print(f"Downloading data for {ticker} with {num_threads} threads at <{start_time}>")
        threads = []
        for dts in divided_dts:
            thread = threading.Thread(target=DataDownloader.download_dts, args=(ticker, dts, final_days))
            thread.start()
            threads.append(thread)
        while True:
            if all([not thread.is_alive() for thread in threads]):
                break
            empty_days = sum([day is None for day in final_days.values()])
            filled_days = number_days - empty_days
            perc = filled_days / number_days * 100
            print(f"\rDownloaded {filled_days}/{number_days} [{perc:.2f}%]", end="")
            time.sleep(log_delta)
        end_time = datetime.now()
        print(f"\nDownload done at <{end_time}> Time needed <{end_time-start_time}>")

        print(f"Memory usage before concat: {psutil.Process().memory_info().rss/1000000000:,.2f} GB")
        dfs = list(final_days.values()) if df is None or df.empty else [*final_days.values(), df]
        df = pd.concat(dfs, axis="index")
        print(f"Memory usage after concat: {psutil.Process().memory_info().rss/1000000000:,.2f} GB")
        del final_days
        del dfs
        gc.collect()
        print(f"Memory usage after dfs garbage collection: {psutil.Process().memory_info().rss/1000000000:,.2f} GB")
        df.sort_index(inplace=True)
        return DataDownloader.calc_columns(df)

    @staticmethod
    def download_dts(ticker: str, dts: list[datetime], final: dict[datetime, pd.DataFrame]) -> None:
        for dt in dts:
            final[dt] = DataDownloader.download_df(ticker=ticker, timestamp=dt)

    @staticmethod
    def download_df(ticker: str, timestamp: datetime) -> pd.DataFrame:
        # Developed by Maximilian Kauwetter
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
        for i in range(10):
            try:
                with requests.get(url, stream=True) as res:
                    rawdata = res.content
                    decomp = lzma.LZMADecompressor(lzma.FORMAT_AUTO, None, None)
                    decompresseddata = decomp.decompress(rawdata)
                    for i in range(0, int(len(decompresseddata) / 20)):
                        data.append(struct.unpack(DataDownloader.FILE_FORMAT, decompresseddata[i * 20 : (i + 1) * 20]))
                break
            except:
                pass
        else:
            print(f"Could not download data: {url}")
        return pd.DataFrame(data=data, columns=["date", "ask", "bid", "ask_vol", "bid_vol"])
