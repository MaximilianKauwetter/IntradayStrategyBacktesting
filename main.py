import io
import os
import sys
from datetime import date, datetime
import time

import numpy as np
import pandas as pd

from Backtesting.DataFile import DataFile
from google.cloud import storage

if __name__ == "__main__":
    print("start")
    print(f"Ticker: {os.getenv('TICKER','No ticker specified')}")
    print(f"Strategy: {os.getenv('STRATEGY','No Strategy specified')}")
    if os.path.exists("google_cloud_authentication.json"):
        client = storage.Client.from_service_account_json("google_cloud_authentication.json")
    else:
        try:
            client = storage.Client()
        except Exception as _:
            print("No google_cloud_authentication.json could be found and automatic authentication is also not possible")
            exit()
    bucket = client.get_bucket("mkauwetter-datascience-bucket")
    print("Bloobs:", bucket.list_blobs())
    exit()

    pd.set_option("display.expand_frame_repr", False)
    start_date = date(year=2024, month=2, day=1)
    end_date = date(year=2024, month=2, day=29)
    ticker = "BTCEUR"
    data = DataFile.from_cloud(bucket=bucket, ticker=ticker, start_date=start_date, end_date=end_date)

    print(data._df)

    exit()

    strat = Strategy2()
    backtesting = Backtesting(security=data, strategy=strat, start_at=10000, threads=64)
    backtesting.performance
    backtesting.plot_weights()
    backtesting.plot_performance(plot_security=True)

    # strat.mom_indicator.plot_soi()

    sma_10 = pd.Series(strat.sma_10000.cache).sort_index()
    sma_50 = pd.Series(strat.sma_50000.cache).sort_index()
    plt.figure(figsize=(15, 6))
    plt.plot(data.mid, label="ts", color="red")
    plt.plot(sma_10, label=f"sma 10,000", color="blue")
    plt.plot(sma_50, label=f"sma 50,000", color="green")
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    plt.ylabel(f"price")
    plt.show()
