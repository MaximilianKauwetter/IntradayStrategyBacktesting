import os
from datetime import datetime

from Backtesting.Backtesting import Backtesting
from Backtesting.Strategies import *
from DataDownload.DataStore import LocalDataStore, BucketDataStore, SplitBucketDataStore

if __name__ == "__main__":
    print("start")
    ticker = os.getenv("TICKER", "No ticker specified")
    strategy = os.getenv("STRATEGY", "")
    start_date = os.getenv("START_DATE", "No start_date specified")
    start_at = os.getenv("START_AT", "0")
    end_date = os.getenv("END_DATE", "No end_date specified")
    num_threads = os.getenv("NUM_THREADS", "1")

    # pd.set_option("display.expand_frame_repr", False)
    # ticker = "USA500IDXUSD"
    # ticker = "EUSIDXEUR"
    # ticker = "BTCEUR"
    # ticker = "ETHEUR"
    # ticker = "NLDIDXEUR"

    # strategy = ""
    # start_date = "2023/12/25"
    # start_at = "2023/12/30"
    # end_date = "2024/1/10"
    # num_threads = "3"

    try:
        start_date = datetime.strptime(start_date, "%Y/%m/%d").date()
        end_date = datetime.strptime(end_date, "%Y/%m/%d").date()
    except ValueError:
        print("Invalid date format, format has to be YYYY/MM/DD but was:")
        print(f"start_date: {start_date}")
        print(f"end_date: {end_date}")
        exit()
    if str.isdigit(start_at):
        start_at = int(start_at)
    else:
        try:
            start_at = datetime.strptime(start_at, "%Y/%m/%d").date()
        except ValueError:
            print(f"Invalid format, start_at needs to be int or date with format YYYY/MM/DD but was: {start_at}")

    if str.isnumeric(num_threads):
        num_threads = int(num_threads)
    else:
        print(f"num_threads must be numeric but is: {num_threads}")
        exit()

    strategies = []
    for strat_str in strategy.split(";"):
        if strat_str == "":
            continue
        try:
            strat: BaseStrategy = eval(strat_str)
            strategies.append(strat)
        except Exception as e:
            print(f'"{strat_str}" is not a valid strategy')

    print(f"Ticker: {ticker}")
    print(f"Strategy: {[strategy.__name__ for strategy in strategies]}")
    print(f"Start Date: {start_date}")
    print(f"Start At: {start_at}")
    print(f"End Date: {end_date}")
    print(f"Num Threads: {num_threads}")

    # data_store = BucketDataStore(ticker, num_threads=num_threads)
    # data_store = BucketDataStore(ticker)
    data_store = SplitBucketDataStore(ticker, num_threads=num_threads)

    data = data_store.create_datafile(start_date=start_date, end_date=end_date)

    print("Data downloaded")
    print("Length", data._df.__len__())
    print("start datetime", data._df.index[0])
    print("end datetime", data._df.index[-1])

    for strat in strategies:
        backtesting = Backtesting(security=data, strategy=strat, start_at=start_at, threads=num_threads)
        data_store.upload_backtest(backtesting.result)

    # strat.mom_indicator.plot_soi()

    # sma_10 = pd.Series(strat.sma_1h.cache).sort_index()
    # sma_50 = pd.Series(strat.sma_6h.cache).sort_index()
    # plt.figure(figsize=(15, 6))
    # plt.plot(data.mid, label="ts", color="red")
    # plt.plot(sma_10, label=f"sma 10,000", color="blue")
    # plt.plot(sma_50, label=f"sma 50,000", color="green")
    # plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    # plt.ylabel(f"price")
    # plt.show()
