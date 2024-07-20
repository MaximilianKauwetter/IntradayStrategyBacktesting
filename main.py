import os
from datetime import datetime, date

from Backtesting.AggBacktestResult import AggBacktestResult
from Backtesting.Backtesting import Backtesting
from Backtesting.Strategies import *
from DataDownload.DataStore import SplitBucketDataStore, LocalDataStore

if __name__ == "__main__":
    print("start")
    ticker = os.getenv("TICKER", "No ticker specified")
    strategy = os.getenv("STRATEGY", "")
    start_date = os.getenv("START_DATE", "No start_date specified")
    start_at_raw: str = os.getenv("START_AT", "0")
    end_date = os.getenv("END_DATE", "No end_date specified")
    num_threads = os.getenv("NUM_THREADS", "1")
    calculate_backtest = eval(os.getenv("BACKTEST", "True"))
    ts_plot = eval(os.getenv("PLOT", "True"))

    ticker_split = [t for t in ticker.split(";") if t not in [""]]
    start_at_split = [dt for dt in start_at_raw.split(";") if dt not in [""]]
    start_date_split = [dt for dt in start_date.split(";") if dt not in [""]]
    end_date_split = [dt for dt in end_date.split(";") if dt not in [""]]
    num = max([len(x) for x in [ticker_split, start_date_split, start_at_split, end_date_split]])

    if len(ticker) == 1 and 1 < num:
        ticker_split = [ticker_split[0]] * num

    try:
        start_date = [datetime.strptime(d, "%Y/%m/%d").date() for d in start_date_split]
        end_date = [datetime.strptime(d, "%Y/%m/%d").date() for d in end_date_split]
        num_start_dates = len(start_date)
        num_end_dates = len(end_date)
        if num_start_dates == 1 and 1 < num:
            start_date = [start_date[0]] * num
            num_start_dates = len(start_date)
        if num_end_dates == 1 and 1 < num:
            end_date = [end_date[0]] * num
            num_end_dates = len(end_date)
        if num_start_dates != len(end_date):
            raise ValueError("Number of start_days does not match number of end_days")
    except ValueError:
        print("Invalid date format, format has to be YYYY/MM/DD but was:")
        print(f"start_date: {start_date}")
        print(f"end_date: {end_date}")
        exit()

    if len(start_at_split) == 1 and len(start_at_split) != num:
        start_at_split = [start_at_raw[0]] * num
    elif len(start_at_split) != num:
        raise ValueError("Number of start_days does not match number of end_days")

    start_at: list[int | date] = []
    for d in start_at_split:
        if d == "":
            continue
        if str.isdigit(d):
            start_at.append(int(d))
        else:
            try:
                start_at.append(datetime.strptime(d, "%Y/%m/%d").date())
            except ValueError:
                print(f"Invalid format, start_at needs to be int or date with format YYYY/MM/DD but was: {d}")

    if str.isnumeric(num_threads):
        num_threads = int(num_threads)
    else:
        print(f"num_threads must be numeric but is: {num_threads}")
        exit()

    strategies: list[BaseStrategy] = []
    for strat_str in strategy.split(";"):
        if strat_str == "":
            continue
        try:
            strat: BaseStrategy = eval(f"{strat_str}(threads={num_threads})")
            strategies.append(strat)
        except Exception as e:
            print(f'"{strat_str}" is not a valid strategy')

    print(f"Ticker: {ticker}")
    print(f"Strategy: {[strategy.__class__.__name__ for strategy in strategies]}")
    print(f"Start Date: {start_date}")
    print(f"Start At: {start_at}")
    print(f"End Date: {end_date}")
    print(f"Num Threads: {num_threads}")

    for tic, start_dt, sta_at, end_dt in zip(ticker_split, start_date, start_at, end_date):
        try:
            data_store = SplitBucketDataStore(ticker=tic, num_threads=num_threads)
        except Exception as e:
            data_store = LocalDataStore(ticker=tic)

        data = data_store.create_datafile(start_date=start_dt, end_date=end_dt)

        if calculate_backtest:
            backtest_results = []
            for strat in strategies:
                backtesting = Backtesting(security=data, strategy=strat, start_at=sta_at, threads=num_threads, iterative=False)
                data_store.upload_backtest(backtesting.result)
                backtest_results.append(backtesting.result)

        backtest_results = []
        for strat in strategies:
            backtest_results.append(data_store.create_backtest_result(start_date=sta_at, end_date=end_dt, strategy_name=strat.__class__.__name__, from_ts=ts_plot))

        agg_res = AggBacktestResult(backtest_results, data_file=data, with_ts=ts_plot)
        data_store.upload_agg_backtest(agg_res, ts_plot)
        print(agg_res.info)
