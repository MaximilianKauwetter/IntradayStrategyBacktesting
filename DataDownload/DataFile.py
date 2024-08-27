from datetime import date, datetime

import matplotlib.pyplot as plt
import pandas as pd


class DataFile:
    def __init__(self, ticker: str, df_ts: pd.DataFrame):
        self.ticker = ticker
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
        return DataFile(ticker=self.ticker, df_ts=self._df.loc[self._df.index <= end_date])

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
