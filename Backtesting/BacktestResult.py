from datetime import date

import pandas as pd


class BacktestResult:
    def __init__(self, ticker: str, weights: pd.Series, performance_rel: pd.Series, strategy_name: str = "NoDefStrat"):
        self.ticker: str = ticker
        self.strategy_name: str = strategy_name

        # ts
        self.weights: pd.Series = weights
        self.performance_rel: pd.Series = performance_rel

        # daily
        grouped_last = self.performance_rel.groupby(self.performance_rel.index.date).last()
        self.daily_performance = grouped_last.divide(grouped_last.shift(1)).add(-1)
        self.daily_performance.iloc[0] = grouped_last.iloc[0] - 1
        positive_days = self.daily_performance[self.daily_performance.gt(0)]
        neutral_days = self.daily_performance[self.daily_performance.eq(0)]
        negative_days = self.daily_performance[self.daily_performance.lt(0)]

        # info
        self.start_date: date = self.weights.index[0].date()
        self.end_date: date = self.weights.index[-1].date()
        self.num_days: int = len(self.daily_performance)
        self.number_neutral_days = len(neutral_days)
        self.total_return = self.performance_rel.iloc[-1] - 1
        self.min_daily_return = self.daily_performance.min()
        self.average_daily_return = self.daily_performance.mean()
        self.max_daily_return = self.daily_performance.max()

        self.number_positive_days = len(positive_days)
        self.average_positive_days_return = positive_days.mean()
        self.number_negative_days = len(negative_days)
        self.average_negative_days_return = negative_days.mean()

        ret = self.performance_rel.pct_change()
        self.var = ret.var()
        self.std_dev = ret.std()

    def to_ts_df(self) -> pd.DataFrame:
        return pd.concat([self.weights, self.performance_rel], axis="columns", ignore_index=False)

    def to_daily(self) -> pd.DataFrame:
        return self.daily_performance.to_frame("daily_performance")

    def to_info_df(self) -> pd.DataFrame:
        return pd.Series(
            dict(
                start_date=self.start_date,
                end_date=self.end_date,
                num_days=self.num_days,
                number_neutral_days=self.number_neutral_days,
                total_return=self.total_return,
                min_daily_return=self.min_daily_return,
                average_daily_return=self.average_daily_return,
                max_daily_return=self.max_daily_return,
                number_positive_days=self.number_positive_days,
                average_positive_days_return=self.average_positive_days_return,
                number_negative_days=self.number_negative_days,
                average_negative_days_return=self.average_negative_days_return,
                var=self.var,
                std_dev=self.std_dev,
            )
        ).to_frame(self.strategy_name)
