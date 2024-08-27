# Intraday Strategy Tests

This project provides a framework to create and benchmark quantitative trading strategies especially intraday strategies due to the high tick density. This framework additionally provides efficient implemented technical indicators to create own strategies in addition to some already implemented strategies. 

## Data

### DataFile

Contains the Security Data in a unified format which can be provided to the Backtest

### DataStore [DataDownload/DataStore]

- LocalDataStore
- BucketDataStore
- SplitBucketStore
- GDSDataStore (Under Development)
- SQLDataStore (Under Development)

### DataDownloader

Downloads the ticker data from Dukascopy and returns a unified pandas Dataframe, that can be easily converted into a DataFile

## Provided Indicators [Backtesting/Indicator]

- BollingerBands
- ExponentialMovingAverage [EMA]
- KeltnerChannels
- RelativeStrengthIndex [RSI]
- SimpleAverageTrueRange [ATR]
- SimpleMovingAverage [SMA]
- StochasticOscillator

## Provided Strategies [Backtesting/Strategy]

- MomentumStrategy
- TrendStrategy
- VolatilityStrategy
- CombinationStrategy
- GoldenCrossStrategy

## How to Run and what to configure

### Configure Env Variables
- TICKER [corresponding dukascopy ticker] 
- STRATEGY [class name of strategy]
- START_DATE ["%Y/%m/%d", date where the timeseries starts]
- START_AT ["%Y/%m/%d", date where backtest starts]
- END_DATE ["%Y/%m/%d", end date of timeseries]
- NUM_THREADS [int, number of threads for parallelising]:
- BACKTEST [True/False, backtest strategies]
- PLOT [True/False, create plot of backtest performance]

### Connect google cloud authentication

- To connect your google cloud you need to:
  - create a service account key and download the json file
  - rename the json file to google_cloud_authentication.json
  - locate the json file in the source root