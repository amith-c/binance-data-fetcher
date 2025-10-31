# Binance Fetcher

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python library for fetching and caching Binance candlestick data.

## Installation

Install the package directly from the GitHub repository:

```bash
pip install git+https://github.com/amith-c/binance-data-fetcher.git
```

## Usage

The `fetch_candlestick_data` function is the main entry point of the library. It fetches candlestick data for a given symbol and timeframe, and caches it locally for future use.

```python
import pandas as pd
from binance_fetcher import fetch_candlestick_data

# Fetch 1-hour candlestick data for BTC/USDT from the beginning of 2023
data = fetch_candlestick_data(
    symbol='BTCUSDT',
    timeframe='1h',
    start_time=pd.to_datetime('2023-01-01'),
    end_time=pd.to_datetime('2023-12-31')
)

print(data)
```

## Caching

The library uses a local cache to store the downloaded candlestick data. The cache is located in the user's cache directory, as determined by the `appdirs` library. The data is stored in Parquet format, which is a compressed, columnar storage format that is optimized for use with pandas.

The caching mechanism is designed to be transparent to the user. When you request candlestick data, the library first checks the cache to see if the data is already available. If it is, the data is read from the cache and returned to you. If it is not, the data is downloaded from Binance, stored in the cache, and then returned to you.

## Dependencies

The library has the following dependencies:

*   pandas
*   requests
*   appdirs
*   rich

These dependencies will be automatically installed when you install the library using pip.
