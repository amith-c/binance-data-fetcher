import os
import time
import requests
import pandas as pd
import appdirs
from pathlib import Path

from rich.progress import Progress

APP_NAME = "BinanceCandleCache"

def _timeframe_to_pandas_freq(tf_str):
    """Converts a timeframe string like '3m' or '1h' to a pandas frequency string."""
    if 'm' in tf_str:
        return f"{int(tf_str.replace('m', ''))}min"
    if 'h' in tf_str:
        return tf_str
    return None

def _download_candlestick_data(
    symbol: str,
    timeframe: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp
) -> pd.DataFrame:
    """
    Downloads candlestick data from Binance API.

    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT').
        timeframe (str): Timeframe string (e.g., '1m', '5m', '1h').
        start_time (pd.Timestamp): Start time for fetching data.
        end_time (pd.Timestamp): End time for fetching data.

    Returns:
        pd.DataFrame: DataFrame containing the downloaded candlestick data with forward-filled missing values.
    """
    all_candles = []

    start_ms = int(start_time.tz_convert("UTC").timestamp() * 1000)
    end_ms = int(end_time.tz_convert("UTC").timestamp() * 1000)
    total_candles = pd.date_range(start=start_time, end=end_time, freq=_timeframe_to_pandas_freq(timeframe), tz='UTC').size
    current_fetch_start = start_ms # This will be updated as we get more and more candles

    with Progress() as progress:
        task = progress.add_task("Downloading candles...", total=total_candles)

        while True:
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={timeframe}&startTime={current_fetch_start}&limit=1000"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                if not data:
                    break

                new_candles = [candle for candle in data if candle[0] <= end_ms]
                all_candles.extend(new_candles)

                if not new_candles or data[-1][0] > end_ms:
                    break

                current_fetch_start = new_candles[-1][0] + 1
                progress.update(task, advance=len(new_candles))
                time.sleep(0.2)
            else:
                return None

    if all_candles:
        ohlc_data = [candle[:6] for candle in all_candles]
        new_df = pd.DataFrame(ohlc_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms').dt.tz_localize('UTC')
        new_df.set_index('timestamp', inplace=True)

        numeric_cols = ["open", "high", "low", "close", "volume"]
        new_df[numeric_cols] = new_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # Fill missing candles with previous OHLC
        new_df = new_df.asfreq(_timeframe_to_pandas_freq(timeframe), method='ffill')

        return new_df
    
def fetch_candlestick_data(
    symbol: str,
    timeframe: str,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp
) -> pd.DataFrame:
    """
    Fetches candlestick data for a given symbol and timeframe, utilizing a local cache.

    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT').
        timeframe (str): Timeframe string (e.g., '1m', '5m', '1h').
        start_time (pd.Timestamp): Start time for fetching data.
        end_time (pd.Timestamp): End time for fetching data.

    Returns:
        pd.DataFrame: DataFrame containing the fetched candlestick data.
    """
    cache_dir = Path(appdirs.user_cache_dir(APP_NAME))
    cache_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{symbol}_{timeframe}.parquet"
    cache_file = cache_dir / file_name

    print(f"Using cache file at: {cache_file}")

    if cache_file.exists():
        main_cache = pd.read_parquet(cache_file)

        # Extract requested range
        requested_data = main_cache[(main_cache.index >= start_time) & (main_cache.index <= end_time)]

        # Check if we have all requested data
        requested_range = pd.date_range(start=start_time, end=end_time, freq=_timeframe_to_pandas_freq(timeframe), tz='UTC')
        missing_timestamps = requested_range.difference(requested_data.index)

        if missing_timestamps.empty:
            return requested_data
        else:
            # Fetch missing data
            fetch_start = missing_timestamps.min()
            fetch_end = missing_timestamps.max()
            new_data = _download_candlestick_data(symbol, timeframe, fetch_start, fetch_end)
            if new_data is not None:
                combined_df = pd.concat([main_cache, new_data]).sort_index().drop_duplicates()
                combined_df.to_parquet(cache_file)
                return combined_df[(combined_df.index >= start_time) & (combined_df.index <= end_time)]
            else:
                return requested_data
    else:
        # Cache does not exist, download all data
        new_data = _download_candlestick_data(symbol, timeframe, start_time, end_time)
        if new_data is not None:
            new_data.to_parquet(cache_file)
            return new_data
        else:
            return pd.DataFrame()