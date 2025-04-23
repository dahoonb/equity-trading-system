# filename: backtest/data.py
# backtest/data.py
import queue
import pandas as pd
import numpy as np # Need numpy for NaN checks
import datetime
import os # Added for exists check
from core.event_queue import event_queue
from core.events import MarketEvent
from utils.logger import setup_logger, logger

logger = setup_logger()

class CSVSimDataHandler:
    """
    Handles loading and streaming historical CSV data for backtesting.
    Includes data validation checks during loading.
    """
    def __init__(self, csv_dir: str, symbols: list, event_q: queue.Queue,
                 start_date: datetime.datetime, end_date: datetime.datetime,
                 # --- ADDED: Validation parameters ---
                 validation_config: dict = None):
        """
        Initializes the data handler.

        Args:
            csv_dir: Directory containing the CSV files.
            symbols: List of symbols to load.
            event_q: The event queue.
            start_date: Backtest start date (inclusive).
            end_date: Backtest end date (inclusive).
            validation_config: Optional dict with validation parameters, e.g.,
                               {'max_daily_return': 0.5}
        """
        self.csv_dir = csv_dir
        self.symbols = list(set(symbols)) # Ensure unique symbols
        self.event_queue = event_q
        self.start_date = start_date
        self.end_date = end_date
        self.validation_config = validation_config or {} # Store validation settings

        self.symbol_data = {} # Stores validated dataframes
        # self.symbol_iters = {} # Iterators are created during streaming start
        self.current_bar_time = None
        self.trading_days = None
        self.day_iter = None

        self._load_and_validate_data() # Changed method name for clarity
        self._prepare_streaming() # Prepare iterators after loading

    def _validate_dataframe(self, symbol: str, df: pd.DataFrame) -> bool:
        """
        Performs validation checks on the loaded DataFrame for a symbol.

        Returns:
            True if data is valid, False otherwise.
        """
        logger.debug(f"Validating data for {symbol}...")
        required_cols = ['open', 'high', 'low', 'close', 'volume']

        # 1. Check for required columns
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Validation Failed ({symbol}): Missing required columns: {missing_cols}")
            return False

        # 2. Check for NaN values in OHLCV columns
        if df[required_cols].isnull().values.any():
            nan_counts = df[required_cols].isnull().sum()
            logger.error(f"Validation Failed ({symbol}): Contains NaN values. Counts:\n{nan_counts[nan_counts > 0]}")
            # Option: Fill NaNs if appropriate, or fail validation. Failing is safer.
            return False

        # 3. Check for non-positive prices (O H L C) and negative volume
        if (df[['open', 'high', 'low', 'close']] <= 0).values.any():
            logger.error(f"Validation Failed ({symbol}): Contains zero or negative price values.")
            return False
        if (df['volume'] < 0).values.any():
            logger.error(f"Validation Failed ({symbol}): Contains negative volume values.")
            return False
        # Optional: Check for zero volume days (might be valid but worth noting)
        zero_volume_days = (df['volume'] == 0).sum()
        if zero_volume_days > 0:
             logger.warning(f"Validation Info ({symbol}): Found {zero_volume_days} days with zero volume.")

        # 4. Optional: Check for extreme daily returns (potential outliers/bad data)
        max_daily_return_threshold = self.validation_config.get('max_daily_return', 0.5) # e.g., 50% limit
        if max_daily_return_threshold and 'close' in df.columns:
             daily_returns = df['close'].pct_change().abs()
             extreme_returns = daily_returns[daily_returns > max_daily_return_threshold]
             if not extreme_returns.empty:
                  logger.error(f"Validation Failed ({symbol}): Found {len(extreme_returns)} days with absolute daily return > {max_daily_return_threshold*100:.1f}%. Dates: {extreme_returns.index.date.tolist()}")
                  return False

        # 5. Optional: Check High >= Low, High >= Open/Close, Low <= Open/Close
        if not (df['high'] >= df['low']).all():
             logger.error(f"Validation Failed ({symbol}): Found instances where High < Low.")
             return False
        if not (df['high'] >= df['open']).all() or not (df['high'] >= df['close']).all():
             logger.error(f"Validation Failed ({symbol}): Found instances where High < Open or High < Close.")
             return False
        if not (df['low'] <= df['open']).all() or not (df['low'] <= df['close']).all():
             logger.error(f"Validation Failed ({symbol}): Found instances where Low > Open or Low > Close.")
             return False

        # Add more checks if needed (e.g., date continuity)

        logger.debug(f"Validation Passed for {symbol}.")
        return True


    def _load_and_validate_data(self):
        """Loads and validates CSV data for all symbols."""
        logger.info("Loading and validating historical data for backtest...")
        loaded_symbols = []
        temp_data_store = {} # Store temporarily before combining dates

        for symbol in self.symbols:
            filepath = os.path.join(self.csv_dir, f"{symbol}.csv") # Use os.path.join
            logger.debug(f"Attempting to load data for {symbol} from {filepath}")

            if not os.path.exists(filepath):
                 logger.error(f"Data Loading Failed ({symbol}): CSV file not found at {filepath}")
                 continue # Skip this symbol

            try:
                df = pd.read_csv(filepath, index_col='date', parse_dates=True)

                # Ensure index is DatetimeIndex
                if not isinstance(df.index, pd.DatetimeIndex):
                     logger.warning(f"Index for {symbol} is not DatetimeIndex after loading. Attempting conversion.")
                     df.index = pd.to_datetime(df.index)

                # Filter by date range BEFORE validation
                df = df[(df.index >= self.start_date) & (df.index <= self.end_date)]

                if df.empty:
                    logger.warning(f"No data found for {symbol} within the specified date range ({self.start_date.date()} to {self.end_date.date()}). Skipping symbol.")
                    continue

                # Ensure standard column names (lowercase)
                df.columns = [x.lower().strip() for x in df.columns] # Add strip()

                # Handle adjusted close BEFORE validation
                if 'adj close' in df.columns:
                    df['close'] = df['adj close']
                    # Optional: Validate consistency between close and adj close?
                    # if 'close' in df.columns and not np.allclose(df['close_orig'], df['close']):
                    #     logger.warning(...)

                # Keep only necessary columns (helps validation)
                ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
                cols_to_keep = [col for col in ohlcv_cols if col in df.columns]
                df = df[cols_to_keep]

                # Ensure numeric types AFTER selecting columns
                try:
                     for col in ohlcv_cols:
                          if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception as type_e:
                     logger.error(f"Validation Failed ({symbol}): Error converting OHLCV columns to numeric: {type_e}")
                     continue # Skip this symbol

                # --- Call Validation ---
                if self._validate_dataframe(symbol, df):
                    df = df.sort_index() # Sort after validation passed
                    # Store the validated data
                    temp_data_store[symbol] = df
                    loaded_symbols.append(symbol)
                    logger.info(f"Successfully loaded and validated {len(df)} bars for {symbol}")
                else:
                    logger.error(f"Data validation failed for {symbol}. Symbol will be excluded from the backtest.")

            except Exception as e:
                logger.exception(f"Error loading or validating data for {symbol} from {filepath}: {e}")
                # Ensure symbol is not included if any error occurred

        # --- Finalize ---
        self.symbol_data = temp_data_store # Assign validated data
        self.symbols = loaded_symbols # Update symbol list to only include validated ones

        if not self.symbol_data:
             logger.critical("No valid historical data loaded for any symbol. Backtest cannot proceed.")
             # raise ValueError("No valid historical data loaded.") # Option to halt immediately
        else:
             logger.info(f"Finished loading data. Validated symbols: {self.symbols}")

    def _prepare_streaming(self):
        """Prepares data iterators and combined trading days for streaming."""
        if not self.symbol_data:
             logger.warning("No symbol data available to prepare for streaming.")
             return

        # Combine all dates from validated data and find unique sorted dates
        all_indices = [df.index for df in self.symbol_data.values()]
        if not all_indices:
            self.trading_days = pd.DatetimeIndex([])
        else:
            # Ensure all indices are timezone-aware (UTC) before union
            aware_indices = []
            for idx in all_indices:
                if isinstance(idx, pd.DatetimeIndex):
                     if idx.tz is None: aware_indices.append(idx.tz_localize('UTC'))
                     elif idx.tz != datetime.timezone.utc: aware_indices.append(idx.tz_convert('UTC'))
                     else: aware_indices.append(idx)

            if not aware_indices: self.trading_days = pd.DatetimeIndex([])
            else: self.trading_days = aware_indices[0].union_many(aware_indices[1:]).sort_values()

        self.day_iter = iter(self.trading_days)
        # No need for symbol_iters here, data is accessed via .loc in stream_next_bar
        logger.info(f"Data streaming prepared. Backtest period covers {len(self.trading_days)} unique trading days across validated symbols.")


    def stream_next_bar(self):
        """Pushes the next bar's MarketEvents onto the queue for all symbols."""
        if self.day_iter is None: # Handle case where no data was loaded
            logger.error("Data streaming cannot start, day iterator not initialized.")
            return False

        try:
            self.current_bar_time = next(self.day_iter)
            # Ensure the time is UTC
            if self.current_bar_time.tzinfo is None:
                 self.current_bar_time = self.current_bar_time.tz_localize('UTC')
            elif self.current_bar_time.tz != datetime.timezone.utc:
                 self.current_bar_time = self.current_bar_time.tz_convert('UTC')

        except StopIteration:
            logger.info("End of historical data reached.")
            self.current_bar_time = None # Reset current time
            return False # Signal end of backtest

        # logger.debug(f"Streaming bar for date: {self.current_bar_time.strftime('%Y-%m-%d')}")
        symbols_with_data_today = 0
        for symbol in self.symbols: # Iterate only over validated symbols
            if symbol in self.symbol_data:
                try:
                    # Get the row for the current date using .loc
                    # Use precise timestamp matching
                    bar = self.symbol_data[symbol].loc[self.current_bar_time]
                    bar_data = bar.to_dict() # Convert the Series to dict

                    # Double-check for NaNs just before sending (should be caught by validation)
                    if any(pd.isna(v) for v in bar_data.values()):
                         logger.warning(f"NaN value encountered during streaming for {symbol} on {self.current_bar_time}. Skipping event.")
                         continue

                    event = MarketEvent(
                        timestamp=self.current_bar_time,
                        symbol=symbol,
                        data_type='BAR',
                        data=bar_data
                    )
                    self.event_queue.put(event)
                    symbols_with_data_today += 1
                except KeyError:
                    # Symbol might not have data for this specific day (this is normal)
                    # logger.debug(f"No data for {symbol} on {self.current_bar_time.strftime('%Y-%m-%d')}")
                    pass
                except Exception as e:
                     logger.exception(f"Error processing/streaming bar for {symbol} on {self.current_bar_time}: {e}")

        # if symbols_with_data_today == 0:
        #      logger.warning(f"No symbols had data for the current bar time: {self.current_bar_time}")

        return True # More data available