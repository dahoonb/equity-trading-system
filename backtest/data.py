# backtest/data.py
import queue
import pandas as pd
import datetime
from core.event_queue import event_queue
from core.events import MarketEvent
from utils.logger import setup_logger

logger = setup_logger()

class CSVSimDataHandler:
    def __init__(self, csv_dir: str, symbols: list, event_q: queue.Queue,
                 start_date: datetime.datetime, end_date: datetime.datetime):
        self.csv_dir = csv_dir
        self.symbols = symbols
        self.event_queue = event_q
        self.start_date = start_date
        self.end_date = end_date
        self.symbol_data = {} # Stores loaded dataframes
        self.symbol_iters = {} # Iterators for each symbol's data
        self.current_bar_time = None
        self._load_data()

    def _load_data(self):
        """Loads CSV data for all symbols."""
        logger.info("Loading historical data for backtest...")
        for symbol in self.symbols:
            filepath = f"{self.csv_dir}/{symbol}.csv"
            try:
                df = pd.read_csv(filepath, index_col='date', parse_dates=True)
                df = df[(df.index >= self.start_date) & (df.index <= self.end_date)]
                # Ensure standard column names (lowercase)
                df.columns = [x.lower() for x in df.columns]
                if 'adj close' in df.columns: # Use adjusted close if available
                    df['close'] = df['adj close']
                self.symbol_data[symbol] = df.sort_index()
                self.symbol_iters[symbol] = self.symbol_data[symbol].iterrows()
                logger.info(f"Loaded {len(df)} bars for {symbol} from {self.start_date} to {self.end_date}")
            except FileNotFoundError:
                logger.error(f"CSV file not found for symbol: {symbol} at {filepath}")
            except Exception as e:
                logger.error(f"Error loading data for {symbol}: {e}")
        # Combine all dates and find the unique sorted dates for iteration
        all_dates = pd.concat([df.index.to_series() for df in self.symbol_data.values()]).unique()
        self.trading_days = pd.to_datetime(all_dates).sort_values()
        self.day_iter = iter(self.trading_days)
        logger.info(f"Backtest period covers {len(self.trading_days)} trading days.")

    def stream_next_bar(self):
        """Pushes the next bar's MarketEvents onto the queue for all symbols."""
        try:
            self.current_bar_time = next(self.day_iter)
        except StopIteration:
            logger.info("End of historical data reached.")
            return False # Signal end of backtest

        logger.debug(f"Streaming bar for date: {self.current_bar_time.strftime('%Y-%m-%d')}")
        for symbol in self.symbols:
            if symbol in self.symbol_data:
                try:
                    # Get the row for the current date
                    bar = self.symbol_data[symbol].loc[self.current_bar_time]
                    bar_data = bar.to_dict()
                    event = MarketEvent(
                        timestamp=self.current_bar_time,
                        symbol=symbol,
                        data_type='BAR',
                        data=bar_data
                    )
                    self.event_queue.put(event)
                except KeyError:
                    # Symbol might not have data for this specific day
                    # logger.debug(f"No data for {symbol} on {self.current_bar_time.strftime('%Y-%m-%d')}")
                    pass
                except Exception as e:
                     logger.error(f"Error processing bar for {symbol} on {self.current_bar_time}: {e}")

        return True # More data available