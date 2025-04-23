# strategy/momentum.py (Revised and Completed)
import queue
import pandas as pd
import numpy as np # Still needed for NaN checks and array operations
import talib # Still needed for SMA calculation
import math # Still needed for isnan checks
import logging
from typing import Optional # For type hinting if needed

# Import base class and event types
from strategy.base import BaseStrategy
from core.events import SignalEvent, MarketEvent

# Ensure logger is configured (assuming setup_logger is called elsewhere)
logger = logging.getLogger("TradingSystem")

class MovingAverageCrossoverStrategy(BaseStrategy):
    """
    Implements a simple Moving Average Crossover momentum strategy.

    Generates a LONG signal when the short-term moving average crosses
    above the long-term moving average.
    Generates an EXIT signal for existing long positions when the short-term
    moving average crosses below the long-term moving average.
    Stop-loss is calculated using ATR via the BaseStrategy.
    """
    def __init__(self, symbols: list, event_q: queue.Queue,
                 short_window: int = 50, long_window: int = 200, **kwargs):
        """
        Initializes the MovingAverageCrossoverStrategy.

        Args:
            symbols: A list of ticker symbols this strategy trades.
            event_q: The main event queue for the system.
            short_window: The lookback period for the short-term SMA.
            long_window: The lookback period for the long-term SMA.
            **kwargs: Additional keyword arguments passed to BaseStrategy
                      (e.g., atr_period, atr_stop_mult, fallback_stop_pct).
        """
        # Pass symbols, event_q, and any ATR/stop parameters (**kwargs) to the base class
        super().__init__(symbols, event_q, **kwargs)

        if not isinstance(short_window, int) or short_window <= 0:
            raise ValueError("short_window must be a positive integer.")
        if not isinstance(long_window, int) or long_window <= short_window:
            raise ValueError("long_window must be a positive integer greater than short_window.")

        self.short_window = short_window
        self.long_window = long_window

        # Log the specific parameters being used, including inherited ones
        logger.info(
            f"Initialized MovingAverageCrossoverStrategy "
            f"(Short MA: {self.short_window}, Long MA: {self.long_window}, "
            f"ATR Stop: {self.atr_stop_mult}x{self.atr_period}, "
            f"Fallback Stop: {self.fallback_stop_pct*100:.1f}%)"
        )

    # The _calculate_stop method is inherited from BaseStrategy

    def calculate_signals(self, event: MarketEvent):
        """
        Calculates moving averages and generates LONG/EXIT signals based on crossovers.
        """
        symbol = event.symbol
        data_df = self.symbol_data.get(symbol)

        # Ensure we have enough data for the longest window + 1 for comparison
        if data_df is None or len(data_df) < self.long_window + 1:
            # logger.debug(f"Not enough data ({len(data_df) if data_df is not None else 0} < {self.long_window + 1}) for {symbol} MA calculation.")
            return

        try:
            # Ensure 'close' column exists and is numeric
            if 'close' not in data_df.columns:
                 logger.error(f"Missing 'close' column for {symbol}.")
                 return
            closes = data_df['close'].values # Use numpy array for talib
            if not np.issubdtype(closes.dtype, np.number):
                 logger.error(f"'close' column for {symbol} is not numeric.")
                 return

            # Calculate SMAs using TA-Lib
            short_mavg = talib.SMA(closes, timeperiod=self.short_window)
            long_mavg = talib.SMA(closes, timeperiod=self.long_window)

            # Check if we have enough non-NaN values for comparison (need current and previous)
            if len(short_mavg) < 2 or len(long_mavg) < 2 or \
               np.isnan(short_mavg[-1]) or np.isnan(long_mavg[-1]) or \
               np.isnan(short_mavg[-2]) or np.isnan(long_mavg[-2]):
                # logger.debug(f"SMA calculation resulted in insufficient non-NaN values for {symbol}.")
                return # Skip if MAs are NaN or not enough history

            latest_short = short_mavg[-1]
            latest_long = long_mavg[-1]
            prev_short = short_mavg[-2]
            prev_long = long_mavg[-2]

            current_position = self.positions.get(symbol, 0.0)
            # Use the close price of the bar that triggered the event as reference
            entry_price_ref = closes[-1]

            # --- Signal Logic ---

            # Bullish Crossover: Short MA crosses above Long MA
            if prev_short <= prev_long and latest_short > latest_long:
                # Enter LONG only if currently flat or short (though shorting isn't handled here)
                if current_position <= 1e-9: # Check if not already long (allow for float inaccuracy)
                    logger.info(f"MOMENTUM [{symbol}]: Bullish Crossover detected. "
                                f"Short MA({self.short_window})={latest_short:.2f}, Long MA({self.long_window})={latest_long:.2f}. "
                                f"Ref Price: {entry_price_ref:.2f}")

                    # Calculate stop loss using the inherited method
                    stop_price = self._calculate_stop(symbol, entry_price_ref)

                    # Ensure stop price is valid (not None and below entry)
                    if stop_price is not None and stop_price < entry_price_ref:
                        signal = SignalEvent(
                            timestamp=event.timestamp,
                            symbol=symbol,
                            direction='LONG',
                            stop_price=stop_price,
                            entry_price=entry_price_ref # Pass reference price
                        )
                        self.event_queue.put(signal)
                        logger.info(f"Generated LONG signal for {symbol} with stop at {stop_price:.2f}")
                    else:
                         logger.error(f"Invalid stop price ({stop_price}) calculated for LONG signal {symbol}. Signal aborted.")
                # else: logger.debug(f"Bullish crossover on {symbol}, but already long ({current_position}). No signal.")


            # Bearish Crossover: Short MA crosses below Long MA
            elif prev_short >= prev_long and latest_short < latest_long:
                # Exit LONG position if currently held
                if current_position > 1e-9: # Check if currently long
                    logger.info(f"MOMENTUM [{symbol}]: Bearish Crossover detected. "
                                f"Short MA({self.short_window})={latest_short:.2f}, Long MA({self.long_window})={latest_long:.2f}. "
                                f"Exiting long position @ ~{entry_price_ref:.2f}.")
                    signal = SignalEvent(
                        timestamp=event.timestamp,
                        symbol=symbol,
                        direction='EXIT', # Signal to flatten the position
                        entry_price=entry_price_ref # Pass reference price
                    )
                    self.event_queue.put(signal)
                # else: logger.debug(f"Bearish crossover on {symbol}, but not long ({current_position}). No signal.")

        except IndexError as e:
             # This might happen if data length is exactly window size, preventing [-2] access
             logger.debug(f"IndexError during SMA signal calculation for {symbol}, likely boundary condition: {e}")
        except Exception as e:
            # Catch any other unexpected errors during calculation
            logger.exception(f"Error calculating momentum signals for {symbol}: {e}")