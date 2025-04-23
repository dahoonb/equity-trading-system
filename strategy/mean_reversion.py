# filename: strategy/mean_reversion.py
# strategy/mean_reversion.py (Modified for Solution C - Passing ATR in SignalEvent)
import queue
import pandas as pd
import numpy as np # Still needed for NaN checks and array operations
import talib # Still needed for RSI calculation
import math # Still needed for isnan checks
import logging
from typing import Optional, Tuple # Import Tuple if type hinting _calculate_stop return

# Import base class and event types
from strategy.base import BaseStrategy
from core.events import SignalEvent, MarketEvent

# Ensure logger is configured (assuming setup_logger is called elsewhere)
logger = logging.getLogger("TradingSystem")

class RsiMeanReversionStrategy(BaseStrategy):
    """
    Implements a simple RSI-based mean reversion strategy (long-only).

    Generates a LONG signal when the RSI crosses below an oversold threshold.
    Generates an EXIT signal for existing long positions when the RSI crosses
    above an exit threshold.
    Stop-loss is calculated using ATR via the BaseStrategy.
    ATR value is included in the LONG SignalEvent for volatility scaling.
    """
    def __init__(self, symbols: list, event_q: queue.Queue,
                 rsi_period: int = 14, oversold_threshold: int = 30,
                 exit_threshold: int = 50, **kwargs):
        """
        Initializes the RsiMeanReversionStrategy.

        Args:
            symbols: A list of ticker symbols this strategy trades.
            event_q: The main event queue for the system.
            rsi_period: The lookback period for the RSI calculation.
            oversold_threshold: The RSI level below which a LONG signal is generated.
            exit_threshold: The RSI level above which an EXIT signal is generated.
            **kwargs: Additional keyword arguments passed to BaseStrategy
                      (e.g., atr_period, atr_stop_mult, fallback_stop_pct).
        """
        # Pass symbols, event_q, and any ATR/stop parameters (**kwargs) to the base class
        super().__init__(symbols, event_q, **kwargs)

        if not isinstance(rsi_period, int) or rsi_period <= 1:
            raise ValueError("rsi_period must be an integer greater than 1.")
        if not isinstance(oversold_threshold, int) or not (0 < oversold_threshold < 100):
            raise ValueError("oversold_threshold must be an integer between 0 and 100.")
        if not isinstance(exit_threshold, int) or not (oversold_threshold < exit_threshold < 100):
            raise ValueError("exit_threshold must be an integer between oversold_threshold and 100.")

        self.rsi_period = rsi_period
        self.oversold_threshold = oversold_threshold
        self.exit_threshold = exit_threshold

        # Log the specific parameters being used, including inherited ones
        logger.info(
            f"Initialized RsiMeanReversionStrategy "
            f"(RSI Period: {self.rsi_period}, Oversold: {self.oversold_threshold}, Exit: {self.exit_threshold}, "
            f"ATR Stop: {self.atr_stop_mult}x{self.atr_period}, "
            f"Fallback Stop: {self.fallback_stop_pct*100:.1f}%)"
        )

    # The _calculate_stop method is inherited from BaseStrategy and now returns (stop, atr)

    def calculate_signals(self, event: MarketEvent):
        """
        Calculates RSI and generates LONG/EXIT signals based on threshold crosses.
        Includes ATR value in LONG signals.
        """
        symbol = event.symbol
        data_df = self.symbol_data.get(symbol)

        # Ensure we have enough data for RSI calculation + 1 for comparison
        if data_df is None or len(data_df) < self.rsi_period + 1:
            # logger.debug(f"Not enough data ({len(data_df) if data_df is not None else 0} < {self.rsi_period + 1}) for {symbol} RSI calculation.")
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

            # Calculate RSI using TA-Lib
            rsi = talib.RSI(closes, timeperiod=self.rsi_period)

            # Check if we have enough non-NaN values for comparison (need current and previous)
            if len(rsi) < self.rsi_period + 1 or np.isnan(rsi[-1]) or np.isnan(rsi[-2]):
                 # logger.debug(f"RSI calculation resulted in insufficient non-NaN values for {symbol}. Length: {len(rsi)}")
                 return # Skip if RSI is NaN or not enough history

            latest_rsi = rsi[-1]
            prev_rsi = rsi[-2]

            current_position = self.positions.get(symbol, 0.0)
            # Use the close price of the bar that triggered the event as reference
            entry_price_ref = closes[-1]

            # --- Signal Logic ---

            # Entry Signal: RSI crosses below oversold threshold
            if prev_rsi >= self.oversold_threshold and latest_rsi < self.oversold_threshold:
                # Enter LONG only if currently flat
                if current_position <= 1e-9: # Check if not already long
                    logger.info(f"MEAN REVERSION [{symbol}]: RSI crossed below {self.oversold_threshold} "
                                f"(Prev: {prev_rsi:.1f}, Curr: {latest_rsi:.1f}). Entering long @ ~{entry_price_ref:.2f}.")

                    # --- MODIFICATION: Capture both stop and ATR ---
                    # Call the modified _calculate_stop which returns (stop_price, atr_value)
                    stop_price, atr_value = self._calculate_stop(symbol, entry_price_ref)
                    # --- END MODIFICATION ---

                    # Ensure stop price is valid (not None and below entry)
                    if stop_price is not None and stop_price < entry_price_ref:
                        # --- MODIFICATION: Pass atr_value to SignalEvent ---
                        signal = SignalEvent(
                            timestamp=event.timestamp,
                            symbol=symbol,
                            direction='LONG',
                            stop_price=stop_price,
                            entry_price=entry_price_ref, # Pass reference price
                            atr_value=atr_value # Include the calculated ATR
                        )
                        # --- END MODIFICATION ---
                        self.event_queue.put(signal)
                        # --- MODIFICATION: Log ATR value ---
                        logger.info(f"Generated LONG signal for {symbol} with stop at {stop_price:.2f} (ATR: {atr_value:.3f if atr_value is not None else 'N/A'})")
                        # --- END MODIFICATION ---
                    else:
                         # Log error if stop price is invalid (atr_value might be None here too)
                         logger.error(f"Invalid stop price ({stop_price}) calculated for LONG signal {symbol}. Signal aborted.")
                # else: logger.debug(f"RSI oversold on {symbol}, but already long ({current_position}). No signal.")

            # Exit Signal: RSI crosses above exit threshold
            elif prev_rsi <= self.exit_threshold and latest_rsi > self.exit_threshold:
                # Exit LONG position if currently held
                if current_position > 1e-9: # Check if currently long
                    logger.info(f"MEAN REVERSION [{symbol}]: RSI crossed above {self.exit_threshold} "
                                f"(Prev: {prev_rsi:.1f}, Curr: {latest_rsi:.1f}). Exiting long position @ ~{entry_price_ref:.2f}.")
                    # EXIT signals don't need ATR for volatility scaling
                    signal = SignalEvent(
                        timestamp=event.timestamp,
                        symbol=symbol,
                        direction='EXIT', # Signal to flatten the position
                        entry_price=entry_price_ref # Pass reference price
                        # No stop_price or atr_value needed for EXIT
                    )
                    self.event_queue.put(signal)
                # else: logger.debug(f"RSI exit threshold crossed on {symbol}, but not long ({current_position}). No signal.")

        except IndexError as e:
             # This might happen if data length is exactly window size, preventing [-2] access
             logger.debug(f"IndexError during RSI signal calculation for {symbol}, likely boundary condition: {e}")
        except Exception as e:
            # Catch any other unexpected errors during calculation
            logger.exception(f"Error calculating mean reversion signals for {symbol}: {e}")