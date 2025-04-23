# filename: strategy/base.py
# strategy/base.py (Revised stop calculation check)
import queue
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import talib
import math
import datetime
import logging
from typing import Optional

from core.events import MarketEvent, SignalEvent

logger = logging.getLogger("TradingSystem")

class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies. Provides common functionality.
    """
    def __init__(self, symbols: list, event_q: queue.Queue,
                 atr_period: int = 14, atr_stop_mult: float = 2.0,
                 fallback_stop_pct: float = 0.05, max_bars: int = 500):
        """Initializes the BaseStrategy."""
        if not isinstance(symbols, list): raise TypeError("Symbols must be a list.")
        if not isinstance(event_q, queue.Queue): raise TypeError("event_q must be a queue.Queue instance.")
        self.symbols = list(set(symbols))
        self.event_queue = event_q
        self.symbol_data = { symbol: pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume']) for symbol in self.symbols }
        for symbol in self.symbols:
            # Ensure index is DatetimeIndex with UTC timezone from the start
             self.symbol_data[symbol].index = pd.to_datetime(self.symbol_data[symbol].index).tz_localize('UTC')
        self.max_bars = max_bars
        self.positions = {symbol: 0.0 for symbol in self.symbols}
        self.atr_period = atr_period
        self.atr_stop_mult = atr_stop_mult
        self.fallback_stop_pct = fallback_stop_pct
        logger.info(f"BaseStrategy initialized for {len(self.symbols)} symbols. Max Bars: {self.max_bars}, ATR Stop: {self.atr_stop_mult}x{self.atr_period} (Fallback: {self.fallback_stop_pct*100:.1f}%)")

    def update_position(self, symbol: str, quantity: float):
        """Allows the Portfolio Manager to update the strategy's view of the position."""
        if symbol in self.symbols:
            current_pos = self.positions.get(symbol, 0.0)
            if abs(current_pos - quantity) > 1e-9:
                 logger.info(f"Strategy {self.__class__.__name__} updating position for {symbol}: {current_pos} -> {quantity}")
                 self.positions[symbol] = quantity
        else:
             logger.warning(f"Strategy {self.__class__.__name__} received update for untracked symbol: {symbol}")

    def _store_market_data(self, event: MarketEvent):
        """Appends new market data (BAR type) and maintains max_bars limit."""
        symbol = event.symbol
        if event.data_type != 'BAR': return
        timestamp = event.timestamp
        if timestamp.tzinfo is None: timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        else: timestamp = timestamp.astimezone(datetime.timezone.utc)
        required_cols = {'open', 'high', 'low', 'close', 'volume'}
        event_data_keys_lower = {k.lower().replace(' ', '_'): v for k, v in event.data.items()}
        if not required_cols.issubset(event_data_keys_lower.keys()):
            missing_cols = required_cols - event_data_keys_lower.keys()
            logger.warning(f"MarketEvent BAR for {symbol} at {timestamp} missing required columns: {missing_cols}")
            return
        try:
            new_row_data = {col: pd.to_numeric(event_data_keys_lower[col], errors='coerce') for col in required_cols}
        except Exception as e:
            logger.error(f"Error converting BAR data for {symbol} at {timestamp}: {e} - Data: {event.data}")
            return
        if any(math.isnan(v) for v in new_row_data.values()):
            logger.warning(f"NaN value detected in BAR data for {symbol} at {timestamp}. Row skipped. Data: {new_row_data}")
            return
        new_data = pd.DataFrame([new_row_data], index=[timestamp])
        combined_df = pd.concat([self.symbol_data[symbol], new_data])
        # Drop duplicate indices (timestamps), keeping the latest data
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
        # Slice to keep only the last max_bars
        self.symbol_data[symbol] = combined_df.iloc[-self.max_bars:]
        # logger.debug(f"Stored BAR for {symbol}. Data length: {len(self.symbol_data[symbol])}")


    def _calculate_stop(self, symbol: str, entry_price: float) -> Optional[float]:
        """
        Calculates the stop-loss price based on ATR.
        Uses common parameters defined in the strategy instance.
        Returns the calculated stop price (rounded) or None if calculation fails.
        """
        if not isinstance(entry_price, (int, float)) or entry_price <= 0:
             logger.error(f"Invalid entry price ({entry_price}) for stop calculation on {symbol}.")
             return None

        data_df = self.symbol_data.get(symbol)
        # Calculate fallback stop first (ensure it's always below entry)
        fallback_stop = round(max(0.01, entry_price * (1.0 - self.fallback_stop_pct)), 2)

        if data_df is None or len(data_df) < self.atr_period + 1:
            logger.warning(f"Not enough data ({len(data_df) if data_df is not None else 0} < {self.atr_period+1}) for ATR({self.atr_period}) for {symbol}. Using fallback stop: {fallback_stop:.2f}")
            return fallback_stop

        try:
            # Ensure correct types and sufficient non-NaN data for TA-Lib
            # Get enough data for lookback + buffer for initial NaNs in ATR
            required_len = self.atr_period * 2 # Heuristic, might need adjustment
            if len(data_df) < required_len:
                 logger.warning(f"Data length ({len(data_df)}) potentially too short for stable ATR({self.atr_period}) for {symbol}. Result might be inaccurate. Using fallback stop: {fallback_stop:.2f}")
                 return fallback_stop

            highs = data_df['high'].values.astype(float)
            lows = data_df['low'].values.astype(float)
            closes = data_df['close'].values.astype(float)

            # Double check for NaNs *before* passing to talib
            if np.isnan(highs[-required_len:]).any() or \
               np.isnan(lows[-required_len:]).any() or \
               np.isnan(closes[-required_len:]).any():
                logger.warning(f"NaN values found in recent data slice needed for ATR({self.atr_period}) for {symbol}. Using fallback stop: {fallback_stop:.2f}")
                return fallback_stop

            atr = talib.ATR(highs, lows, closes, timeperiod=self.atr_period)

            # Check if ATR calculation resulted in NaNs at the end
            if atr is None or len(atr) == 0 or np.isnan(atr[-1]):
                 logger.warning(f"ATR calculation resulted in NaN or empty series for {symbol}. Using fallback stop: {fallback_stop:.2f}")
                 return fallback_stop

            latest_atr = atr[-1]

            if latest_atr <= 1e-6: # Check for non-positive or effectively zero ATR
                 logger.warning(f"Invalid ATR calculated ({latest_atr:.4f}) for {symbol}, using fallback stop: {fallback_stop:.2f}")
                 calculated_stop = fallback_stop
            else:
                 stop_distance = self.atr_stop_mult * latest_atr
                 # Ensure stop > 0 after calculation
                 calculated_stop = round(max(0.01, entry_price - stop_distance), 2)
                 logger.debug(f"Calculated ATR stop for {symbol}: {calculated_stop:.2f} (Entry: {entry_price:.2f}, ATR({self.atr_period}): {latest_atr:.3f}, Multiplier: {self.atr_stop_mult})")

            # --- FIX: Modified Sanity Check ---
            # Check if calculated stop is NOT strictly below entry (handles equality case)
            if calculated_stop >= entry_price:
                 logger.error(f"Calculated stop {calculated_stop:.2f} is not strictly below entry price {entry_price:.2f} for {symbol}. Using fallback.")
                 return fallback_stop
            # --- End Fix ---

            return calculated_stop

        except IndexError as ie: # Handles cases where talib output might be shorter than expected
             logger.warning(f"IndexError during ATR calculation for {symbol} (likely insufficient data or talib issue): {ie}. Using fallback stop: {fallback_stop:.2f}")
             return fallback_stop
        except Exception as e:
            logger.exception(f"Error calculating ATR stop in strategy for {symbol}: {e}")
            return fallback_stop # Fallback on any unexpected error

    @abstractmethod
    def calculate_signals(self, event: MarketEvent):
        """Process new market data and generate SignalEvents."""
        raise NotImplementedError("Should implement calculate_signals()")

    def process_market_event(self, event: MarketEvent):
        """Handles incoming market events."""
        if event.symbol in self.symbols:
            self._store_market_data(event)
            # Only calculate signals if it's a BAR event
            if event.data_type == 'BAR':
                 try:
                     self.calculate_signals(event) # Call subclass implementation
                 except Exception as e:
                     logger.exception(f"Error in {self.__class__.__name__}.calculate_signals for {event.symbol}: {e}")