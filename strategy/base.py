# filename: strategy/base.py
# strategy/base.py (Revised for Solution C - Return ATR from _calculate_stop)
import queue
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import talib
import math
import datetime
import logging
# --- MODIFICATION: Added Tuple ---
from typing import Optional, Tuple, Dict

from core.events import MarketEvent, SignalEvent

logger = logging.getLogger("TradingSystem")

class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies. Provides common functionality.
    MODIFIED: _calculate_stop now returns (stop_price, atr_value) tuple.
              Stores latest calculated ATR per symbol.
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
            self.symbol_data[symbol].index = pd.to_datetime(self.symbol_data[symbol].index).tz_localize('UTC')
        self.max_bars = max_bars
        self.positions = {symbol: 0.0 for symbol in self.symbols}
        self.atr_period = atr_period
        self.atr_stop_mult = atr_stop_mult
        self.fallback_stop_pct = fallback_stop_pct
        # --- MODIFICATION: Added storage for latest ATR ---
        self.latest_atr: Dict[str, Optional[float]] = {symbol: None for symbol in self.symbols}
        # --- END MODIFICATION ---
        logger.info(f"BaseStrategy initialized for {len(self.symbols)} symbols. Max Bars: {self.max_bars}, ATR Stop: {self.atr_stop_mult}x{self.atr_period} (Fallback: {self.fallback_stop_pct*100:.1f}%)")

    def update_position(self, symbol: str, quantity: float):
        # ... (method remains the same) ...
        if symbol in self.symbols:
            current_pos = self.positions.get(symbol, 0.0)
            if abs(current_pos - quantity) > 1e-9:
                 logger.info(f"Strategy {self.__class__.__name__} updating position for {symbol}: {current_pos} -> {quantity}")
                 self.positions[symbol] = quantity
        else:
             logger.warning(f"Strategy {self.__class__.__name__} received update for untracked symbol: {symbol}")

    def _store_market_data(self, event: MarketEvent):
        # ... (method remains the same) ...
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
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
        self.symbol_data[symbol] = combined_df.iloc[-self.max_bars:]

    # --- MODIFICATION: Changed return type hint ---
    def _calculate_stop(self, symbol: str, entry_price: float) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculates the stop-loss price based on ATR.
        Uses common parameters defined in the strategy instance.
        Stores the calculated ATR value internally.
        Returns a tuple: (calculated_stop_price, latest_atr_value) or (None, None) on failure.
        """
        # --- MODIFICATION: Initialize latest_atr used in this calculation ---
        latest_atr_value_for_stop: Optional[float] = None
        # --- END MODIFICATION ---

        if not isinstance(entry_price, (int, float)) or entry_price <= 0:
             logger.error(f"Invalid entry price ({entry_price}) for stop calculation on {symbol}.")
             self.latest_atr[symbol] = None # Ensure state is cleared on error
             return None, None # Return tuple

        data_df = self.symbol_data.get(symbol)
        fallback_stop = round(max(0.01, entry_price * (1.0 - self.fallback_stop_pct)), 2)

        if data_df is None or len(data_df) < self.atr_period + 1:
            logger.warning(f"Not enough data ({len(data_df) if data_df is not None else 0} < {self.atr_period+1}) for ATR({self.atr_period}) for {symbol}. Using fallback stop: {fallback_stop:.2f}")
            self.latest_atr[symbol] = None # Ensure state is cleared
            return fallback_stop, None # Return tuple

        try:
            required_len = self.atr_period * 2
            if len(data_df) < required_len:
                 logger.warning(f"Data length ({len(data_df)}) potentially too short for stable ATR({self.atr_period}) for {symbol}. Result might be inaccurate. Using fallback stop: {fallback_stop:.2f}")
                 self.latest_atr[symbol] = None
                 return fallback_stop, None # Return tuple

            highs = data_df['high'].values.astype(float)
            lows = data_df['low'].values.astype(float)
            closes = data_df['close'].values.astype(float)

            if np.isnan(highs[-required_len:]).any() or \
               np.isnan(lows[-required_len:]).any() or \
               np.isnan(closes[-required_len:]).any():
                logger.warning(f"NaN values found in recent data slice needed for ATR({self.atr_period}) for {symbol}. Using fallback stop: {fallback_stop:.2f}")
                self.latest_atr[symbol] = None
                return fallback_stop, None # Return tuple

            atr = talib.ATR(highs, lows, closes, timeperiod=self.atr_period)

            if atr is None or len(atr) == 0 or np.isnan(atr[-1]):
                 logger.warning(f"ATR calculation resulted in NaN or empty series for {symbol}. Using fallback stop: {fallback_stop:.2f}")
                 self.latest_atr[symbol] = None
                 return fallback_stop, None # Return tuple

            # --- MODIFICATION: Store and use latest_atr ---
            latest_atr_value_for_stop = atr[-1]
            self.latest_atr[symbol] = latest_atr_value_for_stop # Update stored value
            # --- END MODIFICATION ---

            if latest_atr_value_for_stop <= 1e-6:
                 logger.warning(f"Invalid ATR calculated ({latest_atr_value_for_stop:.4f}) for {symbol}, using fallback stop: {fallback_stop:.2f}")
                 calculated_stop = fallback_stop
                 # We have a fallback stop, but ATR was invalid, return None for ATR
                 return calculated_stop, None
            else:
                 stop_distance = self.atr_stop_mult * latest_atr_value_for_stop
                 calculated_stop = round(max(0.01, entry_price - stop_distance), 2)
                 logger.debug(f"Calculated ATR stop for {symbol}: {calculated_stop:.2f} (Entry: {entry_price:.2f}, ATR({self.atr_period}): {latest_atr_value_for_stop:.3f}, Multiplier: {self.atr_stop_mult})")

            if calculated_stop >= entry_price:
                 logger.error(f"Calculated stop {calculated_stop:.2f} is not strictly below entry price {entry_price:.2f} for {symbol}. Using fallback.")
                 # Return fallback stop, but ATR value was calculated, so return it? Or None?
                 # Let's return None for ATR if the stop logic based on it was invalid.
                 return fallback_stop, None

            # --- MODIFICATION: Return tuple ---
            return calculated_stop, latest_atr_value_for_stop

        except IndexError as ie:
             logger.warning(f"IndexError during ATR calculation for {symbol} (likely insufficient data or talib issue): {ie}. Using fallback stop: {fallback_stop:.2f}")
             self.latest_atr[symbol] = None
             return fallback_stop, None # Return tuple
        except Exception as e:
            logger.exception(f"Error calculating ATR stop in strategy for {symbol}: {e}")
            self.latest_atr[symbol] = None
            return fallback_stop, None # Return tuple

    # --- MODIFICATION: Add getter method ---
    def get_latest_atr(self, symbol: str) -> Optional[float]:
        """Returns the last calculated ATR value for the symbol."""
        return self.latest_atr.get(symbol, None)
    # --- END MODIFICATION ---

    @abstractmethod
    def calculate_signals(self, event: MarketEvent):
        """Process new market data and generate SignalEvents."""
        raise NotImplementedError("Should implement calculate_signals()")

    def process_market_event(self, event: MarketEvent):
        # ... (method remains the same) ...
        if event.symbol in self.symbols:
            self._store_market_data(event)
            if event.data_type == 'BAR':
                 try:
                     self.calculate_signals(event)
                 except Exception as e:
                     logger.exception(f"Error in {self.__class__.__name__}.calculate_signals for {event.symbol}: {e}")