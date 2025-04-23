# backtest/portfolio.py (Revised and Completed)
import pandas as pd
import datetime
from collections import defaultdict
import math # Import math
import queue # Import queue
from typing import Dict, Optional, Any # Import types

# Import core components
from core.events import SignalEvent, FillEvent, OrderEvent # Import necessary events
from core.event_queue import event_queue # Use the shared queue instance
from utils.logger import setup_logger, logger # Use configured logger
from performance.tracker import PerformanceTracker # Import tracker
from pandas.tseries.offsets import BusinessDay # For settlement calculation

# Ensure logger is configured
# logger = setup_logger() # Or get it if already configured

class BacktestPortfolio:
    """
    Simulates portfolio management for backtesting in a cash account context.

    Tracks holdings, cash (total and settled), P&L, and handles T+1 settlement.
    Generates OrderEvents based on SignalEvents and risk parameters.
    Uses PerformanceTracker to record equity and trades.
    """
    def __init__(self, initial_capital: float = 100000.0,
                 settlement_days: int = 1, # T+1 settlement for US equities
                 risk_per_trade: float = 0.01, # Default risk % per trade
                 max_active_positions: int = 10, # Max concurrent positions
                 commission_per_share: float = 0.005, # Needed for cost estimation? No, executor handles.
                 min_commission: float = 1.0): # Needed for cost estimation? No.
        """
        Initializes the BacktestPortfolio.

        Args:
            initial_capital: Starting capital for the backtest.
            settlement_days: Number of business days for trade settlement (T+N).
            risk_per_trade: Fraction of settled cash to risk per trade.
            max_active_positions: Maximum number of concurrent positions allowed.
        """
        if not isinstance(initial_capital, (int, float)) or initial_capital <= 0:
             logger.warning(f"Invalid initial_capital ({initial_capital}). Defaulting to 100000.0.")
             initial_capital = 100000.0
        if not isinstance(settlement_days, int) or settlement_days < 0:
             logger.warning(f"Invalid settlement_days ({settlement_days}). Defaulting to 1.")
             settlement_days = 1
        if not isinstance(risk_per_trade, float) or not (0 < risk_per_trade <= 1):
             logger.warning(f"Invalid risk_per_trade ({risk_per_trade}). Defaulting to 0.01.")
             risk_per_trade = 0.01
        if not isinstance(max_active_positions, int) or max_active_positions <= 0:
             logger.warning(f"Invalid max_active_positions ({max_active_positions}). Defaulting to 10.")
             max_active_positions = 10

        self.initial_capital = float(initial_capital)
        self.total_cash = float(initial_capital) # Includes unsettled cash
        self.settled_cash = float(initial_capital) # Cash available for new trades today
        # Holdings: {symbol: {'quantity': float, 'average_cost': float, 'entry_timestamp': datetime}}
        self.holdings = defaultdict(lambda: {'quantity': 0.0, 'average_cost': 0.0, 'entry_timestamp': None})
        self.positions_value = 0.0 # Current market value of holdings
        # Pending Settlements: {settlement_date (datetime.date): amount_float}
        self.pending_settlements = defaultdict(float)
        self.settlement_days = settlement_days
        self.risk_per_trade = risk_per_trade
        self.max_active_positions = max_active_positions

        self.current_time: Optional[datetime.datetime] = None # Updated by the backtest loop (UTC)
        self.last_market_prices: Dict[str, float] = {} # Cache latest prices {symbol: price}

        # Performance Tracking
        self.performance_tracker = PerformanceTracker(self.initial_capital)
        # Record initial state
        # self.performance_tracker.record_equity(start_time_needs_to_be_passed_or_set_in_update_time, self.initial_capital)

        logger.info(f"BacktestPortfolio initialized. Initial Capital: ${self.initial_capital:,.2f}, "
                    f"Settlement: T+{self.settlement_days}, Risk/Trade: {self.risk_per_trade*100:.2f}%, Max Pos: {self.max_active_positions}")


    def update_time(self, timestamp: datetime.datetime):
        """Updates the current time and processes any cash settlements due."""
        if not isinstance(timestamp, datetime.datetime):
            logger.error(f"Invalid timestamp received in update_time: {timestamp}")
            return
        # Ensure timestamp is timezone-aware UTC
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        else:
            timestamp = timestamp.astimezone(datetime.timezone.utc)

        new_date = timestamp.date()
        # Process settlements only if the date has changed or it's the first update
        if self.current_time is None or new_date > self.current_time.date():
            logger.debug(f"Processing settlements for date: {new_date}")
            settled_today = 0.0
            # Iterate safely over keys that might be removed
            pending_dates = list(self.pending_settlements.keys())
            for settlement_date_key in pending_dates:
                 # Ensure key is date object
                 if isinstance(settlement_date_key, datetime.date):
                     settlement_date_dt = settlement_date_key
                 elif isinstance(settlement_date_key, datetime.datetime): # Handle if datetime object was used as key
                     settlement_date_dt = settlement_date_key.date()
                 else: continue # Skip invalid keys

                 # Settle if the settlement date is today or in the past
                 if settlement_date_dt <= new_date:
                     amount = self.pending_settlements.pop(settlement_date_key)
                     settled_today += amount

            if settled_today > 0:
                self.settled_cash += settled_today
                logger.info(f"[{new_date}] Settled ${settled_today:.2f}. Settled Cash now: ${self.settled_cash:,.2f}")

            # Record initial equity if this is the very first time update
            if not self.performance_tracker._initial_equity_recorded:
                 self.performance_tracker.record_equity(timestamp, self.get_total_value())

        # Always update the current time
        self.current_time = timestamp


    def update_market_price(self, symbol: str, price: Optional[float]):
         """Stores latest market price for sizing and P&L calculations."""
         if price is not None and not math.isnan(price) and price > 0:
             self.last_market_prices[symbol] = price
             # Recalculate portfolio value whenever a holding's price changes
             if symbol in self.holdings and abs(self.holdings[symbol]['quantity']) > 1e-9:
                 self._update_portfolio_value()
         # else: logger.warning(f"Received invalid market price ({price}) for {symbol}")

    def _update_portfolio_value(self):
        """Recalculates the total value of the portfolio (cash + holdings)."""
        current_positions_value = 0.0
        for symbol, details in self.holdings.items():
            qty = details['quantity']
            last_price = self.last_market_prices.get(symbol)
            if abs(qty) > 1e-9 and last_price is not None:
                 current_positions_value += qty * last_price
            elif abs(qty) > 1e-9:
                 logger.warning(f"Cannot calculate value for {symbol} ({qty} shares): Missing market price.")
                 # Should we use average cost as fallback? Might be misleading.
                 # For now, value is based only on available market prices.

        self.positions_value = current_positions_value
        new_total_value = self.total_cash + self.positions_value

        # Record equity change if timestamp is available
        if self.current_time:
             self.performance_tracker.record_equity(self.current_time, new_total_value)
        # else: logger.warning("Cannot record equity update, current_time not set.")


    def process_signal(self, signal: SignalEvent):
        """
        Acts on a signal event:
        1. Checks if trading is allowed (max positions).
        2. Calculates position size based on risk % of *settled cash* and stop distance.
        3. Checks if affordable with *settled cash*.
        4. Generates an OrderEvent and puts it on the main event queue.
        """
        if not isinstance(signal, SignalEvent):
             logger.error(f"Invalid event type passed to process_signal: {type(signal)}")
             return

        symbol = signal.symbol
        direction = signal.direction
        stop_price = signal.stop_price # Stop price from strategy for entry risk calc
        timestamp = signal.timestamp # Use signal's timestamp

        # --- Pre-Trade Checks ---
        # 1. Check Max Positions
        current_pos_count = len([s for s, d in self.holdings.items() if abs(d['quantity']) > 0])
        if direction == 'LONG' and current_pos_count >= self.max_active_positions:
            logger.warning(f"Max active positions ({self.max_active_positions}) reached. Ignoring LONG signal for {symbol}.")
            return

        # 2. Get Current Price (needed for sizing and affordability check)
        current_price = self.last_market_prices.get(symbol)
        if current_price is None:
            logger.error(f"Cannot process signal for {symbol}: Missing current market price.")
            return

        # 3. Calculate Quantity based on Direction
        quantity = 0
        order_direction = None
        order_type = 'MKT' # Default to Market order for backtest simplicity

        if direction == 'LONG':
            # Check if already long
            if self.holdings.get(symbol, {}).get('quantity', 0.0) > 1e-9:
                # logger.debug(f"Already long {symbol}. Ignoring duplicate LONG signal.")
                return

            # Stop Price Validation for sizing
            if stop_price is None or stop_price <= 0 or stop_price >= current_price:
                logger.error(f"Invalid stop price ({stop_price}) provided for risk calculation for LONG {symbol} @ {current_price}. Signal ignored.")
                return
            stop_distance_per_share = current_price - stop_price
            if stop_distance_per_share <= 1e-6: # Avoid division by zero/tiny stops
                logger.error(f"Stop distance is too small ({stop_distance_per_share:.4f}) for {symbol}. Signal ignored.")
                return

            # Settled Cash Check
            if self.settled_cash < 1.0: # Need some cash to trade
                 logger.warning(f"Insufficient settled cash (${self.settled_cash:.2f}) to consider LONG {symbol}.")
                 return

            # Position Sizing (Fixed Fractional Risk of Settled Cash)
            risk_amount = self.settled_cash * self.risk_per_trade
            target_quantity = math.floor(risk_amount / stop_distance_per_share)

            if target_quantity <= 0:
                logger.warning(f"Calculated quantity is zero for {symbol}. Risk: ${risk_amount:.2f}, StopDist: ${stop_distance_per_share:.2f}, SettledCash: ${self.settled_cash:.2f}")
                return

            # Check Affordability using Settled Cash
            estimated_cost = target_quantity * current_price
            # Leave a small buffer (e.g., min $10 or 0.1%)
            required_cash_buffer = max(10.0, self.settled_cash * 0.001)
            if estimated_cost > self.settled_cash - required_cash_buffer:
                logger.warning(f"Insufficient settled cash for {target_quantity} {symbol}. Need ~${estimated_cost:.2f}, Have ${self.settled_cash:.2f}. Reducing size.")
                target_quantity = math.floor((self.settled_cash - required_cash_buffer) / current_price)
                if target_quantity <= 0:
                    logger.warning(f"Cannot afford even 1 share of {symbol} with available settled cash.")
                    return

            quantity = target_quantity
            order_direction = 'BUY'

        elif direction == 'EXIT':
            current_qty = self.holdings.get(symbol, {}).get('quantity', 0.0)
            if current_qty <= 1e-9:
                # logger.debug(f"Not currently long {symbol}. Ignoring EXIT signal.")
                return
            quantity = current_qty # Sell entire position
            order_direction = 'SELL'

        # --- Generate Order Event ---
        if quantity > 0 and order_direction:
            order = OrderEvent(
                timestamp=timestamp, # Use signal timestamp for order event time
                symbol=symbol,
                order_type=order_type,
                direction=order_direction,
                quantity=quantity
            )
            # Stop price isn't needed on the OrderEvent itself if portfolio manages stops,
            # but passing it doesn't hurt if executor might use it (though unlikely in backtest).
            # order.stop_price = stop_price if order_direction == 'BUY' else None

            logger.info(f"Backtest Portfolio generating order: {order}")
            event_queue.put(order) # Put order onto the main queue for the executor
        elif direction not in ['LONG', 'EXIT']:
             logger.warning(f"Unsupported signal direction received in backtest portfolio: {direction}")


    def process_fill(self, fill: FillEvent):
        """Updates holdings and cash based on a simulated fill, handling settlement."""
        if not isinstance(fill, FillEvent):
             logger.error(f"Invalid event type passed to process_fill: {type(fill)}")
             return

        logger.debug(f"Backtest Portfolio processing fill: {fill}")
        symbol = fill.symbol
        qty = fill.quantity # Always positive from FillEvent
        direction = fill.direction
        fill_price = fill.fill_price
        commission = fill.commission
        cost = fill.fill_cost # Always positive from FillEvent

        # Get current state before update
        current_details = self.holdings.get(symbol, {'quantity': 0.0, 'average_cost': 0.0, 'entry_timestamp': None})
        current_qty = current_details['quantity']
        current_avg_cost = current_details['average_cost']
        entry_timestamp = current_details['entry_timestamp']

        signed_qty = qty if direction == 'BUY' else -qty
        new_qty = current_qty + signed_qty

        # --- Safety Check for Sells ---
        if direction == 'SELL':
            if qty > current_qty + 1e-6: # Allow for small float inaccuracies
                logger.error(f"BACKTEST ERROR: Attempted to fill sell of {qty} {symbol}, but portfolio only holds {current_qty}. Fill ignored.")
                # This indicates a logic error upstream (e.g., duplicate EXIT signals or incorrect position tracking)
                return # Do not process this fill

        # --- Calculate Realized PNL (only on sells that reduce/close position) ---
        realized_pnl_trade = 0.0
        holding_period_days = None
        if direction == 'SELL' and current_qty > 1e-9: # If we were long before this sell
            closed_qty = qty # In backtest, assume fill qty matches order qty
            if current_avg_cost is not None and current_avg_cost > 0:
                 realized_pnl_trade = closed_qty * (fill_price - current_avg_cost) - commission
                 # self.realized_pnl_today += realized_pnl_trade # Track daily PNL if needed
                 logger.info(f"Backtest Realized PNL for {symbol} closing {closed_qty} shares: ${realized_pnl_trade:.2f}")
                 if entry_timestamp:
                      holding_period = fill.timestamp - entry_timestamp
                      holding_period_days = holding_period.total_seconds() / (24 * 3600)
                 self.performance_tracker.record_trade(fill, realized_pnl_trade, holding_period_days)
                 # No need to track wash sales explicitly in backtest unless optimizing for taxes
            else:
                 logger.warning(f"Cannot calculate backtest realized PNL for {symbol} sell, average cost is zero/invalid.")
                 self.performance_tracker.record_trade(fill, 0.0, None)

        # --- Update Cash and Settlement ---
        if direction == 'BUY':
            purchase_cost = cost + commission
            # Buying uses cash immediately (both total and settled)
            self.total_cash -= purchase_cost
            self.settled_cash -= purchase_cost
            logger.info(f"[{fill.timestamp.date()}] BACKTEST BOUGHT {qty} {symbol} @ {fill_price:.2f}. Cash: ${self.total_cash:.2f}, Settled: ${self.settled_cash:.2f}")
        elif direction == 'SELL':
            proceeds = cost - commission
            # Selling increases total cash now, but settled cash later
            self.total_cash += proceeds
            # Schedule settlement using BusinessDay offset
            try:
                 fill_ts = pd.Timestamp(fill.timestamp) # Ensure pandas Timestamp
                 settlement_date = fill_ts + BusinessDay(n=self.settlement_days)
                 settlement_date_key = settlement_date.normalize().date() # Use date object as key
                 self.pending_settlements[settlement_date_key] += proceeds
                 logger.info(f"[{fill.timestamp.date()}] BACKTEST SOLD {qty} {symbol} @ {fill_price:.2f}. Cash: ${self.total_cash:.2f}, Settled: ${self.settled_cash:.2f}. Pending settle: ${proceeds:.2f} on {settlement_date_key}")
            except Exception as e:
                 logger.exception(f"Error calculating settlement date for backtest fill {fill}: {e}")

        # --- Update Holdings ---
        if abs(new_qty) < 1e-9: # Position closed
            logger.info(f"Backtest position in {symbol} closed.")
            if symbol in self.holdings: del self.holdings[symbol]
            if symbol in self.last_market_prices: del self.last_market_prices[symbol]
        else: # Update existing or new position
            # Update average cost for BUYS only
            if direction == 'BUY':
                 if abs(current_qty) < 1e-9 or current_avg_cost == 0: # New position
                      self.holdings[symbol]['average_cost'] = fill_price
                 else: # Adding to existing position
                      new_total_cost = (current_qty * current_avg_cost) + cost
                      self.holdings[symbol]['average_cost'] = new_total_cost / new_qty
                 # Record entry timestamp for new positions
                 if abs(current_qty) < 1e-9:
                      self.holdings[symbol]['entry_timestamp'] = fill.timestamp

            # Update quantity regardless of direction
            self.holdings[symbol]['quantity'] = new_qty

        # --- Post-Fill Updates ---
        # Update portfolio value based on the fill price (more accurate than waiting for next market event)
        self.update_market_price(symbol, fill_price) # Update last price cache
        self._update_portfolio_value() # Recalculate total value and record equity

    def get_total_value(self) -> float:
        """Calculates the current total portfolio value (cash + market value of holdings)."""
        current_positions_value = 0.0
        for symbol, details in self.holdings.items():
            qty = details['quantity']
            last_price = self.last_market_prices.get(symbol)
            if abs(qty) > 1e-9 and last_price is not None:
                 current_positions_value += qty * last_price
        return self.total_cash + current_positions_value

    # Methods needed by main_loop for final reporting
    def get_final_equity(self) -> float:
         """Returns the final calculated total portfolio value."""
         # Ensure value is updated one last time
         self._update_portfolio_value()
         return self.get_total_value()

    def get_performance_tracker(self) -> PerformanceTracker:
         """Returns the internal performance tracker instance."""
         return self.performance_tracker