# filename: portfolio/live_manager.py
# portfolio/live_manager.py (Revised: Use CallbackManager for sync)
import logging
logger = logging.getLogger("TradingSystem")

import datetime
import pandas as pd
from collections import defaultdict
import math
import queue
import threading # For sync events
import time # For wait logic
from typing import Optional, List, Dict, Any

# --- Import from ibapi ---
from ibapi.contract import Contract as IbkrContract
from ibapi.common import TagValueList, BarData
from ibapi.account_summary_tags import AccountSummaryTags # Import std tags

# Import core components and utilities
from core.events import SignalEvent, OrderEvent, FillEvent, MarketEvent, ShutdownEvent, OrderFailedEvent
from performance.tracker import PerformanceTracker
from data.ib_handler import IBDataHandler, create_ibkr_contract
from strategy.base import BaseStrategy
from core.ib_wrapper import IBWrapper

class LivePortfolioManager:
    """
    Manages the live trading portfolio state using data received via IBWrapper callbacks.
    Handles cash, holdings, P&L, risk, settlement, and order generation.
    Uses IBWrapper's CallbackManager for receiving updates.
    """
    def __init__(self, ib_wrapper: IBWrapper, data_handler: IBDataHandler, event_q: queue.Queue,
                 strategies: List[BaseStrategy], config: Dict[str, Any],
                 performance_tracker: PerformanceTracker):
        # Type checking
        if not isinstance(ib_wrapper, IBWrapper): raise TypeError("ib_wrapper must be an instance of IBWrapper")
        if not isinstance(data_handler, IBDataHandler): raise TypeError("data_handler must be an instance of IBDataHandler")
        if not isinstance(event_q, queue.Queue): raise TypeError("event_q must be an instance of queue.Queue")
        if not isinstance(strategies, list) or not all(isinstance(s, BaseStrategy) for s in strategies):
             raise TypeError("strategies must be a list of BaseStrategy instances.")
        if not isinstance(config, dict): raise TypeError("config must be a dictionary.")
        if not isinstance(performance_tracker, PerformanceTracker): raise TypeError("performance_tracker must be a PerformanceTracker instance.")

        self.ib_wrapper = ib_wrapper
        self.data_handler = data_handler
        self.event_queue = event_q
        self.strategies = strategies
        self.config = config
        self.performance_tracker = performance_tracker

        # Config Parameters
        try:
            self.initial_capital_config = float(config['account']['initial_capital'])
            self.risk_per_trade = float(config['trading']['risk_per_trade'])
            self.max_active_positions = int(config['trading']['max_active_positions'])
            self.settlement_days = int(config['account']['settlement_days'])
            self.max_daily_loss_pct = float(config['trading']['max_daily_loss_pct'])
            self.max_drawdown_pct = float(config['trading']['max_drawdown_pct'])
            self.account_id = config['ibkr'].get('account_id')
            if not self.account_id: logger.warning("IBKR account_id not specified in config. Will attempt to use wrapper's discovered account.")
            else: logger.info(f"Using specified IBKR Account ID: {self.account_id}")
        except (KeyError, ValueError, TypeError) as e:
            logger.critical(f"Invalid config for PortfolioManager: {e}")
            raise ValueError(f"Invalid config for PortfolioManager: {e}")

        # State Tracking
        self.current_holdings: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'quantity': 0.0, 'average_cost': 0.0, 'entry_timestamp': None})
        self.required_account_tags = {'NetLiquidation', 'SettledCash'}
        self.account_values: Dict[str, Optional[Any]] = {tag: None for tag in AccountSummaryTags.AllTags.split(',')}
        self.current_cash: Optional[float] = None
        self.settled_cash: Optional[float] = None
        self.portfolio_value: Optional[float] = None

        # P&L and Drawdown Tracking
        self.high_water_mark: Optional[float] = None
        self.start_of_day_value: Optional[float] = None
        self.start_of_day_value_date: Optional[datetime.date] = None
        self.realized_pnl_today: float = 0.0
        self.unrealized_pnl: float = 0.0
        self.last_market_prices: Dict[str, float] = {}

        # Wash Sale Tracking (Basic)
        self.loss_sale_dates: Dict[str, datetime.datetime] = {}
        self.current_time: Optional[datetime.datetime] = None

        # Control Flags & Sync
        self.trading_halted = False
        self._initial_sync_complete = threading.Event() # Signals account value sync
        self._position_sync_complete = threading.Event() # Signals position sync
        self._stopping = False
        self._account_stream_active = False # Track if we successfully requested the stream

        self._setup_wrapper_listeners() # Use listeners instead of direct assignment

    def _setup_wrapper_listeners(self):
         """Registers listeners for relevant IBWrapper callbacks."""
         self.ib_wrapper.register_listener('updateAccountValue', self.onAccountValue)
         self.ib_wrapper.register_listener('updatePortfolio', self.onUpdatePortfolio)
         self.ib_wrapper.register_listener('updateAccountTime', self.onAccountTime)
         self.ib_wrapper.register_listener('accountDownloadEnd', self.onAccountDownloadEnd)
         self.ib_wrapper.register_listener('position', self.onPosition)
         self.ib_wrapper.register_listener('positionEnd', self.onPositionEnd)
         logger.debug("PortfolioManager registered IBWrapper listeners.")

    def start_initial_sync(self):
        """Initiates the request for initial account state (updates and positions)."""
        self._request_initial_state()

    def _request_initial_state(self):
         """Requests initial account updates and positions."""
         if self._stopping or not self.ib_wrapper.isConnected():
             logger.error("PortfolioManager: Cannot request initial state, wrapper not connected.")
             self._initial_sync_complete.set() # Mark as done (failed)
             self._position_sync_complete.set() # Mark as done (failed)
             return

         logger.info("PortfolioManager requesting initial account state...")
         target_account = self.account_id or self.ib_wrapper.accountName
         if not target_account:
             logger.error("Cannot determine target account ID for initial state request.")
             self._initial_sync_complete.set(); self._position_sync_complete.set(); return

         # Request Account Updates Stream
         logger.info(f"Requesting account updates stream for {target_account}")
         try:
             self.ib_wrapper.reqAccountUpdates(True, target_account)
             self._account_stream_active = True
         except Exception as e:
             logger.exception(f"Error requesting account updates for {target_account}: {e}")
             self._initial_sync_complete.set() # Mark as done (failed)

         # Request Initial Positions
         logger.info("Requesting initial positions")
         try:
             self.ib_wrapper.reqPositions()
         except Exception as e:
             logger.exception(f"Error requesting positions: {e}")
             self._position_sync_complete.set() # Mark as done (failed)

         self.request_time_update()


    def wait_for_initial_sync(self, timeout=15.0) -> bool:
         """Blocks until initial sync events (account values AND positions) are set or timeout."""
         logger.info(f"PortfolioManager waiting up to {timeout}s for initial sync (needs NLV, SettledCash, Positions)...")
         overall_deadline = time.monotonic() + timeout
         account_sync_ok = False
         position_sync_ok = False

         account_timeout = max(0, overall_deadline - time.monotonic())
         if self._initial_sync_complete.is_set() or self._initial_sync_complete.wait(timeout=account_timeout):
              logger.info("PortfolioManager initial account sync signal confirmed (NLV & SettledCash).")
              account_sync_ok = True
         else: logger.warning(f"PortfolioManager account sync timed out (NLV/SettledCash not confirmed).")

         if not account_sync_ok: return False

         position_timeout = max(0, overall_deadline - time.monotonic())
         if self._position_sync_complete.is_set() or self._position_sync_complete.wait(timeout=position_timeout):
              logger.info("PortfolioManager initial position sync signal confirmed (PositionEnd).")
              position_sync_ok = True
         else: logger.warning("PortfolioManager position sync timed out.")

         return account_sync_ok and position_sync_ok

    def _check_and_set_sync_complete(self):
         """Checks if critical data (NLV, SettledCash) is present and sets the account sync event."""
         if self._initial_sync_complete.is_set(): return
         nlv_present = self.account_values.get('NetLiquidation') is not None
         sc_present = self.account_values.get('SettledCash') is not None
         if nlv_present and sc_present:
             logger.info("PortfolioManager: Critical account values (NLV & SettledCash) received. Setting initial account sync complete.")
             self._initial_sync_complete.set()

    # --- Listener Methods (Called by IBWrapper's dispatch) ---

    def onAccountValue(self, key: str, val: str, currency: str, accountName: str):
         """Listener for account value updates."""
         target_account = self.account_id or self.ib_wrapper.accountName
         if target_account and accountName != target_account: return
         value = None
         if currency == 'USD':
             try: value = float(val); value = None if math.isnan(value) else value
             except (ValueError, TypeError): value = val if isinstance(val, str) else None
         old_value = self.account_values.get(key)
         value_changed = (value != old_value) if not (isinstance(value, float) and isinstance(old_value, float)) else (abs(old_value - value) > 1e-6)
         if value_changed:
             log_level = logging.DEBUG
             log_val_str = str(value)
             if isinstance(value, float):
                 log_val_str = f"{value:,.2f}"
                 if key in ['NetLiquidation', 'SettledCash', 'TotalCashValue', 'BuyingPower', 'RealizedPnL', 'UnrealizedPnL']:
                      log_level = logging.INFO
             logger.log(log_level, f"Account Value Update ({accountName}): {key} = {log_val_str}")
             self.account_values[key] = value
             if key == 'SettledCash': self.settled_cash = value if isinstance(value, float) else None
             elif key == 'TotalCashValue': self.current_cash = value if isinstance(value, float) else None
             elif key == 'NetLiquidation': self._update_portfolio_value(value if isinstance(value, float) else None)
             if key in self.required_account_tags: self._check_and_set_sync_complete()

    def onUpdatePortfolio(self, contract: IbkrContract, position: float, marketPrice: float,
                        marketValue: float, averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        """Listener for portfolio updates (streaming)."""
        target_account = self.account_id or self.ib_wrapper.accountName
        if target_account and accountName != target_account: return
        symbol = contract.localSymbol or contract.symbol
        if not symbol or contract.secType not in ['STK', 'CASH', 'FUT', 'OPT']: return
        pos_f = position if not math.isnan(position) else 0.0
        mkt_price_f = marketPrice if not math.isnan(marketPrice) else None
        avg_cost_f = averageCost if not math.isnan(averageCost) else None
        internal_details = self.current_holdings.get(symbol, {})
        internal_qty = internal_details.get('quantity', 0.0)
        needs_update = False
        if abs(internal_qty - pos_f) > 1e-6:
            logger.info(f"Portfolio Update ({accountName}): {symbol} Qty {internal_qty:.2f} -> {pos_f:.2f}")
            self.current_holdings[symbol]['quantity'] = pos_f
            for strat in self.strategies:
                if symbol in strat.symbols: strat.update_position(symbol, pos_f)
            needs_update = True
            if abs(internal_qty) < 1e-9 and abs(pos_f) > 1e-9: self.current_holdings[symbol]['entry_timestamp'] = self.current_time or datetime.datetime.now(datetime.timezone.utc)
        if avg_cost_f is not None and abs(pos_f) > 1e-9:
            internal_avg_cost = internal_details.get('average_cost', 0.0)
            if abs(internal_avg_cost - avg_cost_f) > 1e-6:
                logger.info(f"Portfolio Update ({accountName}): {symbol} AvgCost {internal_avg_cost:.2f} -> {avg_cost_f:.2f}")
                self.current_holdings[symbol]['average_cost'] = avg_cost_f
                needs_update = True
        if abs(pos_f) < 1e-9 and symbol in self.current_holdings:
            if abs(internal_qty) > 1e-9: logger.info(f"Reconciling (UpdatePortfolio): Removing zero pos {symbol}")
            del self.current_holdings[symbol]
            self.last_market_prices.pop(symbol, None)
            needs_update = True
        if mkt_price_f is not None and mkt_price_f > 0:
             if abs(self.last_market_prices.get(symbol, 0.0) - mkt_price_f) > 1e-6:
                 self.last_market_prices[symbol] = mkt_price_f
                 needs_update = True
        if needs_update: self._update_unrealized_pnl()

    def onAccountTime(self, timeStamp: str):
         """Listener for account update timestamp."""
         logger.debug(f"Listener: UpdateAccountTime: {timeStamp}")

    def onAccountDownloadEnd(self, accountName: str):
        """Listener indicating end of initial account state snapshot."""
        target_account = self.account_id or self.ib_wrapper.accountName
        if accountName == target_account:
             logger.info(f"Listener: AccountDownloadEnd received for Acct={accountName}.")

    def onPosition(self, account: str, contract: IbkrContract, position: float, avgCost: float):
         """Listener for position details (from reqPositions)."""
         target_account = self.account_id or self.ib_wrapper.accountName
         if account != target_account: return
         symbol = contract.localSymbol or contract.symbol
         if not symbol: return
         pos_f = position if not math.isnan(position) else 0.0
         avg_cost_f = avgCost if not math.isnan(avgCost) else 0.0
         internal_details = self.current_holdings.get(symbol, {})
         internal_qty = internal_details.get('quantity', 0.0)
         internal_avg_cost = internal_details.get('average_cost', 0.0)
         qty_changed = abs(internal_qty - pos_f) > 1e-6
         cost_changed = abs(internal_avg_cost - avg_cost_f) > 1e-6 if abs(pos_f) > 1e-9 else False
         if qty_changed or cost_changed:
             logger.info(f"Reconciling from onPosition for {symbol}: Qty {internal_qty:.2f}->{pos_f:.2f}, AvgCost {internal_avg_cost:.2f}->{avg_cost_f:.2f}")
             self.current_holdings[symbol]['quantity'] = pos_f
             self.current_holdings[symbol]['average_cost'] = avg_cost_f if abs(pos_f) > 1e-9 else 0.0
             if abs(internal_qty) < 1e-9 and abs(pos_f) > 1e-9: self.current_holdings[symbol]['entry_timestamp'] = self.current_time or datetime.datetime.now(datetime.timezone.utc)
             elif abs(pos_f) < 1e-9: self.current_holdings[symbol]['entry_timestamp'] = None
             for strat in self.strategies:
                 if symbol in strat.symbols: strat.update_position(symbol, pos_f)
             self._update_unrealized_pnl()
         elif abs(pos_f) < 1e-9 and symbol in self.current_holdings:
             logger.info(f"Reconciling: Removing internal pos {symbol} based on zero qty from onPosition.")
             del self.current_holdings[symbol]
             self.last_market_prices.pop(symbol, None)
             self._update_unrealized_pnl()

    def onPositionEnd(self):
         """Listener indicating end of position transmission."""
         logger.info("Listener: PositionEnd received. Signalling position sync complete.")
         self._position_sync_complete.set()

    # --- End Listener Methods ---

    # --- Internal Logic Methods (Remain largely the same) ---
    def _update_portfolio_value(self, new_nlv: Optional[float]):
        """Internal method to update portfolio value (NLV) and dependent metrics."""
        old_nlv_log_str = 'N/A' if self.portfolio_value is None else f"${self.portfolio_value:,.2f}"
        if new_nlv is None or math.isnan(new_nlv) or new_nlv <= 0:
              if self.portfolio_value is not None: logger.warning(f"NetLiquidation invalid ({new_nlv}). Keeping last: {old_nlv_log_str}")
              return
        if self.portfolio_value is None or abs(self.portfolio_value - new_nlv) > 1e-6:
               self.portfolio_value = new_nlv
               if self.high_water_mark is None or new_nlv > self.high_water_mark: self.high_water_mark = new_nlv
               record_time = self.current_time or datetime.datetime.now(datetime.timezone.utc)
               self.performance_tracker.record_equity(record_time, self.portfolio_value)

    def request_time_update(self):
        """Requests the current time from TWS."""
        if self.ib_wrapper.isConnected():
            try: self.ib_wrapper.reqCurrentTime()
            except Exception as e: logger.error(f"Error requesting current time: {e}")

    def update_market_data(self, market_event: MarketEvent):
        """Processes market data (BAR) to update last known prices and UPNL."""
        if self._stopping or not isinstance(market_event, MarketEvent) or market_event.data_type != 'BAR': return
        symbol = market_event.symbol
        new_price = market_event.data.get('close')
        if new_price is not None and not math.isnan(new_price) and new_price > 0:
            if abs(self.last_market_prices.get(symbol, 0.0) - new_price) > 1e-6:
                 self.last_market_prices[symbol] = new_price
                 if symbol in self.current_holdings and abs(self.current_holdings[symbol].get('quantity', 0.0)) > 1e-9:
                     self._update_unrealized_pnl()

    def _update_unrealized_pnl(self):
        """Recalculates the total current unrealized P&L based on holdings and last market prices."""
        current_unrealized = 0.0
        symbols_held = list(self.current_holdings.keys())
        for symbol in symbols_held:
             details = self.current_holdings.get(symbol)
             if not details: continue
             qty, avg_cost = details.get('quantity', 0.0), details.get('average_cost', 0.0)
             last_price = self.last_market_prices.get(symbol)
             if abs(qty) > 1e-9 and avg_cost is not None and last_price is not None and last_price > 0:
                 if avg_cost > 0: current_unrealized += qty * (last_price - avg_cost)
                 else: logger.warning(f"Cannot calculate UPNL for {symbol}: Average cost is {avg_cost}")
        old_upnl = self.unrealized_pnl
        if abs(old_upnl - current_unrealized) > 1e-2:
             self.unrealized_pnl = current_unrealized
             # logger.debug(f"Unrealized PNL updated: ${old_upnl:.2f} -> ${self.unrealized_pnl:.2f}")

    def update_time(self, timestamp: datetime.datetime):
        """Updates the current time (called from main loop) and handles daily state reset."""
        if not isinstance(timestamp, datetime.datetime): return
        if timestamp.tzinfo is None: timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        else: timestamp = timestamp.astimezone(datetime.timezone.utc)
        new_date = timestamp.date()
        if self.current_time is None or new_date > self.current_time.date():
             self.update_daily_state(timestamp)
        self.current_time = timestamp

    def update_daily_state(self, timestamp: datetime.datetime):
        """Resets daily counters and sets start-of-day values."""
        current_date = timestamp.date()
        if self.start_of_day_value_date != current_date:
             if self.portfolio_value is not None:
                  self.start_of_day_value = self.portfolio_value
                  self.start_of_day_value_date = current_date
                  self.high_water_mark = max(self.high_water_mark or -math.inf, self.start_of_day_value)
                  self.realized_pnl_today = 0.0
                  logger.info(f"Daily state {current_date}: Start Value: ${self.start_of_day_value:,.2f}, HWM: ${self.high_water_mark:,.2f}")
             else:
                  logger.warning(f"Cannot update daily state for {current_date}, portfolio value unknown.")
                  self.start_of_day_value, self.start_of_day_value_date = None, current_date
                  self.realized_pnl_today = 0.0

    def process_signal_event(self, signal: SignalEvent):
        """Generates OrderEvent based on SignalEvent, considering risk and available cash."""
        if self._stopping or self.trading_halted: logger.debug(f"Halted/Stopping. Ignoring signal: {signal}"); return
        if not isinstance(signal, SignalEvent): logger.error(f"Invalid event type: {type(signal)}"); return

        symbol, direction, timestamp = signal.symbol, signal.direction, signal.timestamp
        available_settled_cash = self.settled_cash
        if available_settled_cash is None: logger.error(f"Settled cash unknown. Cannot size order for {signal}."); return
        if available_settled_cash < 1.0: logger.warning(f"Insufficient settled cash (${available_settled_cash:.2f}) for {signal}"); return

        current_pos_count = len([s for s, d in self.current_holdings.items() if abs(d.get('quantity', 0.0)) > 1e-9])
        if direction == 'LONG' and current_pos_count >= self.max_active_positions: logger.warning(f"Max positions ({self.max_active_positions}) reached. Ignoring LONG {symbol}."); return

        current_price = self.data_handler.get_latest_price(symbol) or self.last_market_prices.get(symbol)
        if current_price is None: logger.error(f"No price for {symbol}, cannot process signal: {signal}"); return

        quantity, order_direction, order_type, stop_price = 0, None, 'MKT', signal.stop_price

        if direction == 'LONG':
            if self.current_holdings.get(symbol, {}).get('quantity', 0.0) > 1e-9: logger.debug(f"Already long {symbol}. Ignoring LONG signal."); return
            if symbol in self.loss_sale_dates and (timestamp - self.loss_sale_dates[symbol]).days <= 30: logger.warning(f"Potential Wash Sale: Buying {symbol} within 30 days of loss.")
            if stop_price is None or stop_price <= 0: logger.error(f"Invalid stop price ({stop_price}) for LONG {symbol}. Signal ignored."); return
            if stop_price >= current_price: logger.error(f"Stop ({stop_price:.2f}) not below price ({current_price:.2f}) for LONG {symbol}. Signal ignored."); return
            stop_distance = current_price - stop_price
            if stop_distance <= 1e-6: logger.error(f"Stop distance too small ({stop_distance:.4f}) for {symbol}. Signal ignored."); return
            risk_amount = available_settled_cash * self.risk_per_trade
            target_quantity = math.floor(risk_amount / stop_distance)
            if target_quantity <= 0: logger.warning(f"Calculated qty zero for {symbol}. Risk: ${risk_amount:.2f}, StopDist: ${stop_distance:.2f}"); return
            estimated_cost = target_quantity * current_price
            required_cash_buffer = max(10.0, available_settled_cash * 0.001)
            if estimated_cost > (available_settled_cash - required_cash_buffer):
                 new_target_quantity = math.floor((available_settled_cash - required_cash_buffer) / current_price)
                 logger.warning(f"Reducing {symbol} LONG size {target_quantity}->{new_target_quantity} due to settled cash (${available_settled_cash:.2f}).")
                 target_quantity = new_target_quantity
                 if target_quantity <= 0: logger.warning(f"Cannot afford 1 share of {symbol} with settled cash (${available_settled_cash:.2f})."); return
            quantity, order_direction = target_quantity, 'BUY'
        elif direction == 'EXIT':
            current_qty = self.current_holdings.get(symbol, {}).get('quantity', 0.0)
            if current_qty <= 1e-9: logger.debug(f"Not long {symbol}. Ignoring EXIT signal."); return
            quantity, order_direction, stop_price = abs(current_qty), 'SELL', None
        else: logger.warning(f"Unsupported signal direction '{direction}' for {symbol}."); return

        if quantity > 0 and order_direction:
            order = OrderEvent(timestamp, symbol, order_type, order_direction, quantity, stop_price=(stop_price if order_direction == 'BUY' else None))
            logger.info(f"Portfolio Manager generating order: {order}")
            try: self.event_queue.put_nowait(order)
            except queue.Full: logger.error("Event queue full, cannot queue OrderEvent.")

    def process_fill_event(self, fill: FillEvent):
        """Updates internal portfolio state based on a FillEvent from the executor."""
        if self._stopping or not isinstance(fill, FillEvent): return
        logger.info(f"Portfolio processing LIVE fill: {fill}")
        symbol, qty, direction, fill_price, commission, cost = fill.symbol, fill.quantity, fill.direction, fill.fill_price, fill.commission, fill.fill_cost
        current_details = self.current_holdings.get(symbol, {})
        current_qty_before_fill, current_avg_cost, entry_timestamp = current_details.get('quantity', 0.0), current_details.get('average_cost', 0.0), current_details.get('entry_timestamp')
        realized_pnl_trade, holding_period_days = 0.0, None
        if direction == 'SELL' and current_qty_before_fill > 1e-9:
            closed_qty = min(qty, current_qty_before_fill)
            if current_avg_cost is not None and current_avg_cost > 0:
                 realized_pnl_trade = closed_qty * (fill_price - current_avg_cost) - commission
                 self.realized_pnl_today += realized_pnl_trade
                 logger.info(f"Realized PNL {symbol} ({closed_qty:.2f} sh): ${realized_pnl_trade:.2f} (Daily: ${self.realized_pnl_today:.2f})")
                 if entry_timestamp: holding_period_days = round((fill.timestamp - entry_timestamp).total_seconds() / (24 * 3600), 2)
                 self.performance_tracker.record_trade(fill, realized_pnl_trade, holding_period_days)
                 if realized_pnl_trade < 0: self.loss_sale_dates[symbol] = fill.timestamp; logger.debug(f"Recorded loss sale date {symbol}: {fill.timestamp.date()}")
            else: logger.warning(f"Cannot calc realized PNL for {symbol} sell: avg cost {current_avg_cost}."); self.performance_tracker.record_trade(fill, 0.0, None)
        signed_qty = qty if direction == 'BUY' else -qty
        new_qty = current_qty_before_fill + signed_qty
        if direction == 'SELL' and qty > current_qty_before_fill + 1e-6: logger.error(f"CRITICAL FILL MISMATCH: Sell {qty} {symbol}, Hold {current_qty_before_fill:.4f}. State inconsistent."); new_qty = 0
        if abs(new_qty) < 1e-9:
            logger.info(f"Internal state update: Position {symbol} closed.")
            if symbol in self.current_holdings: del self.current_holdings[symbol]
            self.last_market_prices.pop(symbol, None)
        else:
            if direction == 'BUY':
                 new_avg_cost = fill_price if abs(current_qty_before_fill) < 1e-9 or current_avg_cost == 0 else ((current_qty_before_fill * current_avg_cost) + cost) / new_qty if new_qty != 0 else 0
                 self.current_holdings[symbol]['average_cost'] = new_avg_cost
                 if abs(current_qty_before_fill) < 1e-9: self.current_holdings[symbol]['entry_timestamp'] = fill.timestamp
            self.current_holdings[symbol]['quantity'] = new_qty
        for strat in self.strategies:
            if symbol in strat.symbols: strat.update_position(symbol, new_qty)
        self.last_market_prices[symbol] = fill_price
        self._update_unrealized_pnl()

    def process_order_failed_event(self, event: OrderFailedEvent):
        """Handles notifications that an order failed."""
        logger.error(f"Received Order Failed Event: {event}")

    def check_risk_limits(self) -> bool:
        """Checks portfolio risk limits (daily loss, max drawdown). Returns True if halted."""
        if self._stopping or self.trading_halted: return self.trading_halted
        shutdown_triggered, reason = False, ""
        current_val, start_val, hwm = self.portfolio_value, self.start_of_day_value, self.high_water_mark
        if current_val is None or current_val <= 0: return False # Cannot check if NLV unknown
        if start_val is not None and start_val > 0:
            daily_pnl = current_val - start_val
            daily_loss_limit_abs = start_val * self.max_daily_loss_pct
            if daily_pnl < -daily_loss_limit_abs: reason, shutdown_triggered = f"Daily loss limit breached (${daily_pnl:.2f} < -${daily_loss_limit_abs:.2f})", True
        if not shutdown_triggered and hwm is not None and hwm > 1e-9:
            drawdown_pct = (hwm - current_val) / hwm
            if drawdown_pct > self.max_drawdown_pct: reason, shutdown_triggered = f"Max drawdown limit breached ({drawdown_pct*100:.2f}% > {self.max_drawdown_pct*100:.1f}%)", True
        if shutdown_triggered:
            self.trading_halted = True
            logger.critical(f"RISK LIMIT BREACHED: {reason}. Halting trading.")
            self.trigger_portfolio_shutdown(reason)
            return True
        return False

    def trigger_portfolio_shutdown(self, reason: str):
         """Initiates system shutdown by putting ShutdownEvent on queue."""
         if not self._stopping:
             logger.warning(f"Portfolio triggering shutdown: {reason}")
             self._stopping, self.trading_halted = True, True
             try: self.event_queue.put_nowait(ShutdownEvent(reason))
             except queue.Full: logger.error("Event queue full while triggering portfolio shutdown.")

    def shutdown(self):
        """Cleans up portfolio manager resources."""
        logger.info("Shutting down Portfolio Manager...")
        self._stopping = True
        # Unregister listeners
        self.ib_wrapper.unregister_listener('updateAccountValue', self.onAccountValue)
        self.ib_wrapper.unregister_listener('updatePortfolio', self.onUpdatePortfolio)
        self.ib_wrapper.unregister_listener('updateAccountTime', self.onAccountTime)
        self.ib_wrapper.unregister_listener('accountDownloadEnd', self.onAccountDownloadEnd)
        self.ib_wrapper.unregister_listener('position', self.onPosition)
        self.ib_wrapper.unregister_listener('positionEnd', self.onPositionEnd)
        # Cancel stream
        if self.ib_wrapper.isConnected() and self._account_stream_active:
             target_account = self.account_id or self.ib_wrapper.accountName
             if target_account:
                 try:
                     logger.info(f"Cancelling account updates stream for {target_account}...")
                     self.ib_wrapper.reqAccountUpdates(False, target_account)
                     self._account_stream_active = False
                 except Exception as e: logger.error(f"Error cancelling account updates for {target_account}: {e}")
             else: logger.warning("Cannot cancel account stream, account ID unknown.")
        else: logger.info("Not connected or stream inactive, skipping cancellation.")
        final_nlv, final_cash = self.portfolio_value, self.settled_cash
        logger.info(f"Final Portfolio Value (NLV): {'N/A' if final_nlv is None else f'${final_nlv:,.2f}'}")
        logger.info(f"Final Settled Cash (IB): {'N/A' if final_cash is None else f'${final_cash:,.2f}'}")
        logger.info("Portfolio Manager shutdown complete.")