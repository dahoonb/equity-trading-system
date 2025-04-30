# filename: portfolio/live_manager.py
# portfolio/live_manager.py (Revised: Use CallbackManager for sync)
import logging
logger = logging.getLogger("TradingSystem")

import datetime
import pandas as pd
from pandas.tseries.offsets import BusinessDay
from collections import defaultdict, deque
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
from core.events import SignalEvent, OrderEvent, FillEvent, MarketEvent, ShutdownEvent, OrderFailedEvent, InternalFillProcessedEvent
from performance.tracker import PerformanceTracker
from data.ib_handler import IBDataHandler, create_ibkr_contract
from strategy.base import BaseStrategy
from core.ib_wrapper import IBWrapper
from strategy.base import BaseStrategy # Ensure BaseStrategy is imported if needed for get_latest_atr
from config_loader import load_config

class LivePortfolioManager:
    """
    Manages the live trading portfolio state using data received via IBWrapper callbacks.
    Handles cash, holdings, P&L, risk, settlement, and order generation.
    Uses IBWrapper's CallbackManager for receiving updates.
    """
    def __init__(self, ib_wrapper: IBWrapper, data_handler: IBDataHandler, event_q: queue.Queue,
                 strategies: List[BaseStrategy], config: Dict[str, Any],
                 performance_tracker: PerformanceTracker):
        """
        Constructs a LivePortfolioManager that tracks live IBKR account state
        and feeds orders back into the execution layer.
        """
        # --- type‑checking guards --------------------------------------------
        if not isinstance(ib_wrapper, IBWrapper):
            raise TypeError("ib_wrapper must be an instance of IBWrapper")
        if not isinstance(data_handler, IBDataHandler):
            raise TypeError("data_handler must be an instance of IBDataHandler")
        if not isinstance(event_q, queue.Queue):
            raise TypeError("event_q must be an instance of queue.Queue")
        if not isinstance(strategies, list) or not all(isinstance(s, BaseStrategy) for s in strategies):
            raise TypeError("strategies must be a list[BaseStrategy]")
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        if not isinstance(performance_tracker, PerformanceTracker):
            raise TypeError("performance_tracker must be a PerformanceTracker")

        # wiring
        self.ib_wrapper          = ib_wrapper
        self.data_handler        = data_handler
        self.event_queue         = event_q
        self.strategies          = strategies
        self.config              = config
        self.performance_tracker = performance_tracker

        # ------------------------------------------------------------------ #
        # config parameters                                                  #
        # ------------------------------------------------------------------ #
        try:
            self.initial_capital_config = float(config['account']['initial_capital'])
            self.risk_per_trade         = float(config['trading']['risk_per_trade'])
            self.max_active_positions   = int(config['trading']['max_active_positions'])
            self.settlement_days        = int(config['account']['settlement_days'])
            self.max_daily_loss_pct     = float(config['trading']['max_daily_loss_pct'])
            self.max_drawdown_pct       = float(config['trading']['max_drawdown_pct'])
            self.account_id             = config['ibkr'].get('account_id')
            if not self.account_id:
                logger.warning("IBKR account_id not specified; will use first account received from IB.")
            else:
                logger.info("Using specified IBKR Account ID: %s", self.account_id)            
            # --- ADDED: Load Leverage and Sector Limits ---
            trading_config = config.get('trading', {})
            self.max_gross_exposure_ratio = trading_config.get('max_gross_exposure_ratio', 2.0) # Default 2x
            self.max_sector_allocation_pct = trading_config.get('max_sector_allocation_pct', 0.30) # Default 30%
            self.symbol_sector_map = config.get('sector_map', {}) # Load sector mapping
            logger.info(f"Max Gross Exposure Ratio Limit: {self.max_gross_exposure_ratio:.2f}")
            logger.info(f"Max Sector Allocation Limit: {self.max_sector_allocation_pct:.2%}")
            if not self.symbol_sector_map:
                 logger.warning("Sector map not found or empty in config. Sector limits will not be enforced.")
            # --- END ADDED ---
            # --- MODIFICATION: Load Volatility Monitoring Config ---
            self.volatility_config = config.get('volatility_monitoring', {})
            self.volatility_monitoring_enabled = self.volatility_config.get('enabled', False)
            self.volatility_symbol = self.volatility_config.get('symbol')
            self.vix_threshold_high = self.volatility_config.get('threshold_high', 30.0)
            self.vix_threshold_critical = self.volatility_config.get('threshold_critical') # Optional
            self.pause_entries_on_high_vix = self.volatility_config.get('pause_entries_on_high', True)
            self.latest_vix_value: Optional[float] = None
            self.is_high_volatility_regime = False # Internal state flag

            if self.volatility_monitoring_enabled and not self.volatility_symbol:
                 logger.error("Volatility monitoring enabled in config, but 'symbol' is missing.")
                 self.volatility_monitoring_enabled = False # Disable if misconfigured
            elif self.volatility_monitoring_enabled:
                 logger.info(f"Volatility Monitoring Enabled: Symbol={self.volatility_symbol}, HighThreshold={self.vix_threshold_high}, PauseEntries={self.pause_entries_on_high_vix}")
            self.base_risk_per_trade = float(config['trading']['risk_per_trade'])
            self.current_risk_per_trade = self.base_risk_per_trade # Initialize
            self.high_vol_risk_multiplier = self.volatility_config.get('high_vol_risk_multiplier', 0.5) # e.g., reduce risk by 50%
            # --- END MODIFICATION ---
            # --- MODIFICATION: Load rapid loss config ---
            monitor_config = config.get('monitoring', {})
            self.rapid_loss_window_sec = monitor_config.get('rapid_loss_check_window_sec', 60)
            self.rapid_loss_threshold_pct = monitor_config.get('rapid_loss_threshold_pct', 0.01)
            logger.info(f"Rapid Loss Check Config: Window={self.rapid_loss_window_sec}s, Threshold={self.rapid_loss_threshold_pct*100:.2f}%")
            # --- END MODIFICATION ---
                    
        except (KeyError, ValueError, TypeError) as exc:
            logger.critical("Invalid PortfolioManager config: %s", exc)
            raise ValueError(f"Invalid PortfolioManager config: {exc}") from exc

        # ------------------------------------------------------------------ #
        # live state                                                         #
        # ------------------------------------------------------------------ #
        # MODIFICATION: Add fields for trailing stop calculation to holdings
        self.current_holdings: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "quantity": 0.0,
                "average_cost": 0.0,
                "entry_timestamp": None,
                "entry_atr": None,          # <-- ADDED: ATR at time of entry
                "initial_stop_price": None  # <-- ADDED: Stop price used at entry
            }
        )

        # critical tags required *before* trading may start
        self.required_account_tags: set[str] = {
            "NetLiquidation", "SettledCash", "TotalCashValue",
            "BuyingPower", "GrossPositionValue" # <-- ADDED for exposure/leverage checks
        }

        # normalise IB variants -> canonical tag
        self._tag_aliases: Dict[str, str] = {
            "SettledCash": "SettledCash",
            "SettledCash-S": "SettledCash",
            "SettledCash-C": "SettledCash",
            "SettledCash-P": "SettledCash",
            "TotalCashValue": "TotalCashValue",
            "TotalCashValue-S": "TotalCashValue",
            "TotalCashValue-C": "TotalCashValue",
            "TotalCashValue-P": "TotalCashValue",
            "GrossPositionValue": "GrossPositionValue", # <-- ADDED alias
            "GrossPositionValue-S": "GrossPositionValue",
        }

        self.account_values: Dict[str, Any] = {tag: None for tag in self.required_account_tags}

        self.current_cash:    Optional[float] = None   # from TotalCashValue
        self.settled_cash:    Optional[float] = None   # from SettledCash
        self.portfolio_value: Optional[float] = None   # from NetLiquidation
        # Stores {settlement_date (datetime.date): amount_float}
        self.pending_settlements = defaultdict(float)
        # Internally tracked settled cash, initialized from IBKR during sync
        self.settled_cash_internal: Optional[float] = None
        self._settled_cash_initialized_from_ib = False # Flag for initial seeding

        # P&L / drawdown helpers
        self.high_water_mark:        Optional[float] = None
        self.start_of_day_value:     Optional[float] = None
        self.start_of_day_value_date: Optional[datetime.date] = None
        self.realized_pnl_today:     float = 0.0
        self.unrealized_pnl:         float = 0.0
        self.last_market_prices:     Dict[str, float] = {}
        
        # --- MODIFICATION: Add portfolio value history ---
        self.portfolio_value_history = deque() # Stores (timestamp, value) tuples
        # --- END MODIFICATION ---

        # wash‑sale
        self.loss_sale_dates: Dict[str, datetime.datetime] = {}

        self.current_time: Optional[datetime.datetime] = None  # updated by market data

        # ------------------------------------------------------------------ #
        # synchronisation flags                                              #
        # ------------------------------------------------------------------ #
        self.trading_halted          = False
        # --- MODIFICATION: Add specific pause flag ---
        self.new_entries_paused = False # Specific flag for volatility pause etc.
        # --- END MODIFICATION ---
        self._initial_sync_complete  = threading.Event()   # waits for cash+NLV
        self._position_sync_complete = threading.Event()   # waits for positionEnd
        self._stopping               = False
        self._account_stream_active  = False

        # register callbacks with IB wrapper
        self._setup_wrapper_listeners()

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

        # Check for required values in the self.account_values dictionary, 
        # ensuring they have been processed by onAccountValue first.
        nlv = self.account_values.get('NetLiquidation')
        settled_cash_val = self.account_values.get('SettledCash') 
        total_cash_val = self.account_values.get('TotalCashValue')

        nlv_present = nlv is not None and isinstance(nlv, (float, int)) and nlv >= 0 # Ensure valid NLV

        # Determine if a valid cash value is available from the processed updates
        usable_cash_val = None
        cash_source = None
        # Prioritize SettledCash if it's valid
        if settled_cash_val is not None and isinstance(settled_cash_val, (float, int)):
            usable_cash_val = settled_cash_val
            cash_source = "SettledCash"
        # Use TotalCashValue only as a fallback if SettledCash is specifically missing/None
        elif settled_cash_val is None and total_cash_val is not None and isinstance(total_cash_val, (float, int)):
            usable_cash_val = total_cash_val
            cash_source = "TotalCashValue (proxy)"
        
        cash_present = usable_cash_val is not None

        logger.debug(f"Sync check: NLV present: {nlv_present}, Cash present: {cash_present} (Source: {cash_source})")

        # --- REQUIRE BOTH NLV AND a Usable Cash Value ---
        if nlv_present and cash_present:
            logger.info(f"PortfolioManager: Critical values received (NLV & {cash_source}). Setting initial account sync complete.")

            # --- Initialize Internal Settled Cash (only once) ---
            if not self._settled_cash_initialized_from_ib:
                self.settled_cash_internal = usable_cash_val # Use the validated cash value
                self._settled_cash_initialized_from_ib = True
                # Update the direct attribute self.settled_cash for consistency if not already set
                if self.settled_cash is None:
                     self.settled_cash = usable_cash_val
                logger.info(f"Initialized internal settled cash from IBKR (using {cash_source}): ${self.settled_cash_internal:,.2f}")
            # elif self._settled_cash_initialized_from_ib: # Optional: Log if already initialized
            #    logger.debug("Internal settled cash already initialized on subsequent check.")
            
            # --- Set the event ONLY when all conditions met ---
            self._initial_sync_complete.set()
            
        # Optional: Log detailed status if still waiting
        # else:
        #    if not self._stopping: # Avoid logging during shutdown
        #        missing = []
        #        if not nlv_present: missing.append("NetLiquidation")
        #        if not cash_present: missing.append("SettledCash/TotalCashValue")
        #        if missing: logger.debug(f"Sync check: Still waiting for: {', '.join(missing)}")

    # --- Listener Methods (Called by IBWrapper's dispatch) ---

    # ------------------------------------------------------------------ #
    # IB callback – account‑value updates                                #
    # ------------------------------------------------------------------ #
    def onAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """Normalises and stores IB account‑value updates."""
        logger.debug(f"DEBUG: onAccountValue RAW ENTRY: key={repr(key)}, val={repr(val)}, currency={repr(currency)}, accountName={repr(accountName)}")
        target = self.account_id or getattr(self.ib_wrapper, "accountName", None)
        # --- Add .strip() to accountName and target ---
        if target and accountName.strip() != target.strip():
            # Optional: Add a debug log here if you want to see skipped accounts
            logger.debug(f"DEBUG: Skipping account update for '{accountName}' (target: '{target}')")
            return # Exit if account doesn't match after stripping whitespace

        canonical = self._tag_aliases.get(key, key)
        # --- ADD THIS LOG ---
        if key == "SettledCash-S":
            logger.debug(f"DEBUG Alias Result: key='{key}', canonical='{canonical}'")
        # --- END LOG ---
        logger.debug(f"DEBUG Entry: key='{key}', canonical='{canonical}', val='{val}'")

        # Ignore non‑USD currencies for now
        ## if currency not in ("USD", "", None):
        ##     return

        # try numeric
        try:
            value: Any = float(val)
            if math.isnan(value):
                value = None
        except (ValueError, TypeError):
            value = val  # keep raw string if not numeric

        # only proceed if changed
        old = self.account_values.get(canonical)
        changed = value != old if not (isinstance(value, float) and isinstance(old, float)) else abs(value - old) > 1e-6
        
        logger.debug(f"DEBUG CheckChange: key='{key}', canonical='{canonical}', old='{old}' (Type: {type(old)}), new='{value}' (Type: {type(value)}), changed={changed}")
        
        if not changed:
            logger.debug(f"DEBUG CheckChange: Returning early because value did not change for {canonical}.")
            return

        # log: high‑value tags at INFO, others DEBUG
        level = logging.INFO if canonical in (
            "NetLiquidation", "SettledCash", "TotalCashValue",
            "BuyingPower", "RealizedPnL", "UnrealizedPnL",
            'GrossPositionValue'
        ) else logging.DEBUG
        logger.log(level, "Account Value Update (%s): %s = %s",
                   accountName, canonical,
                   f"{value:,.2f}" if isinstance(value, float) else value)

        # Add this debug block:
        if canonical == "SettledCash":
            logger.debug(f"DEBUG: Processing canonical='{canonical}'. Current dict value: {self.account_values.get(canonical)}. New float value: {value}")

        # store
        self.account_values[canonical] = value
        
        # Add this debug block:
        if canonical == "SettledCash":
            logger.debug(f"DEBUG: Value *after* storing in account_values['{canonical}']: {self.account_values.get(canonical)}")
    
        # fast caches
        if canonical == "SettledCash":
            # Store the value received from IBKR
            self.settled_cash = value if isinstance(value, float) else None
            # --- MODIFICATION: Seed internal value only during initial sync ---
            if not self._settled_cash_initialized_from_ib and self.settled_cash is not None:
                # This seeding is now primarily handled in _check_and_set_sync_complete
                # to ensure NLV is also present, but we can log if it arrives here first.
                logger.info(f"Received initial SettledCash value: ${self.settled_cash:,.2f}. Waiting for NLV for full sync.")
            elif self._settled_cash_initialized_from_ib and self.settled_cash is not None:
                # Optional: Log if IBKR's value differs significantly from internal tracking after init?
                if self.settled_cash_internal is not None and abs(self.settled_cash - self.settled_cash_internal) > 1.0: # Example threshold $1
                    logger.warning(f"IBKR SettledCash ({self.settled_cash:,.2f}) differs from internally tracked ({self.settled_cash_internal:,.2f}). Check logic or external activity.")
            # --- END MODIFICATION ---

        elif canonical == "TotalCashValue":
            self.current_cash = value if isinstance(value, float) else None # Still useful for reference
        elif canonical == "NetLiquidation":
            # --- MODIFICATION: Pass value to _update_portfolio_value ---
            # self.portfolio_value = value if isinstance(value, float) else None # Original direct assignment
            self._update_portfolio_value(value if isinstance(value, float) else None) # Call update method
            # --- END MODIFICATION ---
        
        # Add this debug block before the check:
        if canonical == "SettledCash":
            logger.debug(f"DEBUG: Before calling _check_and_set_sync_complete. Value in dict: {self.account_values.get('SettledCash')}")

        # recheck sync barrier (existing code)
        if canonical in self.required_account_tags or canonical == "SettledCash": # Ensure SettledCash also triggers check
            self._check_and_set_sync_complete()

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
               logger.debug(f"Portfolio Value (NLV) updated: {old_nlv_log_str} -> ${self.portfolio_value:,.2f}")

               if self.high_water_mark is None or new_nlv > self.high_water_mark: self.high_water_mark = new_nlv
               logger.info(f"New High Water Mark set: ${self.high_water_mark:,.2f}")
               
               record_time = self.current_time or datetime.datetime.now(datetime.timezone.utc)
               self.performance_tracker.record_equity(record_time, self.portfolio_value)
               
               # --- Add history tracking and rapid loss check ---
               if self.portfolio_value is not None:
                  # Ensure record_time is timezone-aware UTC
                  if record_time.tzinfo is None: record_time = record_time.replace(tzinfo=datetime.timezone.utc)
                  else: record_time = record_time.astimezone(datetime.timezone.utc)
                  
                  self.portfolio_value_history.append((record_time, self.portfolio_value))

                  # Prune history older than the check window
                  cutoff_time = record_time - datetime.timedelta(seconds=self.rapid_loss_window_sec)
                  while self.portfolio_value_history and self.portfolio_value_history[0][0] < cutoff_time:
                      self.portfolio_value_history.popleft()
                      # Check for rapid loss if not already halted/stopping
                      if not self._stopping and not self.trading_halted:
                          self.check_rapid_loss_limit() # Call the new check method
                # --- End modification ---

    # --- MODIFICATION START: Added check_rapid_loss_limit method ---
    def check_rapid_loss_limit(self):
        """Checks for rapid portfolio value drops over a short time window."""
        if len(self.portfolio_value_history) < 2 or self.rapid_loss_threshold_pct <= 0:
            return # Not enough history or check disabled

        # Get the current value and time
        current_time, current_value = self.portfolio_value_history[-1]

        # Define the start of the time window
        start_time_window = current_time - datetime.timedelta(seconds=self.rapid_loss_window_sec)

        # Find the portfolio value at (or just after) the start of the window
        value_at_window_start = None
        earliest_time_in_window = None
        for ts, val in self.portfolio_value_history:
            if ts >= start_time_window:
                value_at_window_start = val
                earliest_time_in_window = ts
                break # Found the earliest value within the window

        # Check if we have a valid starting value and the window is sufficiently covered
        if value_at_window_start is None or value_at_window_start <= 0 or earliest_time_in_window is None:
             # logger.debug("Rapid loss check skipped: Not enough history in window or invalid start value.")
             return # Cannot calculate percentage

        # Ensure enough time has passed for a meaningful check
        time_elapsed_in_window = (current_time - earliest_time_in_window).total_seconds()
        if time_elapsed_in_window < self.rapid_loss_window_sec * 0.5: # Optional: Check only if window is somewhat full
            # logger.debug(f"Rapid loss check skipped: Window not sufficiently covered ({time_elapsed_in_window:.1f}s < {self.rapid_loss_window_sec * 0.5:.1f}s)")
            return

        # Calculate the percentage change
        pct_change = (current_value - value_at_window_start) / value_at_window_start

        logger.debug(f"Rapid loss check: StartVal={value_at_window_start:.2f} @ {earliest_time_in_window}, CurrVal={current_value:.2f} @ {current_time}, PctChange={pct_change*100:.3f}%")

        # Check if the loss threshold is breached
        if pct_change < -self.rapid_loss_threshold_pct:
            actual_window_duration = (current_time - earliest_time_in_window).total_seconds()
            reason = (f"Rapid loss limit breached: {-pct_change*100:.2f}% drop "
                      f"(Threshold: {self.rapid_loss_threshold_pct*100:.2f}%) | "
                      f"Value ${value_at_window_start:,.2f} -> ${current_value:,.2f} "
                      f"over ~{actual_window_duration:.1f} seconds (Window: {self.rapid_loss_window_sec}s)")

            self.trading_halted = True
            logger.critical(f"RAPID LOSS KILL-SWITCH TRIGGERED: {reason}. Halting trading immediately.")
            self.trigger_portfolio_shutdown(reason) # Trigger shutdown sequence
    # --- MODIFICATION END ---
    
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
        if new_price is not None and not math.isnan(new_price) and new_price >= 0: # Allow 0 for VIX?
            # --- MODIFICATION: Handle VIX Update ---
            if self.volatility_monitoring_enabled and symbol == self.volatility_symbol:
                 if self.latest_vix_value is None or abs(self.latest_vix_value - new_price) > 1e-4: # Check for change
                      self.latest_vix_value = new_price
                      logger.info(f"Volatility Index ({self.volatility_symbol}) updated: {self.latest_vix_value:.2f}")
                      self._check_volatility_thresholds() # Check thresholds on update
            # --- END MODIFICATION ---
            else: # Handle regular trading symbols
                if abs(self.last_market_prices.get(symbol, 0.0) - new_price) > 1e-6:
                    self.last_market_prices[symbol] = new_price
                    if symbol in self.current_holdings and abs(self.current_holdings[symbol].get('quantity', 0.0)) > 1e-9:
                        self._update_unrealized_pnl()

    # --- MODIFICATION: Add method to check VIX thresholds ---
    def _check_volatility_thresholds(self):
        """Checks the latest VIX value against configured thresholds and updates state."""
        if not self.volatility_monitoring_enabled or self.latest_vix_value is None:
            # Ensure state is normal if disabled or no data
            if self.is_high_volatility_regime:
                 logger.info(f"Volatility ({self.volatility_symbol}) returned to normal range or monitoring disabled.")
                 self.is_high_volatility_regime = False
                 self.new_entries_paused = False # Resume entries if pause was due to VIX
            return

        currently_high = self.latest_vix_value > self.vix_threshold_high
        
        # Optional: Critical threshold check for potential halt
        if self.vix_threshold_critical is not None and self.latest_vix_value > self.vix_threshold_critical:
            reason = f"CRITICAL VOLATILITY: {self.volatility_symbol} ({self.latest_vix_value:.2f}) exceeded critical threshold ({self.vix_threshold_critical:.2f})!"
            logger.critical(reason)
            # --- ADD SHUTDOWN TRIGGER ---
            self.trigger_portfolio_shutdown(reason)
            # --- END ADDITION ---

        # Handle high threshold crossing
        if currently_high and not self.is_high_volatility_regime:
            self.is_high_volatility_regime = True
            logger.warning(f"High Volatility Regime Entered: {self.volatility_symbol} ({self.latest_vix_value:.2f}) > Threshold ({self.vix_threshold_high:.2f})")
            if self.pause_entries_on_high_vix:
                 logger.warning("Pausing new trade entries due to high volatility.")
                 self.new_entries_paused = True
            self.current_risk_per_trade = self.base_risk_per_trade * self.high_vol_risk_multiplier
            logger.warning(f"Reducing risk per trade to {self.current_risk_per_trade:.4f} due to high volatility.")


        elif not currently_high and self.is_high_volatility_regime:
            self.is_high_volatility_regime = False
            logger.info(f"High Volatility Regime Exited: {self.volatility_symbol} ({self.latest_vix_value:.2f}) <= Threshold ({self.vix_threshold_high:.2f})")
            # Resume entries ONLY if the pause was due to high VIX
            if self.pause_entries_on_high_vix and self.new_entries_paused:
                 logger.info("Resuming new trade entries as volatility subsided.")
                 self.new_entries_paused = False
            self.current_risk_per_trade = self.base_risk_per_trade
            logger.info(f"Restoring risk per trade to {self.current_risk_per_trade:.4f} as volatility subsided.")

    # --- END MODIFICATION ---

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

        # --- MODIFICATION: Process settlements FIRST ---
        # Process settlements due *before* this new day starts
        # Ensure internal cash is initialized before processing
        if self._settled_cash_initialized_from_ib and self.settled_cash_internal is not None:
            if self.current_time is None or new_date > self.current_time.date(): # Process only on date change
                settled_today = 0.0
                pending_dates = list(self.pending_settlements.keys())
                for settlement_date_key in pending_dates:
                    # Add robust type checking for the key
                    if isinstance(settlement_date_key, datetime.date):
                        settlement_date_dt = settlement_date_key
                    elif isinstance(settlement_date_key, datetime.datetime):
                        settlement_date_dt = settlement_date_key.date()
                    else:
                        logger.warning(f"Skipping invalid settlement key type: {type(settlement_date_key)}")
                        continue

                    if settlement_date_dt <= new_date: # Settle if due today or earlier
                        try:
                            amount = self.pending_settlements.pop(settlement_date_key)
                            settled_today += amount
                            logger.debug(f"Settling ${amount:.2f} due on {settlement_date_dt} for date {new_date}")
                        except KeyError:
                            logger.warning(f"Settlement key {settlement_date_key} already removed, potentially concurrency issue.")
                            pass # Avoid error if key was somehow removed

                if settled_today > 0:
                    self.settled_cash_internal += settled_today
                    logger.info(f"[{new_date}] Internally settled ${settled_today:.2f}. Internal Settled Cash now: ${self.settled_cash_internal:,.2f}")
        elif self.current_time is None or new_date > self.current_time.date():
            # Log warning if date changes but cannot process settlement yet
            logger.warning(f"[{new_date}] Cannot process internal settlements, internal settled cash not yet initialized from IBKR.")
        # --- END MODIFICATION ---

        # --- Existing daily state update ---
        if self.current_time is None or new_date > self.current_time.date():
            self.update_daily_state(timestamp) # Existing call

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
        """Generates OrderEvent based on SignalEvent, considering risk, *internally tracked settled cash*, and volatility scaling."""
        # --- MODIFICATION: Add check for volatility pause ---
        if self.new_entries_paused and signal.direction == 'LONG':
             logger.info(f"New entries paused (High Volatility?). Ignoring LONG signal for {signal.symbol}")
             return
        # --- END MODIFICATION ---
        
        if self._stopping or self.trading_halted: logger.debug(f"Halted/Stopping. Ignoring signal: {signal}"); return
        if not isinstance(signal, SignalEvent): logger.error(f"Invalid event type: {type(signal)}"); return

        symbol, direction, timestamp = signal.symbol, signal.direction, signal.timestamp

        cash_to_use = self.settled_cash_internal
        if direction == 'LONG':
            if cash_to_use is None or cash_to_use < 1.0:
                 if not self._settled_cash_initialized_from_ib: logger.warning(f"Internal settled cash not yet initialized from IBKR. Cannot size order for {signal}. Waiting for sync.")
                 else: logger.error(f"Internal settled cash is None even after initial sync! State error. Cannot size order for {signal}.")
                 return
            if cash_to_use < 1.0: logger.warning(f"Insufficient internal settled cash (${cash_to_use:.2f}) to consider LONG {signal}."); return

        # --- Pre-Trade Checks (Existing and New) ---
        if self.portfolio_value is None or self.portfolio_value <= 0:
             logger.error(f"Cannot process signal for {symbol}, portfolio value unknown or zero.")
             return

        current_pos_count = len([s for s, d in self.current_holdings.items() if abs(d.get('quantity', 0.0)) > 1e-9])
        if direction == 'LONG' and current_pos_count >= self.max_active_positions:
            logger.warning(f"Max positions ({self.max_active_positions}) reached. Ignoring LONG {symbol}.")
            return

        current_price = self.data_handler.get_latest_price(symbol) or self.last_market_prices.get(symbol)
        if current_price is None: logger.error(f"No price for {symbol}, cannot process signal: {signal}"); return

        quantity, order_direction, order_type = 0, None, 'MKT'
        stop_price, limit_price, take_profit_price = signal.stop_price, None, None
        atr_value = signal.atr_value
        trailing_amount, trailing_percent, trail_stop_price = None, None, None

        # --- ADDED: Sector Limit Check (Gap 2) ---
        if direction == 'LONG' and self.max_sector_allocation_pct < 1.0 and self.symbol_sector_map:
            signal_sector = self.symbol_sector_map.get(symbol)
            if signal_sector:
                current_sector_value = 0.0
                # Calculate current value allocated to this sector
                for held_symbol, details in self.current_holdings.items():
                    if abs(details.get('quantity', 0.0)) > 1e-9:
                        sector = self.symbol_sector_map.get(held_symbol)
                        if sector == signal_sector:
                            price = self.last_market_prices.get(held_symbol, details.get('average_cost', 0.0))
                            current_sector_value += abs(details['quantity'] * price)

                # Estimate potential value add of this trade *before* detailed sizing
                # Use risk amount as proxy for potential value add (conservative)
                cash_available_for_risk = self.settled_cash_internal or 0.0
                risk_amount = cash_available_for_risk * self.current_risk_per_trade # Use the potentially adjusted risk
                # A better proxy might be needed if risk amount doesn't correlate well with position value
                # For now, let's assume the potential *cost* is related to risk amount,
                # but this is imprecise. A better way is to calculate target_quantity *first*.
                # Let's defer this check until after quantity calculation.

            else:
                logger.warning(f"No sector mapping found for {symbol}. Cannot enforce sector limit for this trade.")
        # --- END Sector Limit Check (Initial Part) ---

        # --- Initialize Variables ---
        quantity = 0
        order_direction = None
        stop_price = signal.stop_price # Get initial stop from signal for LONG
        atr_value = signal.atr_value   # Get ATR from signal (for vol scaling AND potentially storing for trailing stop)
        limit_price = None
        take_profit_price = None
        order_type = 'MKT' # Default
        trailing_amount = None
        trailing_percent = None
        trail_stop_price = None

        # --- Sizing Logic for LONG ---
        if direction == 'LONG':
            # Use the internally tracked settled cash value.
            cash_to_use = self.settled_cash_internal
            
            # Check if internal settled cash is initialized and sufficient (only for LONG trades)
            if cash_to_use is None or cash_to_use < 1.0:
                # Log appropriate message based on whether initial sync should be complete
                if not self._settled_cash_initialized_from_ib:
                    logger.warning(f"Internal settled cash not yet initialized from IBKR. Cannot size order for {signal}. Waiting for sync.")
                else:
                    logger.error(f"Internal settled cash is None even after initial sync! State error. Cannot size order for {signal}.")
                return # Cannot proceed without internal settled cash
            
            if cash_to_use < 1.0:
                logger.warning(f"Insufficient internal settled cash (${cash_to_use:.2f}) to consider LONG {signal}.")
                return
            
            # --- Existing checks (already long, wash sale, stop price validation) ---
            if self.current_holdings.get(symbol, {}).get('quantity', 0.0) > 1e-9: logger.debug(f"Already long {symbol}. Ignoring LONG signal."); return
            if symbol in self.loss_sale_dates and (timestamp - self.loss_sale_dates[symbol]).days <= 30: logger.warning(f"Potential Wash Sale: Buying {symbol} within 30 days of loss.")
            if stop_price is None or stop_price <= 0: logger.error(f"Invalid stop price ({stop_price}) for LONG {symbol}. Signal ignored."); return
            if stop_price >= current_price: logger.error(f"Stop ({stop_price:.2f}) not below price ({current_price:.2f}) for LONG {symbol}. Signal ignored."); return
            stop_distance = current_price - stop_price
            if stop_distance <= 1e-6: logger.error(f"Stop distance too small ({stop_distance:.4f}) for {symbol}. Signal ignored."); return

            # --- Calculate Initial Quantity (Based on Fixed Fractional Risk of Settled Cash) ---
            risk_amount = cash_to_use * self.risk_per_trade # Use correct settled cash
            target_quantity = math.floor(risk_amount / stop_distance)
            if target_quantity <= 0:
                logger.warning(f"Calculated initial qty zero for {symbol}. Risk: ${risk_amount:.2f}, StopDist: ${stop_distance:.2f}, Settled: ${cash_to_use:.2f}")
                return

            # --- SOLUTION C: Volatility Scaling (Optional) ---
            apply_vol_scaling = self.config.get('trading', {}).get('apply_volatility_scaling', False)
            target_vol_contrib = self.config.get('trading', {}).get('target_volatility_contribution', 0.005) # Example target daily volatility %

            if apply_vol_scaling and target_vol_contrib > 0:
                if atr_value is not None and atr_value > 1e-6 and current_price > 0:
                    # Calculate volatility as ATR percentage of current price
                    current_vol_pct = atr_value / current_price
                    if current_vol_pct > 1e-6: # Avoid division by zero if vol is tiny
                        # Calculate scaling factor
                        vol_scaling_factor = min(1.0, target_vol_contrib / current_vol_pct)
                        # Apply scaling factor to the quantity derived from risk %
                        scaled_quantity = math.floor(target_quantity * vol_scaling_factor)

                        if scaled_quantity < target_quantity:
                            logger.info(f"Applying volatility scaling to {symbol}. Qty {target_quantity} -> {scaled_quantity} "
                                        f"(ATR %: {current_vol_pct:.4f}, Target %: {target_vol_contrib:.4f}, Factor: {vol_scaling_factor:.2f})")
                            target_quantity = scaled_quantity # Update quantity
                        # Ensure quantity is still positive after scaling
                        if target_quantity <= 0:
                            logger.warning(f"Quantity became zero after volatility scaling for {symbol}. Signal ignored.")
                            return
                    else:
                        logger.warning(f"Calculated volatility percentage is near zero ({current_vol_pct:.6f}) for {symbol}. Skipping scaling.")
                else:
                    logger.warning(f"Could not apply volatility scaling for {symbol}: Missing ATR ({atr_value}), zero ATR, or zero price ({current_price}). Using original quantity: {target_quantity}")
            # --- END SOLUTION C ---

            # --- ADDED: Leverage/Gross Exposure Check (Gap 1) ---
            # Requires GrossPositionValue from account updates, or calculate manually
            current_gross_pos_value = self.account_values.get("GrossPositionValue") # Try direct value
            if current_gross_pos_value is None: # Fallback: Calculate manually
                 current_gross_pos_value = sum(
                     abs(details['quantity'] * self.last_market_prices.get(sym, details.get('average_cost', 0.0)))
                     for sym, details in self.current_holdings.items()
                 )
                 logger.debug("GrossPositionValue not available from IB, calculated manually.")

            potential_trade_value = target_quantity * current_price
            potential_total_gross_exposure = current_gross_pos_value + potential_trade_value
            max_allowed_gross_exposure = self.portfolio_value * self.max_gross_exposure_ratio

            if potential_total_gross_exposure > max_allowed_gross_exposure:
                 logger.warning(f"Gross Exposure limit breached for {symbol}. "
                                f"Potential Exposure: ${potential_total_gross_exposure:,.2f} "
                                f"(Current: ${current_gross_pos_value:,.2f}, New: ${potential_trade_value:,.2f}) > "
                                f"Limit: ${max_allowed_gross_exposure:,.2f} "
                                f"({self.max_gross_exposure_ratio:.2f}x Portfolio ${self.portfolio_value:,.2f}). "
                                f"Signal ignored.")
                 return # Reject signal
            else:
                 logger.debug(f"Gross Exposure Check Passed for {symbol}: "
                              f"Potential: ${potential_total_gross_exposure:,.2f} <= Limit: ${max_allowed_gross_exposure:,.2f}")
            # --- END Leverage/Gross Exposure Check ---


            # --- REVISED: Sector Limit Check (Gap 2 - After Quantity Calculation) ---
            if direction == 'LONG' and self.max_sector_allocation_pct < 1.0 and self.symbol_sector_map:
                signal_sector = self.symbol_sector_map.get(symbol)
                if signal_sector:
                    current_sector_value = 0.0
                    for held_symbol, details in self.current_holdings.items():
                        if abs(details.get('quantity', 0.0)) > 1e-9:
                            sector = self.symbol_sector_map.get(held_symbol)
                            if sector == signal_sector:
                                price = self.last_market_prices.get(held_symbol, details.get('average_cost', 0.0))
                                current_sector_value += abs(details['quantity'] * price)

                    potential_trade_value = target_quantity * current_price # Value based on calculated quantity
                    potential_sector_total = current_sector_value + potential_trade_value
                    max_allowed_sector_value = self.portfolio_value * self.max_sector_allocation_pct

                    if potential_sector_total > max_allowed_sector_value:
                        logger.warning(f"Sector Allocation limit for '{signal_sector}' breached for {symbol}. "
                                       f"Potential Allocation: ${potential_sector_total:,.2f} "
                                       f"(Current: ${current_sector_value:,.2f}, New: ${potential_trade_value:,.2f}) > "
                                       f"Limit: ${max_allowed_sector_value:,.2f} "
                                       f"({self.max_sector_allocation_pct:.1%} of Portfolio ${self.portfolio_value:,.2f}). "
                                       f"Signal ignored.")
                        return # Reject signal
                    else:
                         logger.debug(f"Sector Allocation Check Passed for {symbol} ({signal_sector}): "
                                      f"Potential: ${potential_sector_total:,.2f} <= Limit: ${max_allowed_sector_value:,.2f}")
            # --- END REVISED Sector Limit Check ---

            # --- Determine Order Type and Limit Price (Solution A) ---
            # --- Add logic here based on config or strategy preference ---
            # Example: Use LMT for mean reversion, MKT for momentum (requires knowing strategy type)
            # Or use config: prefer_lmt_orders = self.config.get('trading', {}).get('prefer_limit_orders', False)
            prefer_lmt_orders = False # Placeholder - Set based on real logic/config
            if prefer_lmt_orders:
                 order_type = 'LMT'
                 # Calculate Limit Price (Needs specific logic)
                 # Example: Set limit at current price (potentially aggressive) or midpoint/ask
                 limit_price = current_price # Placeholder - Replace with actual calculation
                 # Add validation for calculated limit_price
                 if limit_price is None or limit_price <= 0:
                      logger.warning(f"Could not determine valid limit price for LMT {symbol}. Reverting to MKT.")
                      order_type = 'MKT'
                      limit_price = None
            # --- End Order Type Logic ---

            # --- Calculate Take Profit Price (Solution B) ---
            # --- Add logic here based on config or strategy preference ---
            # Example: Use a fixed Risk:Reward ratio based on stop distance
            # rr_ratio = self.config.get('trading', {}).get('risk_reward_ratio', 2.0) # Example R:R=2
            rr_ratio = 2.0 # Placeholder
            if rr_ratio > 0 and stop_distance > 0:
                 take_profit_price = round(current_price + (stop_distance * rr_ratio), 2)
            # --- End Take Profit Logic ---

            # --- Affordability Check (uses potentially scaled target_quantity) ---
            estimated_cost = target_quantity * current_price
            required_cash_buffer = max(10.0, cash_to_use * 0.001) # Use correct settled cash
            if estimated_cost > (cash_to_use - required_cash_buffer):
                 new_target_quantity = math.floor((cash_to_use - required_cash_buffer) / current_price)
                 logger.warning(f"Reducing {symbol} LONG size {target_quantity}->{new_target_quantity} due to available internal settled cash (${cash_to_use:.2f}).")
                 target_quantity = new_target_quantity
                 if target_quantity <= 0: logger.warning(f"Cannot afford 1 share of {symbol} with available internal settled cash (${cash_to_use:.2f})."); return
            # --- END Affordability Check ---
            quantity, order_direction = target_quantity, 'BUY'

        # --- Logic for EXIT ---
        elif direction == 'EXIT':
             current_qty_details = self.current_holdings.get(symbol, {})
             current_qty = current_qty_details.get('quantity', 0.0)
             if current_qty <= 1e-9: logger.debug(f"Not long {symbol}. Ignoring EXIT signal."); return
             quantity = abs(current_qty)
             order_direction = 'SELL'
             stop_price = None # No stop loss needed for market EXIT

             # --- Trailing Stop Logic (RESOLUTION 1) ---
             use_trailing_stop = self.config.get('trading', {}).get('use_trailing_stops', True) # Get config flag
             order_type = 'MKT' # Default exit type

             if use_trailing_stop:
                # --- Refined Trailing Stop Logic ---
                entry_details = self.current_holdings.get(symbol, {})
                entry_atr = entry_details.get('entry_atr')
                initial_stop = entry_details.get('initial_stop_price')
                entry_price = entry_details.get('average_cost') # Use avg cost

                # Prefer ATR-based trail if available
                if entry_atr is not None and entry_atr > 0:
                    # Use the same multiplier as entry stop for trail distance
                    atr_stop_mult = self.config.get('trading', {}).get('atr_stop', {}).get('atr_stop_mult', 2.0)
                    trail_dist_atr = entry_atr * atr_stop_mult
                    trailing_amount = round(trail_dist_atr, 2)
                    order_type = 'TRAIL'
                    logger.info(f"Using ATR-based TRAIL stop for {symbol} exit: Trail Amount ${trailing_amount:.2f} (Based on Entry ATR: {entry_atr:.3f})")
                # Fallback to percentage if configured and ATR failed
                elif self.config.get('trading', {}).get('trailing_stop_percent') is not None:
                     trailing_percent_config = self.config['trading']['trailing_stop_percent']
                     if trailing_percent_config > 0:
                         trailing_percent = trailing_percent_config
                         order_type = 'TRAIL'
                         logger.info(f"Using config-based TRAIL stop for {symbol} exit: Trail Percent {trailing_percent:.2f}%")
                     else:
                         logger.warning(f"Invalid trailing_stop_percent ({trailing_percent_config}) in config. Reverting TRAIL to MKT.")
                else:
                    logger.warning(f"Could not determine trailing parameters for {symbol} from entry data or config. Reverting TRAIL to MKT exit.")
                # --- End Refined Trailing Stop Logic ---

             # Clear other prices if using TRAIL or MKT exit
             if order_type in ['TRAIL', 'MKT']:
                 stop_price = None
                 limit_price = None
                 take_profit_price = None
             # --- End Trailing Stop Logic ---
             
        else:
             logger.warning(f"Unsupported signal direction '{direction}' for {symbol}."); return

        # --- Generate and Queue OrderEvent ---
        if quantity > 0 and order_direction:
            # --- MODIFICATION: Pass all relevant prices/params to OrderEvent ---
            order = OrderEvent(
                timestamp=timestamp,
                symbol=symbol,
                order_type=order_type, # Can be MKT, LMT, or TRAIL now
                direction=order_direction,
                quantity=quantity,
                stop_price=stop_price, # None for EXITs
                limit_price=limit_price, # None for MKT/TRAIL EXITs
                take_profit_price=take_profit_price, # None for EXITs
                # --- SOLUTION C: Pass TRAIL params ---
                trailing_amount=trailing_amount,
                trailing_percent=trailing_percent,
                trail_stop_price=trail_stop_price
                # --- END SOLUTION C ---
            )
            # --- END MODIFICATION ---
            logger.info(f"Portfolio Manager generating order: {order}")
            try:
                self.event_queue.put_nowait(order)
            except queue.Full:
                logger.error("Event queue full, cannot queue OrderEvent.")

    def process_fill_event(self, fill: FillEvent):
        """
        Updates internal portfolio state based on a FillEvent from the executor.
        MODIFIED: Tracks internal settled cash and schedules settlements, and puts InternalFillProcessedEvent onto queue for OER tracking.
        """
        if self._stopping or not isinstance(fill, FillEvent): return
        logger.info(f"Portfolio processing LIVE fill: {fill}")
        symbol, qty, direction, fill_price, commission, cost = fill.symbol, fill.quantity, fill.direction, fill.fill_price, fill.commission, fill.fill_cost

        current_details = self.current_holdings.get(symbol, {})
        current_qty_before_fill, current_avg_cost, entry_timestamp = current_details.get('quantity', 0.0), current_details.get('average_cost', 0.0), current_details.get('entry_timestamp')

        realized_pnl_trade, holding_period_days = 0.0, None

        # --- Realized PNL Calculation (Existing Logic - No change needed here) ---
        if direction == 'SELL' and current_qty_before_fill > 1e-9:
            closed_qty = min(qty, current_qty_before_fill)
            if current_avg_cost is not None and current_avg_cost > 0:
                 realized_pnl_trade = closed_qty * (fill_price - current_avg_cost) - commission
                 self.realized_pnl_today += realized_pnl_trade # Tracks daily realized PnL
                 logger.info(f"Realized PNL {symbol} ({closed_qty:.2f} sh): ${realized_pnl_trade:.2f} (Daily: ${self.realized_pnl_today:.2f})")
                 if entry_timestamp: holding_period_days = round((fill.timestamp - entry_timestamp).total_seconds() / (24 * 3600), 2)
                 # Record the trade using the tracker
                 self.performance_tracker.record_trade(fill, realized_pnl_trade, holding_period_days)
                 # Track loss sale date for wash sale warnings (optional but present)
                 if realized_pnl_trade < 0: self.loss_sale_dates[symbol] = fill.timestamp; logger.debug(f"Recorded loss sale date {symbol}: {fill.timestamp.date()}")
            else:
                 logger.warning(f"Cannot calc realized PNL for {symbol} sell: avg cost {current_avg_cost}.")
                 # Record trade even if PnL calculation failed, PnL is 0.0
                 self.performance_tracker.record_trade(fill, 0.0, None)

        # --- MODIFICATION START: Update Internal Cash & Schedule/Use Settlement ---

        # Update Total Cash (Optional - if tracking separately, otherwise rely on IBKR TotalCashValue)
        # If tracking total cash internally:
        # if direction == 'BUY':
        #     self.total_cash_internal -= (cost + commission)
        # elif direction == 'SELL':
        #     self.total_cash_internal += (cost - commission)

        # Update Settled Cash (Internal Tracking)
        if direction == 'BUY':
            purchase_cost = cost + commission
            if self.settled_cash_internal is not None:
                self.settled_cash_internal -= purchase_cost
                logger.info(f"[{fill.timestamp.date()}] LIVE BOUGHT {qty} {symbol} @ {fill_price:.2f}. "
                            f"Debited internal settled cash. New balance: ${self.settled_cash_internal:,.2f}")
            else:
                # This should ideally not happen if initial sync works correctly
                logger.error(f"[{fill.timestamp.date()}] LIVE BOUGHT {qty} {symbol} BUT internal settled cash is None! State inconsistency. Cannot debit settled cash.")

        elif direction == 'SELL':
            proceeds = cost - commission
            # Schedule settlement for internally tracked settled cash
            if self.settlement_days >= 0: # settlement_days=0 means settles same day
                try:
                    # Ensure fill timestamp is timezone-aware UTC
                    fill_ts_utc = fill.timestamp
                    if fill_ts_utc.tzinfo is None:
                        fill_ts_utc = fill_ts_utc.replace(tzinfo=datetime.timezone.utc)
                    elif fill_ts_utc.tzinfo != datetime.timezone.utc:
                         fill_ts_utc = fill_ts_utc.astimezone(datetime.timezone.utc)

                    # Convert to pandas Timestamp for BusinessDay calculation
                    pd_fill_ts = pd.Timestamp(fill_ts_utc)

                    # Calculate settlement date
                    settlement_date = pd_fill_ts + BusinessDay(n=self.settlement_days)
                    # Use date object as the dictionary key
                    settlement_date_key = settlement_date.normalize().date()

                    self.pending_settlements[settlement_date_key] += proceeds
                    logger.info(f"[{fill.timestamp.date()}] LIVE SOLD {qty} {symbol} @ {fill_price:.2f}. "
                                f"Scheduling internal settle of ${proceeds:.2f} on {settlement_date_key}. "
                                f"Current internal settled cash: ${self.settled_cash_internal if self.settled_cash_internal is not None else 'N/A':,.2f}")
                except Exception as e:
                    logger.exception(f"Error calculating settlement date for live fill {fill}: {e}")
            else:
                 # If settlement_days is negative (invalid config), maybe log error or treat as T+0?
                 logger.warning(f"Invalid settlement_days ({self.settlement_days}). Treating proceeds ${proceeds:.2f} from {symbol} sell as immediately settled.")
                 if self.settled_cash_internal is not None:
                      self.settled_cash_internal += proceeds
                 else:
                      logger.error("Cannot add immediately settled proceeds, internal settled cash is None.")
        # --- MODIFICATION END ---

        # --- MODIFICATION: Signal fill processing for OER ---
        # After successfully processing the fill and updating state:
        try:
             internal_event = InternalFillProcessedEvent(fill.timestamp, fill.order_id)
             self.event_queue.put_nowait(internal_event)
             logger.debug(f"Queued {internal_event} for OER tracking.")
        except queue.Full:
             logger.error("Event queue full, cannot queue InternalFillProcessedEvent for OER.")
        except Exception as e:
             logger.exception(f"Error queuing InternalFillProcessedEvent: {e}")
        # --- END MODIFICATION ---

        # --- Update Holdings (Existing Logic - No change needed here) ---
        signed_qty = qty if direction == 'BUY' else -qty
        new_qty = current_qty_before_fill + signed_qty
        # Critical Fill Mismatch Check (Existing Logic)
        if direction == 'SELL' and qty > current_qty_before_fill + 1e-6:
            logger.error(f"CRITICAL FILL MISMATCH: Sell {qty} {symbol}, Hold {current_qty_before_fill:.4f}. State inconsistent."); new_qty = 0 # Prevent negative position

        if abs(new_qty) < 1e-9: # Position closed
            logger.info(f"Internal state update: Position {symbol} closed.")
            if symbol in self.current_holdings: del self.current_holdings[symbol]
            self.last_market_prices.pop(symbol, None)
        else: # Update existing or new position
            if direction == 'BUY':
                 # Update Average Cost (Existing Logic)
                 new_avg_cost = fill_price if abs(current_qty_before_fill) < 1e-9 or current_avg_cost == 0 else ((current_qty_before_fill * current_avg_cost) + cost) / new_qty if new_qty != 0 else 0
                 self.current_holdings[symbol]['average_cost'] = new_avg_cost
                 # --- Record Entry Details (RESOLUTION 1) ---
                 if abs(current_qty_before_fill) < 1e-9: # This fill opened the position
                     self.current_holdings[symbol]['entry_timestamp'] = fill.timestamp
                     entry_strategy = next((s for s in self.strategies if symbol in s.symbols), None)
                     if entry_strategy:
                         # Store ATR *at the time of the signal that led to this entry*
                         # Assuming atr_value was passed in the originating SignalEvent
                         # and potentially stored temporarily until fill confirms entry.
                         # For simplicity, we use the strategy's latest calculated ATR here.
                         entry_atr = entry_strategy.get_latest_atr(symbol)
                         self.current_holdings[symbol]['entry_atr'] = entry_atr
                         # Recalculate initial stop based on fill price
                         stop_price_at_entry, _ = entry_strategy._calculate_stop(symbol, fill_price)
                         self.current_holdings[symbol]['initial_stop_price'] = stop_price_at_entry
                         logger.info(f"Stored entry details for {symbol}: FillPrice={fill_price:.2f}, ATR={entry_atr}, InitialStop={stop_price_at_entry}")
                     else:
                          logger.warning(f"Could not find strategy for {symbol} to store entry ATR/Stop.")
                 # --- End Recording Entry Details ---
                 
            # Update Quantity (Existing Logic)
            self.current_holdings[symbol]['quantity'] = new_qty

        # --- Update Strategy View (Existing Logic) ---
        for strat in self.strategies:
            if symbol in strat.symbols: strat.update_position(symbol, new_qty)

        # --- Post-Fill Updates (Existing Logic) ---
        self.last_market_prices[symbol] = fill_price
        self._update_unrealized_pnl()
        # Trigger portfolio value update using last known NLV from IBKR
        # This ensures equity curve reflects fill impact immediately
        self._update_portfolio_value(self.account_values.get("NetLiquidation"))

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