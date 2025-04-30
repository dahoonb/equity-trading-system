# filename: data/ib_handler.py
# data/ib_handler.py (Revised V6 - Using concurrent.futures.Future for Sync)
import pandas as pd
import datetime
import time
import queue
import logging
import math
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
import threading
from collections import defaultdict, deque
import traceback # Added for more detailed exception logging
import concurrent.futures # Needed for Future object

# --- Import from ibapi ---
from ibapi.contract import Contract as IbkrContract, ContractDetails
from ibapi.common import BarData, HistogramData, HistoricalTick, HistoricalTickBidAsk, HistoricalTickLast

# Import core components
from core.events import MarketEvent, ShutdownEvent, ContractQualificationCompleteEvent
from core.ib_wrapper import IBWrapper, parse_ib_datetime
from utils.logger import logger
from config_loader import load_config

# --- Helper to create IbkrContract (Unchanged) ---
def create_ibkr_contract(symbol: str, sec_type_override: Optional[str] = None, exchange_override: Optional[str] = None, currency_override: Optional[str] = None) -> IbkrContract:
    """Creates an ibapi Contract object, allowing overrides for specific cases like VIX."""
    contract = IbkrContract()
    contract.symbol = symbol
    contract.currency = currency_override or 'USD'
    contract.exchange = exchange_override or 'SMART' # Default to SMART unless overridden

    if sec_type_override:
         contract.secType = sec_type_override
    elif '/' in symbol: # Basic Forex detection
        parts = symbol.split('/')
        if len(parts) == 2:
            contract.symbol = parts[0]
            contract.secType = 'CASH'
            contract.currency = parts[1] # Use the second part as currency for Forex
            contract.exchange = 'IDEALPRO'
        else:
            logger.warning(f"Cannot parse Forex symbol: {symbol}. Defaulting to Stock.")
            contract.secType = 'STK' # Fallback
            contract.exchange = 'SMART' # Reset exchange if fallback
    elif symbol.isdigit() and len(symbol) > 5: # Basic conId detection
         contract.conId = int(symbol)
         # Exchange often empty if using conId, unless overridden
         contract.exchange = exchange_override or ''
    else: # Assume Stock if not Forex, conId, or overridden
        contract.secType = 'STK'
        contract.exchange = 'SMART'

    # Refine exchange based on secType if not explicitly overridden
    if not exchange_override:
        if contract.secType == 'IND':
            contract.exchange = 'CBOE' # Common default for indices like VIX
        elif contract.secType == 'CASH':
             contract.exchange = 'IDEALPRO'
        elif contract.secType == 'STK' and contract.exchange != '': # Keep SMART unless conId path cleared it
             contract.exchange = 'SMART'
        # Add other common defaults (Futures, Options etc.) if needed
        # elif contract.secType == 'FUT': contract.exchange = 'GLOBEX' # Example

    logger.debug(f"Created contract: Symbol={contract.symbol}, SecType={contract.secType}, Exch={contract.exchange}, Curr={contract.currency}, ConId={getattr(contract, 'conId', 'N/A')}")
    return contract


# --- Constants for Historical Data State ---
HIST_STATE_PENDING = 'pending'
HIST_STATE_RECEIVING = 'receiving'
HIST_STATE_DONE = 'done'
HIST_STATE_ERROR = 'error'
HIST_STATE_TIMEOUT = 'timeout'
HIST_STATE_CANCELLED = 'cancelled'

# --- Type Alias for Historical Result ---
# Result can be DataFrame on success, Exception on error, or None on failure/empty
HistResultType = Union[pd.DataFrame, Exception, None]

class IBDataHandler:
    """
    Handles connection state, contract qualification, market data subscriptions,
    and historical data requests via the IBWrapper and ibapi.
    Uses permanent listeners and internal request tracking with Futures.
    """
    def __init__(self, event_q: queue.Queue, symbols: list, ib_wrapper: IBWrapper):
        if not isinstance(ib_wrapper, IBWrapper): raise TypeError("ib_wrapper must be an instance of IBWrapper")
        if not isinstance(event_q, queue.Queue): raise TypeError("event_q must be an instance of queue.Queue")
        self.ib_wrapper = ib_wrapper
        self.event_queue = event_q
        self.trading_symbols = list(set(symbols)) # Store original trading symbols
        self.latest_bar_close: Dict[str, float] = {}
        self.volatility_symbol_details = None
        self.all_symbols_to_manage = list(self.trading_symbols)

        # --- Load Config ---
        try:
            config = load_config("config.yaml")
            vol_config = config.get('volatility_monitoring', {})
            if vol_config.get('enabled', False):
                vol_symbol = vol_config.get('symbol')
                if vol_symbol:
                    self.volatility_symbol_details = {
                        'symbol': vol_symbol,
                        'sec_type': vol_config.get('sec_type', 'IND'),
                        'exchange': vol_config.get('exchange', 'CBOE'),
                        'currency': vol_config.get('currency', 'USD')
                    }
                    if vol_symbol not in self.all_symbols_to_manage:
                        self.all_symbols_to_manage.append(vol_symbol)
                    logger.info(f"Volatility monitoring enabled for: {self.volatility_symbol_details}")
                else: logger.warning("Volatility monitoring enabled but no symbol specified.")
            else: logger.info("Volatility monitoring disabled in config.")

            self.hist_req_limit = config.get('ibkr', {}).get('hist_req_limit_count', 59)
            self.hist_req_window_sec = config.get('ibkr', {}).get('hist_req_limit_window_sec', 600)
            logger.info(f"Historical data rate limit set to {self.hist_req_limit} requests per {self.hist_req_window_sec} seconds.")

        except Exception as config_exc:
             logger.exception(f"Error loading config for data handler: {config_exc}")
             self.hist_req_limit = 59 # Defaults
             self.hist_req_window_sec = 600

        # --- Instance Variables ---
        self.symbols = self.all_symbols_to_manage # Symbols currently managed (post-qualification)
        self.contracts: Dict[str, IbkrContract] = {} # Qualified contracts map

        self._stopping = False
        self._subscriptions_active = False

        # --- Qualification State ---
        self._qual_lock = threading.RLock()
        self._pending_qualification_reqs: set[int] = set()
        self._qualification_results: Dict[int, List[ContractDetails]] = defaultdict(list)
        self._qualification_req_id_map: Dict[int, str] = {} # reqId -> symbol
        self._qualification_failed_symbols: set[str] = set()
        self._initial_qualification_complete = threading.Event()
        self._qualification_started = False

        # --- Historical Data State (Using Futures for Sync) ---
        self._hist_lock = threading.Lock()
        self._hist_req_symbol_map: Dict[int, str] = {} # reqId -> symbol
        self._hist_req_data: Dict[int, List[Any]] = {} # reqId -> list of bars/ticks
        # Using concurrent.futures.Future for result/error synchronization
        self._hist_req_pending_futures: Dict[int, concurrent.futures.Future] = {} # reqId -> Future for result/error
        self._hist_req_state: Dict[int, str] = {} # reqId -> HIST_STATE_* status

        # --- Rate Limiting State ---
        self._hist_req_timestamps = deque()
        self._hist_req_pacing_lock = threading.Lock()

        # --- Live Data State ---
        self._subscription_map_lock = threading.Lock()
        self._subscription_req_map: Dict[int, str] = {} # reqId -> symbol

        # --- Register Permanent Listeners ---
        self.ib_wrapper.add_callback('contractDetails', self._on_contract_details)
        self.ib_wrapper.add_callback('contractDetailsEnd', self._on_contract_details_end)
        self.ib_wrapper.add_callback('error', self._on_error) # Generic error handler
        # Add permanent historical data listeners
        self.ib_wrapper.add_callback('historicalData', self._on_historical_data)
        self.ib_wrapper.add_callback('historicalDataEnd', self._on_historical_data_end)
        self.ib_wrapper.add_callback('historicalDataUpdate', self._on_historical_data_update)
        # Add others if needed (e.g., realtimeBar, tickPrice etc.)
        # self.ib_wrapper.add_callback('realtimeBar', self._on_realtime_bar)

        logger.debug("IBDataHandler initialized and registered permanent listeners.")

    # --- Qualification methods (_start_async..., _on_contract_details, _on_contract_details_end, _signal_qualification_complete) ---
    # --- remain largely unchanged ---
    def start_initial_sync(self):
        """Initiates the asynchronous contract qualification process."""
        self.start_async_contract_qualification()

    def start_async_contract_qualification(self):
        """Initiates asynchronous contract qualification if not already started."""
        if self._qualification_started:
             logger.info("Asynchronous contract qualification already initiated.")
             return

        if not self.ib_wrapper.isConnected():
            logger.error("Cannot start qualification, IB wrapper not connected.")
            self._signal_qualification_complete() # Signal immediate failure
            return

        unqualified_symbols = [s for s in self.all_symbols_to_manage if s not in self.contracts or not self.contracts[s].conId]

        if not unqualified_symbols:
            logger.info("All managed symbols appear to be already qualified.")
            self._qualification_started = True # Mark as started even if nothing to do
            self._signal_qualification_complete()
            return

        logger.info(f"Starting asynchronous qualification for {len(unqualified_symbols)} managed symbols...")
        self._qualification_started = True

        with self._qual_lock:
            self._pending_qualification_reqs.clear()
            self._qualification_results.clear()
            self._qualification_req_id_map.clear()
            self._qualification_failed_symbols.clear()
            self._initial_qualification_complete.clear()

            initial_pending_count = 0
            for symbol in unqualified_symbols:
                sec_type_override, exchange_override, currency_override = None, None, None
                # Check for volatility symbol overrides
                if self.volatility_symbol_details and symbol == self.volatility_symbol_details['symbol']:
                     details = self.volatility_symbol_details
                     sec_type_override = details.get('sec_type')
                     exchange_override = details.get('exchange')
                     currency_override = details.get('currency')
                     logger.debug(f"Using overrides for volatility symbol {symbol}: SecType={sec_type_override}, Exch={exchange_override}, Curr={currency_override}")

                temp_contract = create_ibkr_contract(
                    symbol,
                    sec_type_override=sec_type_override,
                    exchange_override=exchange_override,
                    currency_override=currency_override
                )

                if not temp_contract.symbol and not temp_contract.conId:
                    logger.error(f"Cannot request details for '{symbol}', insufficient info to create contract.")
                    self._qualification_failed_symbols.add(symbol)
                    continue

                req_id = self.ib_wrapper.get_next_req_id()
                self._pending_qualification_reqs.add(req_id)
                self._qualification_req_id_map[req_id] = symbol # Map reqId to symbol
                initial_pending_count += 1

                logger.debug(f"Async Qualify: Requesting details for {symbol} with ReqId {req_id}")
                try:
                    # Use the ib_wrapper instance directly
                    self.ib_wrapper.reqContractDetails(req_id, temp_contract)
                    time.sleep(0.05) # Small delay between requests
                except Exception as e:
                    logger.exception(f"Async Qualify: Exception requesting details for {symbol} (ReqId {req_id}): {e}")
                    # Clean up state for this failed request
                    self._pending_qualification_reqs.discard(req_id)
                    self._qualification_req_id_map.pop(req_id, None)
                    self._qualification_failed_symbols.add(symbol)
                    initial_pending_count -= 1 # Decrement count as it failed to submit

            if initial_pending_count == 0 and not self._pending_qualification_reqs:
                 logger.warning("Async Qualification: No requests successfully submitted or all failed immediately.")
                 # If no requests are pending, qualification is effectively complete (with failures)
                 self._signal_qualification_complete()

    def _on_contract_details(self, reqId: int, contractDetails: ContractDetails):
        """Handles incoming contract details (Callback from IBWrapper)."""
        with self._qual_lock:
            if reqId in self._pending_qualification_reqs:
                symbol = self._qualification_req_id_map.get(reqId, "Unknown")
                # logger.debug(f"Async Qualify: Received contract details for ReqId {reqId} ({symbol})")
                self._qualification_results[reqId].append(contractDetails)
            # else: logger.debug(f"Received contract details for inactive/unknown qual reqId {reqId}")

    def _on_contract_details_end(self, reqId: int):
        """Handles the end signal for a contract details request (Callback from IBWrapper)."""
        should_signal_completion = False
        with self._qual_lock:
            if reqId in self._pending_qualification_reqs:
                symbol = self._qualification_req_id_map.get(reqId)
                logger.debug(f"Async Qualify: ContractDetailsEnd received for ReqId {reqId} ({symbol or 'Unknown'})")

                if not symbol:
                    logger.error(f"Async Qualify: Received End for unknown ReqId {reqId}. Discarding.")
                else:
                    results = self._qualification_results.pop(reqId, []) # Get and remove results
                    if not results:
                        logger.error(f"Async Qualify: Failed qualification for {symbol} (ReqId {reqId}: No details received). Check symbol/permissions.")
                        self._qualification_failed_symbols.add(symbol)
                    elif len(results) == 1:
                        qualified_contract = results[0].contract
                        self.contracts[symbol] = qualified_contract # Store qualified contract
                        logger.info(f"Async Qualify: Contract qualified for {symbol}: ID={qualified_contract.conId}, LocalSymbol={qualified_contract.localSymbol or qualified_contract.symbol}")
                    else: # Ambiguous result
                        logger.warning(f"Async Qualify: Ambiguous contract for {symbol} ({len(results)} matches). Trying primary exchange...")
                        primary_found = None
                        # Common primary exchanges for US stocks
                        primary_exchanges = {'NASDAQ', 'NYSE', 'ARCA', 'AMEX', 'BATS', 'ISLAND'} # Added ISLAND
                        for detail in results:
                            # Use primaryExchange if available, otherwise fallback to exchange
                            primary_exch = getattr(detail.contract, 'primaryExchange', detail.contract.exchange)
                            if primary_exch in primary_exchanges:
                                if primary_found:
                                     logger.warning(f"Async Qualify: Multiple primary exchange matches for {symbol}. Sticking with first found ({primary_found.conId} on {getattr(primary_found, 'primaryExchange', primary_found.exchange)}).")
                                     break # Stick with the first primary match found
                                primary_found = detail.contract
                        if primary_found:
                            self.contracts[symbol] = primary_found
                            logger.info(f"Async Qualify: Selected primary exchange contract for {symbol}: ID={primary_found.conId}, Exch={getattr(primary_found, 'primaryExchange', primary_found.exchange)}")
                        else:
                            # Fallback: maybe just take the first result if no primary found? Or fail.
                            logger.error(f"Async Qualify: Could not determine primary contract for ambiguous {symbol}. Taking first result as fallback.")
                            # self._qualification_failed_symbols.add(symbol) # Option: Fail if ambiguous
                            self.contracts[symbol] = results[0].contract # Option: Take first result

                # Clean up state for this reqId
                self._pending_qualification_reqs.discard(reqId)
                self._qualification_req_id_map.pop(reqId, None)

                # Check if all pending requests are now processed
                if not self._pending_qualification_reqs:
                    logger.debug("All pending qualification requests processed.")
                    should_signal_completion = True
            # else: logger.debug(f"Received contract details end for inactive/unknown qual reqId {reqId}")

        # Signal completion *outside* the lock if needed
        if should_signal_completion:
            self._signal_qualification_complete()

    def _on_error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson="", *extra_pos_args, **kwargs):
        """Handles errors from IB. Routes errors for active requests (Qual, Hist) to specific handlers."""

        # Check if it's a qualification error
        symbol = None
        is_qual_error = False
        should_signal_qual_complete = False
        with self._qual_lock:
            if reqId in self._pending_qualification_reqs:
                is_qual_error = True
                symbol = self._qualification_req_id_map.get(reqId, "Unknown")

        if is_qual_error:
            logger.error(f"Async Qualify: Error for {symbol} (ReqId {reqId}): Code {errorCode} - {errorString}")
            with self._qual_lock:
                # Ensure symbol is added to failed list even if it was unknown initially
                if symbol != "Unknown":
                    self._qualification_failed_symbols.add(symbol)
                else:
                    logger.warning(f"Qualification error for unknown ReqId {reqId}. Cannot mark specific symbol as failed.")

                self._qualification_results.pop(reqId, None) # Clean up results if any
                self._pending_qualification_reqs.discard(reqId)
                self._qualification_req_id_map.pop(reqId, None)
                if not self._pending_qualification_reqs:
                    should_signal_qual_complete = True
            # Signal completion outside lock if necessary
            if should_signal_qual_complete:
                self._signal_qualification_complete()
            return # Handled as qualification error

        # Check if it's a historical data error
        is_hist_error = False
        hist_symbol = "Unknown"
        with self._hist_lock:
            # Check the future dictionary now
            if reqId in self._hist_req_pending_futures:
                 is_hist_error = True
                 hist_symbol = self._hist_req_symbol_map.get(reqId, "Unknown")

        if is_hist_error:
            logger.debug(f"[Hist CB _on_error]: Routing error for hist ReqId {reqId} ({hist_symbol}) code {errorCode} to handler.")
            self._handle_historical_error(reqId, errorCode, errorString)
            return # Handled by historical error processor

        # If not Qual or Hist error, let the IBWrapper log it as per its logic
        # (The IBWrapper's error method already logs these general errors)
        pass # No further action needed here for unhandled errors

    def _signal_qualification_complete(self):
        """Internal: Signals that the qualification process is finished and puts event."""
        if self._initial_qualification_complete.is_set():
             logger.debug("Qualification complete signal attempted but already set.")
             return

        logger.info("Async Contract Qualification: Finalizing results...")
        # Update the managed symbol lists based on success/failure
        with self._qual_lock: # Ensure thread safety reading failed symbols
             if self._qualification_failed_symbols:
                  logger.error(f"Removing symbols that failed qualification: {sorted(list(self._qualification_failed_symbols))}")
                  # Update the primary lists of symbols the handler manages
                  self.symbols = [s for s in self.symbols if s not in self._qualification_failed_symbols]
                  self.trading_symbols = [s for s in self.trading_symbols if s not in self._qualification_failed_symbols]
                  # Remove failed contracts
                  for s in self._qualification_failed_symbols: self.contracts.pop(s, None)

             # Check if the special volatility symbol failed
             if self.volatility_symbol_details and self.volatility_symbol_details['symbol'] in self._qualification_failed_symbols:
                  logger.error(f"Volatility monitoring symbol '{self.volatility_symbol_details['symbol']}' failed qualification.")
                  self.volatility_symbol_details = None # Disable if failed

        # Log final state
        logger.info(f"Async Contract Qualification process complete. Active managed symbols: {len(self.symbols)}")
        logger.info(f"Qualified contracts: {list(self.contracts.keys())}")
        if self._qualification_failed_symbols: logger.info(f"Failed symbols: {sorted(list(self._qualification_failed_symbols))}")

        # Set the event flag
        self._initial_qualification_complete.set()

        # Put event onto the main queue
        completion_event = ContractQualificationCompleteEvent(
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            successful_symbols=sorted(list(self.contracts.keys())),
            failed_symbols=sorted(list(self._qualification_failed_symbols))
        )
        try:
             # Use non-blocking put or timeout to avoid deadlocking main thread if queue is full
             self.event_queue.put(completion_event, block=True, timeout=5)
             logger.info(f"Put {type(completion_event).__name__} onto event queue.")
        except queue.Full:
             logger.critical(f"Event queue FULL. Failed to put {type(completion_event).__name__}!")
        except Exception as e:
             logger.exception(f"Error putting {type(completion_event).__name__} onto queue: {e}")

    def is_qualification_complete(self) -> bool:
         """Checks if the initial qualification process has finished."""
         return self._initial_qualification_complete.is_set()

    # --- subscribe_live_data unchanged ---
    def subscribe_live_data(self) -> bool:
         """Subscribes to live market data (5-second bars). Requires qualification first."""
         if self._stopping: logger.warning("Cannot subscribe, stopping."); return False
         if not self.ib_wrapper.isConnected(): logger.warning("Cannot subscribe, not connected."); return False

         # Wait briefly for qualification if it hasn't completed yet
         if not self.is_qualification_complete():
             logger.info("Waiting up to 10s for contract qualification before subscribing...")
             if not self._initial_qualification_complete.wait(timeout=10):
                 logger.error("Cannot subscribe live data, contract qualification timed out.")
                 return False
             logger.info("Qualification complete, proceeding with subscription.")

         if self._subscriptions_active: logger.info("Live data subscriptions already marked active."); return True
         if not self.contracts: logger.warning("No qualified contracts to subscribe to."); self._subscriptions_active = True; return True # No contracts is technically success

         logger.info(f"Requesting 5-second bar subscriptions for {len(self.contracts)} qualified contracts...")
         subscribed_count = 0
         failed_symbols = []
         req_ids_used = []
         with self._subscription_map_lock: # Lock around the whole subscription loop
             for symbol, contract in self.contracts.items():
                 # Check if already subscribed under this handler's management
                 if symbol in self._subscription_req_map.values():
                      logger.debug(f"Subscription for {symbol} seems already active locally.")
                      subscribed_count += 1
                      continue

                 if not contract or not contract.conId:
                     logger.warning(f"Skipping subscription for {symbol}, invalid/unqualified contract.")
                     continue

                 req_id = self.ib_wrapper.get_next_req_id()
                 logger.debug(f"Requesting realTimeBars for {symbol} (ConId: {contract.conId}, SecType: {contract.secType}) with ReqId {req_id}")
                 try:
                     self.ib_wrapper.reqRealTimeBars(
                         reqId=req_id, contract=contract, barSize=5, whatToShow="TRADES",
                         useRTH=False, # Get data outside RTH as well
                         realTimeBarsOptions=[]
                     )
                     self._subscription_req_map[req_id] = symbol # Map reqId to symbol
                     req_ids_used.append(req_id)
                     time.sleep(0.1) # Pacing
                     subscribed_count += 1
                 except Exception as e:
                     logger.error(f"Failed subscription request for {symbol} (ReqId {req_id}): {e}", exc_info=True)
                     failed_symbols.append(symbol)
                     # Remove req_id if it failed? No, keep it mapped maybe? Let's assume API call failed, no reqId active.

         if subscribed_count > 0: logger.info(f"Initiated/confirmed {subscribed_count} real-time bar subscriptions (ReqIDs: {req_ids_used}).")
         if failed_symbols: logger.error(f"Failed real-time bar subscriptions for: {failed_symbols}")

         # Mark active if at least one subscription succeeded or if there were no contracts to begin with
         self._subscriptions_active = subscribed_count > 0 or not self.contracts
         return self._subscriptions_active

    # =========================================================================
    # Synchronous Historical Data Request (Revised V7 - Using Futures)
    # =========================================================================
    def request_historical_data_sync(self, symbol: str, duration='1 Y', bar_size='1 day',
                                     what_to_show='TRADES', use_rth=True,
                                     end_dt: Optional[Union[datetime.datetime, str]] = None, # Corrected type hint
                                     max_retries=3, timeout_per_req=300.0) -> Optional[pd.DataFrame]:
        """
        Requests historical bar data synchronously using concurrent.futures.Future.
        Revised V7: Uses Futures for synchronization, robust state cleanup,
                    and returns None on final failure.
        Returns DataFrame on success, None on failure/timeout after retries.
        """
        requesting_thread = threading.current_thread().name
        logger.debug(f"[Hist Sync][{requesting_thread}] === Requesting {symbol} ===")

        if self._stopping:
            logger.warning(f"[Hist Sync][{requesting_thread}] Aborting request for {symbol}, system stopping.")
            return None

        # --- Contract Check ---
        contract = self.contracts.get(symbol)
        if not contract or not contract.conId:
            is_vol_symbol = self.volatility_symbol_details and symbol == self.volatility_symbol_details['symbol']
            if is_vol_symbol:
                 logger.warning(f"[Hist Sync][{requesting_thread}] Volatility symbol {symbol} not qualified. Attempting dynamic.")
                 details = self.volatility_symbol_details
                 contract = create_ibkr_contract(symbol, details['sec_type'], details['exchange'], details['currency'])
            else:
                 qual_status = "incomplete" if not self.is_qualification_complete() else "not found/qualified"
                 logger.error(f"[Hist Sync][{requesting_thread}] Cannot request {symbol}, qualification {qual_status}.")
                 return None

        # --- End Date String Prep ---
        end_dt_str = ""
        if isinstance(end_dt, datetime.datetime):
            # Ensure UTC if timezone-aware
            if end_dt.tzinfo is None:
                 end_dt_utc = end_dt.replace(tzinfo=datetime.timezone.utc)
            else:
                 end_dt_utc = end_dt.astimezone(datetime.timezone.utc)
            end_dt_str = end_dt_utc.strftime("%Y%m%d %H:%M:%S UTC")
        elif isinstance(end_dt, str):
            end_dt_str = end_dt

        # --- Pacing Check ---
        self._wait_for_pacing(requesting_thread, symbol)
        logger.info(f"[Hist Sync][{requesting_thread}] Requesting {symbol}, Dur: {duration}, Bar: {bar_size}, End: '{end_dt_str or 'Now'}'")

        # --- Retry Loop ---
        for attempt in range(max_retries):
            attempt_label = f"Attempt {attempt+1}/{max_retries}"
            req_id = -1 # Initialize req_id outside try
            # Using concurrent.futures.Future for synchronization
            req_future = concurrent.futures.Future()

            try:
                # --- Pre-Attempt Connection Check ---
                if not self.ib_wrapper.isConnected():
                    logger.error(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol}] Not connected before attempt.")
                    raise ConnectionError("Not connected")

                # --- Setup Request State ---
                req_id = self.ib_wrapper.get_next_req_id()
                with self._hist_lock:
                    if req_id in self._hist_req_pending_futures: # Sanity check for req_id reuse
                         logger.warning(f"[Hist Sync][{requesting_thread}] ReqId {req_id} already pending? Overwriting state for {symbol}.")
                    self._hist_req_symbol_map[req_id] = symbol
                    self._hist_req_data[req_id] = []
                    # Store the Future object instead of a Queue
                    self._hist_req_pending_futures[req_id] = req_future
                    self._hist_req_state[req_id] = HIST_STATE_PENDING
                    logger.debug(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol}] ReqId {req_id} state initialized (pending).")

                # --- Record Timestamp for Pacing ---
                with self._hist_req_pacing_lock:
                    self._hist_req_timestamps.append(time.monotonic())

                # --- Make API Call ---
                logger.debug(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] Calling reqHistoricalData...")
                self.ib_wrapper.reqHistoricalData(
                    reqId=req_id, contract=contract, endDateTime=end_dt_str,
                    durationStr=duration, barSizeSetting=bar_size, whatToShow=what_to_show,
                    useRTH=1 if use_rth else 0, formatDate=2, keepUpToDate=False, chartOptions=[]
                )
                with self._hist_lock: self._hist_req_state[req_id] = HIST_STATE_RECEIVING
                logger.debug(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] Request sent. Waiting for result...")

                # --- Wait for Result from Future ---
                # This is the blocking part for this attempt
                # Future.result() raises TimeoutError on timeout
                result = req_future.result(timeout=timeout_per_req)

                # --- Process Result ---
                if isinstance(result, pd.DataFrame):
                    logger.info(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] SUCCESS. Received DataFrame ({len(result)} rows).")
                    # Cleanup should have happened in the callback's finally block
                    return result # <<< SUCCESS EXIT POINT >>>

                elif result is None:
                    # This indicates processing failed or no data, treat as failure for sync
                    logger.error(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] FAILED. Received None signal (likely empty/failed processing).")
                    # State should have been cleaned up by the callback's finally block
                    return None # Indicate failure

                else: # Should not happen if callbacks only set DataFrame or None
                    logger.error(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] FAILED. Received unexpected result type from Future: {type(result)}")
                    raise TypeError(f"Unexpected result type {type(result)}")

            except TimeoutError: # Catch timeout from future.result()
                # --- Timeout Handling ---
                elapsed = timeout_per_req # Approximate time waited
                logger.error(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] TIMEOUT after {elapsed:.1f}s.")
                conn_status = "Connected" if self.ib_wrapper.isConnected() else "Disconnected"
                logger.error(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] Connection status at timeout: {conn_status}")

                # Update state and attempt cancellation
                with self._hist_lock:
                    if req_id != -1 and req_id in self._hist_req_state: # Check if state still exists
                         self._hist_req_state[req_id] = HIST_STATE_TIMEOUT
                if req_id != -1:
                     try:
                         logger.warning(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] Cancelling request due to timeout...")
                         self.ib_wrapper.cancelHistoricalData(req_id)
                         with self._hist_lock:
                              if req_id in self._hist_req_state: # Check again before setting cancelled
                                   self._hist_req_state[req_id] = HIST_STATE_CANCELLED
                     except Exception as cancel_e:
                         logger.error(f"[Hist Sync][{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] Error cancelling after timeout: {cancel_e}")

                # Raise TimeoutError to be caught by the consolidated exception handler below
                # No need to explicitly raise, falling through to the generic handler works
                pass # Let the generic handler below catch TimeoutError

            # Catch exceptions set on the Future by callbacks OR exceptions from this function
            except (ConnectionError, Exception) as e:
                # --- Consolidated Exception Handling for the Attempt ---
                # Check if the exception came from the Future (signaled by callback)
                error_code = getattr(e, 'code', -1)
                log_prefix = "[Hist Sync][Callback Error]" if hasattr(e, 'code') else "[Hist Sync][Request Error]"
                logger.warning(f"{log_prefix}[{requesting_thread}][{attempt_label}][{symbol} ReqId {req_id}] Attempt failed: {type(e).__name__} - {e}")

                # --- CRITICAL: Clean up state for the *failed attempt's* req_id ---
                # Cleanup happens in the callback's finally block now, but call here as safety
                # if the exception occurred *before* the callback could run/cleanup.
                if req_id != -1:
                    self._cleanup_hist_req_state(req_id, log_reason=f"Attempt {attempt+1} Exception: {type(e).__name__}")

                # --- Retry Logic ---
                if attempt < max_retries - 1:
                    # Check if the exception itself indicates no retry
                    if self._is_hist_error_non_retryable(error_code):
                         logger.error(f"[Hist Sync][{requesting_thread}] Non-retryable error {error_code} encountered during attempt {attempt+1}. Aborting retries.")
                         return None # Permanent failure

                    # --- Log BEFORE sleep ---
                    wait_time_retry = 2 ** (attempt + 1) # Exponential backoff
                    logger.info(f"[Hist Sync][{requesting_thread}][{symbol}] Retrying (Attempt {attempt+2}/{max_retries}). Waiting {wait_time_retry}s...")
                    time.sleep(wait_time_retry)
                    continue # Proceed to the next attempt in the for loop
                else:
                    # --- Max retries reached ---
                    logger.error(f"[Hist Sync][{requesting_thread}][{symbol}] Max retries ({max_retries}) reached after failure. Request definitively failed.")
                    return None # <<< FINAL FAILURE EXIT POINT >>>

        # Fallback exit point
        logger.error(f"[Hist Sync][{requesting_thread}] Failed to retrieve historical data for {symbol} after {max_retries} attempts (Loop Exit Fallback).")
        return None

    # --- Historical Data Callbacks (Modified for Futures) ---

    def _on_historical_data(self, reqId: int, bar: BarData):
        """Callback for individual historical bars."""
        with self._hist_lock:
            # Check the future dictionary
            if reqId in self._hist_req_pending_futures:
                # logger.debug(f"[Hist CB _on_historical_data]: Received bar for ReqId {reqId} ({bar.date})")
                self._hist_req_data[reqId].append(bar)
            # else: logger.warning(f"[Hist CB _on_historical_data]: Received bar for unknown/inactive ReqId {reqId}")

    def _on_historical_data_update(self, reqId: int, bar: BarData):
        """Callback for bars when keepUpToDate=True (Not used in sync request)."""
        logger.debug(f"[Hist CB _on_historical_data_update]: Received update bar for ReqId {reqId} - Unexpected for sync request.")

    def _on_historical_data_end(self, reqId: int, start: str, end: str):
        """Callback signalling the end of a historical data stream. Sets Future result."""
        logger.debug(f"[Hist CB _on_historical_data_end]: Received End signal for ReqId {reqId} (Range: {start} to {end})")
        hist_symbol = "Unknown"
        req_future: Optional[concurrent.futures.Future] = None
        bars_to_process: Optional[List[BarData]] = None
        should_signal = False

        with self._hist_lock:
            # Check the future dictionary
            if reqId in self._hist_req_pending_futures:
                hist_symbol = self._hist_req_symbol_map.get(reqId, "Unknown")
                current_state = self._hist_req_state.get(reqId, "Unknown")

                # Check if we are expecting the end signal
                if current_state in [HIST_STATE_RECEIVING, HIST_STATE_PENDING]:
                     self._hist_req_state[reqId] = HIST_STATE_DONE
                     # Retrieve the Future object
                     req_future = self._hist_req_pending_futures.get(reqId)
                     bars_to_process = list(self._hist_req_data.get(reqId, [])) # Get a copy
                     logger.debug(f"[Hist CB _on_historical_data_end]: ReqId {reqId} ({hist_symbol}) marked DONE. Processing {len(bars_to_process)} bars.")
                     should_signal = True
                     # DO NOT CLEANUP STATE HERE - moved to finally block after signaling
                else:
                     logger.warning(f"[Hist CB _on_historical_data_end]: Received End for ReqId {reqId} ({hist_symbol}) but state was '{current_state}'. Ignoring signal.")
                     # Cleanup might still be needed if state is inconsistent
                     self._cleanup_hist_req_state(req_id=reqId, log_reason=f"Data End arrived late (state={current_state})")
            # else: logger.warning(f"[Hist CB _on_historical_data_end]: Received End for unknown/inactive ReqId {reqId}")

        # --- Process and Signal Result (Outside Lock, inside try...finally) ---
        if should_signal and req_future:
            processed_df = None # Initialize
            try:
                # Process the collected bars into a DataFrame
                processed_df = self._process_historical_df_from_bardata(bars_to_process, hist_symbol)
                logger.debug(f"[Hist CB _on_historical_data_end]: Processed DataFrame for ReqId {reqId}, shape {processed_df.shape if processed_df is not None else 'None'}")
                # Set the result on the Future object
                if not req_future.done():
                    req_future.set_result(processed_df) # Set DataFrame or None
                    logger.debug(f"[Hist CB _on_historical_data_end]: Result set on Future for ReqId {reqId}.")
                else:
                    logger.warning(f"[Hist CB _on_historical_data_end]: Future for ReqId {reqId} was already done. Not setting result.")
            except Exception as e:
                 logger.exception(f"[Hist CB _on_historical_data_end]: Exception processing/setting result for ReqId {reqId}: {e}")
                 # Try to set the exception on the Future
                 try:
                     if not req_future.done():
                         req_future.set_exception(e) # Signal failure
                         logger.error(f"[Hist CB _on_historical_data_end]: Exception set on Future for ReqId {reqId}.")
                     else:
                          logger.warning(f"[Hist CB _on_historical_data_end]: Future for ReqId {reqId} was already done. Not setting exception.")
                 except Exception as set_exc:
                     # CRITICAL: Failed even to set the exception on the future
                     logger.critical(f"[Hist CB _on_historical_data_end]: CRITICAL FAILURE - Could not set exception on Future for ReqId {reqId}: {set_exc}")
            finally:
                # Cleanup adapted for Futures - Called AFTER attempting to set future
                self._cleanup_hist_req_state(req_id=reqId, log_reason="Data End Callback Finalized")
                logger.debug(f"[Hist CB _on_historical_data_end]: State cleanup executed for ReqId {reqId}.")

    def _handle_historical_error(self, reqId: int, errorCode: int, errorString: str):
         """Processes errors related to historical data requests. Sets Future exception."""
         logger.debug(f"[Hist CB _handle_historical_error]: Handling error for ReqId {reqId}, Code: {errorCode}")
         hist_symbol = "Unknown"
         req_future: Optional[concurrent.futures.Future] = None
         should_signal = False

         # Define non-retryable errors specific to historical data
         NON_RETRYABLE_HIST_CODES = {162, 200, 203, 321, 322, 354, 504}
         # 366 is handled specially (occurs after successful cancellation)

         # Create an exception object to signal the error
         error_exception = RuntimeError(f"Hist Error Code {errorCode}: {errorString}")
         setattr(error_exception, 'code', errorCode) # Attach code

         with self._hist_lock:
            # Check the future dictionary
            if reqId in self._hist_req_pending_futures:
                hist_symbol = self._hist_req_symbol_map.get(reqId, "Unknown")
                current_state = self._hist_req_state.get(reqId, "Unknown")

                if errorCode == 366:
                     # This often follows cancelHistoricalData due to timeout
                     if current_state == HIST_STATE_CANCELLED:
                          logger.warning(f"[Hist CB _handle_historical_error]: Ignoring expected Error 366 for cancelled ReqId {reqId} ({hist_symbol}).")
                          # Do not signal the future again. Cleanup might already be done.
                          self._cleanup_hist_req_state(req_id=reqId, log_reason="Expected Error 366")
                     else:
                          logger.error(f"[Hist CB _handle_historical_error]: Unexpected Error 366 for ReqId {reqId} ({hist_symbol}), state: {current_state}. Treating as error.")
                          self._hist_req_state[reqId] = HIST_STATE_ERROR
                          req_future = self._hist_req_pending_futures.get(reqId)
                          should_signal = True
                          # DO NOT CLEANUP STATE HERE - moved to finally block after signaling
                elif current_state not in [HIST_STATE_DONE, HIST_STATE_ERROR, HIST_STATE_CANCELLED]:
                     # Process other errors only if request is considered active
                     logger.error(f"[Hist CB _handle_historical_error]: Processing error for ReqId {reqId} ({hist_symbol}), Code {errorCode}: {errorString}")
                     self._hist_req_state[reqId] = HIST_STATE_ERROR
                     req_future = self._hist_req_pending_futures.get(reqId)
                     should_signal = True
                     # DO NOT CLEANUP STATE HERE - moved to finally block after signaling
                else:
                     # Error arrived for an already completed/failed request
                     logger.warning(f"[Hist CB _handle_historical_error]: Received error code {errorCode} for ReqId {reqId} ({hist_symbol}) but state was '{current_state}'. Ignoring signal.")
                     # Ensure cleanup just in case
                     self._cleanup_hist_req_state(req_id=reqId, log_reason=f"Late Error Code {errorCode} (state={current_state})")

            # else: logger.warning(f"[Hist CB _handle_historical_error]: Received error for unknown/inactive ReqId {reqId}")

         # --- Signal Error (Outside Lock, inside try...finally) ---
         if should_signal and req_future:
             try:
                 # Set the exception on the Future object
                 if not req_future.done():
                     req_future.set_exception(error_exception)
                     logger.debug(f"[Hist CB _handle_historical_error]: Exception set on Future for ReqId {reqId}.")
                 else:
                      logger.warning(f"[Hist CB _handle_historical_error]: Future for ReqId {reqId} was already done. Not setting exception.")
             except Exception as set_exc:
                 # CRITICAL: Failed even to set the exception on the future
                 logger.critical(f"[Hist CB _handle_historical_error]: CRITICAL FAILURE - Could not set exception on Future for ReqId {reqId}: {set_exc}")
             finally:
                 # Cleanup adapted for Futures - Called AFTER attempting to set future
                 self._cleanup_hist_req_state(req_id=reqId, log_reason=f"Error Code {errorCode} Callback Finalized")
                 logger.debug(f"[Hist CB _handle_historical_error]: State cleanup executed for ReqId {reqId}.")

    def _is_hist_error_non_retryable(self, error_code: int) -> bool:
         """Checks if a given historical error code suggests retrying won't help."""
         NON_RETRYABLE_CODES = {162, 200, 203, 321, 322, 354, 504}
         return error_code in NON_RETRYABLE_CODES

    def _cleanup_hist_req_state(self, req_id: int, log_reason: str = "Cleanup"):
         """Removes state associated with a completed/failed historical request (adapted for Futures)."""
         with self._hist_lock:
            removed_symbol = self._hist_req_symbol_map.pop(req_id, None)
            removed_data = self._hist_req_data.pop(req_id, None)
            # Pop from the futures dictionary
            removed_future = self._hist_req_pending_futures.pop(req_id, None)
            removed_state = self._hist_req_state.pop(req_id, None)
            # Optional: Log if something was actually removed
            # if removed_symbol or removed_data is not None or removed_future or removed_state:
            #      logger.debug(f"[Hist Cleanup] ReqId {req_id} ({removed_symbol or 'N/A'}) state removed. Reason: {log_reason}. State was: {removed_state or 'N/A'}")

    # --- _wait_for_pacing, _process_historical_df_from_bardata, _empty_dataframe, _process_historical_df ---
    # --- remain unchanged from previous version (no direct dependency on Queue/Future) ---
    def _wait_for_pacing(self, thread_name: str, symbol: str):
            """Checks rate limits and sleeps if necessary."""
            wait_time = 0.0
            limit_reached = False # Flag to track if limit was hit
            current_count = 0 # Track count for logging
            with self._hist_req_pacing_lock:
                now = time.monotonic()
                # Remove old timestamps outside the window
                while self._hist_req_timestamps and now - self._hist_req_timestamps[0] > self.hist_req_window_sec:
                    self._hist_req_timestamps.popleft()

                current_count = len(self._hist_req_timestamps) # Get current count within lock

                # Check if the limit is currently reached or exceeded
                if current_count >= self.hist_req_limit:
                    limit_reached = True # Set flag
                    # Calculate when the oldest request will expire, allowing a new one
                    time_of_oldest_allowed = self._hist_req_timestamps[0]
                    wait_time = max(0, (time_of_oldest_allowed + self.hist_req_window_sec) - now + 0.1) # Ensure non-negative, add buffer
                    if wait_time > 0: # Only log if actual wait is needed
                        logger.warning(f"[Hist Pace][{thread_name}] Rate limit reached for {symbol}. Count={current_count}/{self.hist_req_limit}. Waiting {wait_time:.2f}s...")
                    # else: logger.debug(f"[Hist Pace][{thread_name}] Limit reached for {symbol} ({current_count}/{self.hist_req_limit}) but calculated wait is {wait_time:.2f}s (likely <=0). Proceeding.")
                # else: logger.debug(f"[Hist Pace][{thread_name}] Pacing OK for {symbol}. Count={current_count}/{self.hist_req_limit}")

            # Release lock BEFORE sleeping
            if wait_time > 0 and limit_reached: # Ensure we only sleep if the limit was actually reached
                actual_start_wait = time.monotonic()
                time.sleep(wait_time)
                actual_end_wait = time.monotonic()
                logger.info(f"[Hist Pace][{thread_name}] Finished waiting for {symbol}. Actual wait: {actual_end_wait - actual_start_wait:.2f}s (requested: {wait_time:.2f}s)")

    def _process_historical_df_from_bardata(self, bars: List[BarData], symbol: str) -> Optional[pd.DataFrame]:
         """Processes a list of BarData objects into a pandas DataFrame."""
         if not bars:
              logger.warning(f"[Hist Process] No bars received for {symbol}.")
              return self._empty_dataframe() # Return empty DF, not None
         try:
             data_list = []
             parse_errors = 0
             numeric_errors = 0
             nan_price_errors = 0

             for bar in bars:
                 timestamp = None
                 # Use stricter check for epoch (e.g., length > 8 and all digits)
                 if isinstance(bar.date, str) and len(bar.date) >= 8 and bar.date.isdigit():
                     try:
                         ts = int(bar.date)
                         timestamp = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                     except (ValueError, OverflowError):
                         # logger.warning(f"Could not parse potential epoch timestamp '{bar.date}' for {symbol}. Trying other formats.")
                         timestamp = parse_ib_datetime(bar.date) # Fallback to string parser
                 else:
                     timestamp = parse_ib_datetime(str(bar.date)) # Ensure it's a string for parser

                 if timestamp is None:
                      # logger.warning(f"Could not parse date string '{bar.date}' for {symbol}")
                      parse_errors += 1
                      continue

                 try:
                    open_val = float(bar.open); high_val = float(bar.high); low_val = float(bar.low); close_val = float(bar.close)
                    # Handle potential None or non-numeric volume
                    volume_val = 0.0
                    if bar.volume is not None:
                         try:
                             vol_f = float(bar.volume)
                             if vol_f >= 0 and not math.isnan(vol_f) and not math.isinf(vol_f):
                                  volume_val = vol_f
                         except (ValueError, TypeError): pass # Keep volume as 0.0 if conversion fails

                    # Check for NaN/inf in essential price data
                    if any(math.isnan(v) or math.isinf(v) for v in [open_val, high_val, low_val, close_val]):
                        # logger.warning(f"NaN/inf price value in historical bar for {symbol} at {timestamp}. Skipped.")
                        nan_price_errors += 1
                        continue

                 except (ValueError, TypeError):
                    # logger.warning(f"Non-numeric value in historical bar for {symbol} at {timestamp}. Skipped. Data: {bar}")
                    numeric_errors += 1
                    continue

                 data_list.append({'date': timestamp, 'open': open_val, 'high': high_val, 'low': low_val, 'close': close_val, 'volume': volume_val})

             # Log aggregate errors if any occurred
             if parse_errors > 0: logger.warning(f"[{symbol}] {parse_errors}/{len(bars)} bars failed date parsing.")
             if numeric_errors > 0: logger.warning(f"[{symbol}] {numeric_errors}/{len(bars)} bars had non-numeric OHLC values.")
             if nan_price_errors > 0: logger.warning(f"[{symbol}] {nan_price_errors}/{len(bars)} bars had NaN/inf price values.")


             if not data_list:
                  logger.error(f"{symbol} historical data is empty after parsing all bars.")
                  return self._empty_dataframe()

             # Create DataFrame
             df = pd.DataFrame(data_list)
             # Convert date column and set index (handle potential coercion errors again)
             df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
             df.dropna(subset=['date'], inplace=True) # Drop rows where date conversion failed

             if df.empty:
                  logger.error(f"{symbol} historical data is empty after date conversion/dropna.")
                  return self._empty_dataframe()

             df.set_index('date', inplace=True)
             # Final processing (sorting, type checks, etc.) handled by _process_historical_df
             return self._process_historical_df(df, symbol)

         except Exception as e:
              logger.exception(f"Error processing BarData list for {symbol}: {e}")
              return self._empty_dataframe() # Return empty DF on unexpected error

    def _empty_dataframe(self) -> pd.DataFrame:
         """Creates a standard empty DataFrame for historical data."""
         required_cols = ['open', 'high', 'low', 'close', 'volume']
         index = pd.DatetimeIndex([], tz=datetime.timezone.utc, name='date') # Ensure name and TZ
         df = pd.DataFrame(columns=required_cols, index=index)
         # Ensure correct dtypes even when empty
         df = df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
         return df

    def _process_historical_df(self, df: Optional[pd.DataFrame], symbol: str) -> pd.DataFrame:
         """Ensures DataFrame has correct columns, index, dtypes, and is sorted."""
         if df is None or df.empty:
             # logger.warning(f"Processing historical data for {symbol}: Input DataFrame is None or empty.")
             return self._empty_dataframe()
         try:
             # --- Index Check ---
             if not isinstance(df.index, pd.DatetimeIndex):
                 logger.error(f"Internal error processing {symbol}: Index is not DatetimeIndex ({type(df.index)}). Attempting conversion.")
                 try: df.index = pd.to_datetime(df.index, errors='raise', utc=True)
                 except Exception as conv_err: logger.exception(f"Failed to convert index to DatetimeIndex for {symbol}: {conv_err}"); return self._empty_dataframe()
             # Ensure UTC timezone
             if df.index.tz is None: df.index = df.index.tz_localize('UTC')
             elif df.index.tz != datetime.timezone.utc: df.index = df.index.tz_convert('UTC')
             df.index.name = 'date' # Ensure index name

             # --- Column Check ---
             required_cols = ['open', 'high', 'low', 'close', 'volume']
             missing_cols = [col for col in required_cols if col not in df.columns]
             if missing_cols:
                 logger.error(f"Hist data {symbol} missing columns: {missing_cols}. Has: {list(df.columns)}. Returning empty.")
                 return self._empty_dataframe()

             # --- Data Cleaning & Sorting ---
             df = df[required_cols] # Select and order columns
             # Ensure numeric types, coercing errors (should be minimal if _from_bardata worked)
             for col in required_cols:
                 df[col] = pd.to_numeric(df[col], errors='coerce')

             initial_len = len(df)
             # Drop rows with NaN prices (essential)
             df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
             # Handle NaN volume - fill with 0 seems reasonable for calculations
             df['volume'] = df['volume'].fillna(0.0)
             # Ensure volume is non-negative
             df['volume'] = df['volume'].apply(lambda x: max(0.0, x))


             if len(df) < initial_len:
                 logger.warning(f"Dropped {initial_len - len(df)} rows with NaN prices for {symbol}.")

             if df.empty:
                 logger.warning(f"Hist data {symbol} empty after NaN price handling.")
                 return self._empty_dataframe() # Return empty if all rows had NaN prices

             # Sort by date index
             df.sort_index(inplace=True)

             # Remove duplicate index entries (e.g., from multiple data sources or errors)
             # Keep the last occurrence as it might be more up-to-date
             initial_len = len(df)
             df = df[~df.index.duplicated(keep='last')]
             if len(df) < initial_len:
                  logger.warning(f"Removed {initial_len - len(df)} duplicate index entries for {symbol}.")

             return df
         except Exception as e:
              logger.exception(f"Error processing historical DataFrame for {symbol}: {e}")
              return self._empty_dataframe()

    # --- get_latest_price unchanged ---
    def get_latest_price(self, symbol: str) -> Optional[float]:
         """Gets the last known closing price for a symbol (needs population from live/hist data)."""
         # TODO: This needs to be updated reliably, perhaps from _on_realtime_bar or end of hist data
         return self.latest_bar_close.get(symbol)

    # --- shutdown (adapted for Futures) ---
    def shutdown(self):
        """Shuts down the data handler: cancel subscriptions & cleanup."""
        logger.info("Shutting down Data Handler...")
        self._stopping = True

        # --- Cancel Pending Historical Requests ---
        req_ids_to_cancel = []
        with self._hist_lock:
             # Get futures from the correct dictionary
             req_ids_to_cancel = list(self._hist_req_pending_futures.keys())
             # Signal any waiting threads with an error
             shutdown_exception = SystemExit("Data Handler shutting down")
             for req_id, future_obj in self._hist_req_pending_futures.items():
                  try:
                      # Set exception on the future if not already done
                      if not future_obj.done():
                           future_obj.set_exception(shutdown_exception)
                  except Exception: pass # Ignore errors setting exception
             # Clear state dictionaries
             self._hist_req_pending_futures.clear() # Clear future dict
             self._hist_req_data.clear()
             self._hist_req_state.clear()
             self._hist_req_symbol_map.clear()

        if req_ids_to_cancel:
             logger.info(f"Attempting to cancel {len(req_ids_to_cancel)} pending historical requests during shutdown...")
             if self.ib_wrapper.isConnected():
                 for req_id in req_ids_to_cancel:
                     try:
                         # logger.debug(f"Shutdown: Cancelling hist request {req_id}")
                         self.ib_wrapper.cancelHistoricalData(req_id)
                         time.sleep(0.02) # Small pacing
                     except Exception as e: logger.error(f"Shutdown: Error cancelling hist reqId {req_id}: {e}")
             else: logger.warning("Shutdown: Not connected, cannot send hist data cancellations.")

        # --- Cancel Live Data Subscriptions (Unchanged) ---
        if self.ib_wrapper.isConnected():
             logger.info("Cancelling active real-time bar subscriptions...")
             req_ids_to_cancel_live = []
             with self._subscription_map_lock:
                  req_ids_to_cancel_live = list(self._subscription_req_map.keys())
                  self._subscription_req_map.clear() # Clear map immediately

             if not req_ids_to_cancel_live:
                  logger.info("No active real-time subscriptions found to cancel.")
             else:
                 cancelled_count = 0
                 for req_id in req_ids_to_cancel_live:
                     # Removed symbol lookup as map is cleared
                     try:
                         logger.debug(f"Shutdown: Cancelling realTimeBars for ReqId {req_id}...")
                         self.ib_wrapper.cancelRealTimeBars(req_id)
                         cancelled_count += 1
                         time.sleep(0.02) # Small pacing
                     except Exception as e: logger.error(f"Shutdown: Error cancelling realTimeBars ReqId {req_id}: {e}")
                 logger.info(f"Attempted cancellation for {cancelled_count} real-time subscriptions.")
        else:
             logger.info("Shutdown: Not connected, skipping real-time subscription cancellation.")
        self._subscriptions_active = False # Mark as inactive

        # --- Unregister Permanent Listeners (Unchanged) ---
        logger.debug("Unregistering permanent DataHandler listeners...")
        try:
             self.ib_wrapper.unregister_listener('contractDetails', self._on_contract_details)
             self.ib_wrapper.unregister_listener('contractDetailsEnd', self._on_contract_details_end)
             self.ib_wrapper.unregister_listener('error', self._on_error)
             self.ib_wrapper.unregister_listener('historicalData', self._on_historical_data)
             self.ib_wrapper.unregister_listener('historicalDataEnd', self._on_historical_data_end)
             self.ib_wrapper.unregister_listener('historicalDataUpdate', self._on_historical_data_update)
             # Add others if they were registered in __init__
             # self.ib_wrapper.unregister_listener('realtimeBar', self._on_realtime_bar)
        except Exception as e:
             logger.exception(f"Error unregistering permanent listeners during shutdown: {e}")

        logger.info("Data Handler shutdown complete.")