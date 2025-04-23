# filename: data/ib_handler.py
# data/ib_handler.py (Revised: Async Contract Qualification with Event Queue Signaling)
import pandas as pd
import datetime
import time
import queue
import logging
import math
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import threading
from collections import defaultdict, deque # Use defaultdict and deque

# --- Import from ibapi ---
from ibapi.contract import Contract as IbkrContract, ContractDetails
from ibapi.common import BarData

# Import core components
# MODIFICATION: Import the completion event
from core.events import MarketEvent, ShutdownEvent, ContractQualificationCompleteEvent
from core.ib_wrapper import IBWrapper, parse_ib_datetime # IBWrapper now has CallbackManager
from utils.logger import logger
# --- MODIFICATION START: Import config loader ---
from config_loader import load_config
# --- MODIFICATION END ---

# --- Helper to create IbkrContract ---
# (create_ibkr_contract function remains the same)
def create_ibkr_contract(symbol: str, sec_type_override: Optional[str] = None, exchange_override: Optional[str] = None, currency_override: Optional[str] = None) -> IbkrContract:
    """Creates an ibapi Contract object, allowing overrides for specific cases like VIX."""
    contract = IbkrContract()
    contract.symbol = symbol
    contract.currency = currency_override or 'USD'
    contract.exchange = exchange_override or 'SMART' # Default to SMART unless overridden

    if sec_type_override:
         contract.secType = sec_type_override
    elif '/' in symbol:
        parts = symbol.split('/')
        if len(parts) == 2:
            contract.symbol = parts[0]
            contract.secType = 'CASH'
            contract.currency = parts[1] # Use the second part as currency for Forex
            contract.exchange = 'IDEALPRO'
        else:
            logger.warning(f"Cannot parse Forex symbol: {symbol}. Defaulting to Stock.")
            contract.secType = 'STK'
    elif symbol.isdigit() and len(symbol) > 5:
         contract.conId = int(symbol)
         contract.exchange = exchange_override or '' # Often empty if using conId unless overridden
    else: # Assume Stock if not Forex, conId, or overridden
        contract.secType = 'STK'

    # Ensure exchange is set correctly based on secType if not overridden
    if not exchange_override:
        if contract.secType == 'IND':
            contract.exchange = 'CBOE' # Common default for indices like VIX
        elif contract.secType == 'CASH':
             contract.exchange = 'IDEALPRO'
        elif contract.secType == 'STK':
             contract.exchange = 'SMART'
        # Add other defaults as needed

    logger.debug(f"Created contract: Symbol={contract.symbol}, SecType={contract.secType}, Exch={contract.exchange}, Curr={contract.currency}")
    return contract

class IBDataHandler:
    """
    Handles connection state, contract qualification (asynchronously), market data subscriptions,
    and historical data requests via the IBWrapper and ibapi.
    Signals qualification completion via the event queue.
    """
    def __init__(self, event_q: queue.Queue, symbols: list, ib_wrapper: IBWrapper):
        if not isinstance(ib_wrapper, IBWrapper): raise TypeError("ib_wrapper must be an instance of IBWrapper")
        if not isinstance(event_q, queue.Queue): raise TypeError("event_q must be an instance of queue.Queue")
        self.ib_wrapper = ib_wrapper
        self.event_queue = event_q
        self.trading_symbols = list(set(symbols)) # Store original trading symbols
        self.latest_bar_close: Dict[str, float] = {}
        # --- MODIFICATION: Add Volatility Symbol if enabled ---
        self.volatility_symbol_details = None
        self.all_symbols_to_manage = list(self.trading_symbols) # Start with trading symbols

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
                else:
                     logger.warning("Volatility monitoring enabled but no symbol specified.")
            else:
                 logger.info("Volatility monitoring disabled in config.")

            # Load rate limit config here as well
            self.hist_req_limit = config.get('ibkr', {}).get('hist_req_limit_count', 59)
            self.hist_req_window_sec = config.get('ibkr', {}).get('hist_req_limit_window_sec', 600)
            logger.info(f"Historical data rate limit set to {self.hist_req_limit} requests per {self.hist_req_window_sec} seconds.")

        except Exception as config_exc:
             logger.exception(f"Error loading config for data handler: {config_exc}")
             # Set defaults if config fails
             self.hist_req_limit = 59
             self.hist_req_window_sec = 600

        self.symbols = self.all_symbols_to_manage # Use the combined list for internal management
        self.contracts: Dict[str, IbkrContract] = {} # Stores qualified contracts {symbol: IbkrContract}
        # --- END MODIFICATION ---

        # --- State/Tracking ---
        self._stopping = False
        self._subscriptions_active = False # For live data subscriptions

        # --- Asynchronous Contract Qualification State ---
        self._qual_lock = threading.RLock()
        self._pending_qualification_reqs: set[int] = set()
        self._qualification_results: Dict[int, List[ContractDetails]] = defaultdict(list)
        self._qualification_req_id_map: Dict[int, str] = {}
        self._qualification_failed_symbols: set[str] = set()
        # Keep the threading.Event as a simple status flag for internal checks or optional waits
        self._initial_qualification_complete = threading.Event()
        self._qualification_started = False # Flag to prevent multiple starts

        # --- Synchronous Historical Data State --- (Remains the same)
        self.historical_data_results: Dict[int, List[BarData]] = {}
        self.historical_data_status: Dict[int, str] = {}
        self._hist_data_lock = threading.Lock()

        # --- Live Data Subscription State --- (Remains the same)
        self._subscription_map_lock = threading.Lock()
        self._subscription_req_map: Dict[int, str] = {} # reqId -> symbol
        
        self.hist_req_timestamps = deque() # Stores monotonic timestamps of requests
        self._hist_req_lock = threading.Lock() # Protect access to the deque


        # --- Register Permanent Listeners for Qualification ---
        self.ib_wrapper.register_listener('contractDetails', self._on_contract_details)
        self.ib_wrapper.register_listener('contractDetailsEnd', self._on_contract_details_end)
        self.ib_wrapper.register_listener('error', self._on_error) # Generic error listener
        logger.debug("IBDataHandler initialized and registered listeners.")

    # =========================================================================
    # MODIFICATION START: Asynchronous Contract Qualification Implementation
    # =========================================================================

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

        # --- MODIFICATION: Use self.all_symbols_to_manage ---
        # Filter out symbols that are already qualified from the combined list
        unqualified_symbols = [s for s in self.all_symbols_to_manage if s not in self.contracts or not self.contracts[s].conId]

        if not unqualified_symbols:
            logger.info("All managed symbols appear to be already qualified.")
            # ... (signal completion) ...
            self._qualification_started = True
            self._signal_qualification_complete()
            return

        logger.info(f"Starting asynchronous qualification for {len(unqualified_symbols)} managed symbols...")
        self._qualification_started = True

        with self._qual_lock:
            # Reset state for this batch
            self._pending_qualification_reqs.clear()
            self._qualification_results.clear()
            self._qualification_req_id_map.clear()
            self._qualification_failed_symbols.clear()
            self._initial_qualification_complete.clear() # Ensure event is unset

            initial_pending_count = 0
            for symbol in unqualified_symbols:
                # --- MODIFICATION: Use overrides for volatility symbol ---
                sec_type_override, exchange_override, currency_override = None, None, None
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
                # --- END MODIFICATION ---
                
                if not temp_contract.symbol and not temp_contract.conId:
                    logger.error(f"Cannot request details for '{symbol}', insufficient info to create contract.")
                    self._qualification_failed_symbols.add(symbol) # Mark as failed immediately
                    continue

                req_id = self.ib_wrapper.get_next_req_id()
                self._pending_qualification_reqs.add(req_id)
                self._qualification_req_id_map[req_id] = symbol
                initial_pending_count += 1

                logger.debug(f"Async Qualify: Requesting details for {symbol} with ReqId {req_id}")
                try:
                    self.ib_wrapper.reqContractDetails(req_id, temp_contract)
                    # Optional brief pause to help avoid pacing issues, adjust/remove as needed
                    time.sleep(0.05) # e.g., 50ms between requests
                except Exception as e:
                    logger.exception(f"Async Qualify: Exception requesting details for {symbol} (ReqId {req_id}): {e}")
                    # Handle immediate failure during request submission
                    self._pending_qualification_reqs.discard(req_id)
                    self._qualification_req_id_map.pop(req_id, None)
                    self._qualification_failed_symbols.add(symbol)
                    initial_pending_count -= 1 # Adjust count as it never truly became pending

            # Check if all failed immediately or if none were actually pending
            if initial_pending_count == 0 and not self._pending_qualification_reqs:
                 # Need to release lock before signaling if _signal method acquires it
                 logger.warning("Async Qualification: No requests successfully submitted or all failed immediately.")
                 # Fall through, the lock will be released, and _signal will be called if needed by callbacks later
                 # OR signal here if absolutely sure no callbacks will arrive
                 # self._signal_qualification_complete() # Consider if safe to signal now

        # --- DO NOT BLOCK HERE ---
        if initial_pending_count > 0:
             logger.info(f"Asynchronous qualification requests submitted for {initial_pending_count} symbols.")
        else:
             # If all failed during submission loop, signal completion now
             self._signal_qualification_complete()

    # ... (_on_contract_details, _on_contract_details_end, _on_error methods remain conceptually the same) ...
    # Important: _on_contract_details_end needs to handle potential ambiguity resolution for IND type if multiple matches occur.
    # The current primary exchange logic might need adjustment for indices if they don't have a primaryExchange attribute.
    # Might need to select based on exchange='CBOE' explicitly for VIX if ambiguous.

    def _on_contract_details(self, reqId: int, contractDetails: ContractDetails):
        """Handles incoming contract details (Callback from IBWrapper)."""
        with self._qual_lock:
            # Check if this reqId belongs to our current qualification batch
            if reqId in self._pending_qualification_reqs:
                symbol = self._qualification_req_id_map.get(reqId, "Unknown")
                logger.debug(f"Async Qualify: Received contract details for ReqId {reqId} ({symbol})")
                self._qualification_results[reqId].append(contractDetails)
            # else: logger.debug(f"Received contract details for non-pending or unknown ReqId {reqId}")


    def _on_contract_details_end(self, reqId: int):
        """Handles the end signal for a contract details request (Callback from IBWrapper)."""
        with self._qual_lock:
            # Check if this reqId belongs to our current qualification batch
            if reqId in self._pending_qualification_reqs:
                symbol = self._qualification_req_id_map.get(reqId)
                logger.debug(f"Async Qualify: ContractDetailsEnd received for ReqId {reqId} ({symbol or 'Unknown'})")

                if not symbol:
                    logger.error(f"Async Qualify: Received End for unknown ReqId {reqId}. Discarding.")
                    self._pending_qualification_reqs.discard(reqId) # Remove unknown request
                else:
                    # Process the results for this symbol
                    results = self._qualification_results.pop(reqId, []) # Get results and remove entry

                    if not results:
                        # No details received before End signal
                        logger.error(f"Async Qualify: Failed qualification for {symbol} (ReqId {reqId}: No details received). Check symbol/permissions.")
                        self._qualification_failed_symbols.add(symbol)
                    elif len(results) == 1:
                        # Single unambiguous result
                        qualified_contract = results[0].contract
                        self.contracts[symbol] = qualified_contract
                        logger.info(f"Async Qualify: Contract qualified for {symbol}: ID={qualified_contract.conId}, LocalSymbol={qualified_contract.localSymbol or qualified_contract.symbol}")
                    else:
                        # Ambiguous result - try to resolve
                        logger.warning(f"Async Qualify: Ambiguous contract for {symbol} ({len(results)} matches). Trying primary exchange...")
                        primary_found = None
                        primary_exchanges = {'NASDAQ', 'NYSE', 'ARCA', 'AMEX', 'BATS'} # Common US stock exchanges
                        for detail in results:
                            # Use primaryExchange attribute if available, fallback to exchange
                            primary_exch = getattr(detail.contract, 'primaryExchange', detail.contract.exchange)
                            if primary_exch in primary_exchanges:
                                if primary_found:
                                     logger.warning(f"Async Qualify: Multiple primary exchange matches for {symbol}. Sticking with first found ({primary_found.conId}).")
                                     break # Stick with the first one found
                                primary_found = detail.contract
                        if primary_found:
                            self.contracts[symbol] = primary_found
                            logger.info(f"Async Qualify: Selected primary exchange contract for {symbol}: ID={primary_found.conId}, Exch={getattr(primary_found, 'primaryExchange', primary_found.exchange)}")
                        else:
                            logger.error(f"Async Qualify: Could not determine primary contract for ambiguous {symbol}. Skipping.")
                            self._qualification_failed_symbols.add(symbol)

                    # --- Request housekeeping ---
                    self._pending_qualification_reqs.discard(reqId) # Mark this request as done
                    self._qualification_req_id_map.pop(reqId, None) # Clean up map

                # --- Check if all requests are complete ---
                if not self._pending_qualification_reqs:
                    self._signal_qualification_complete() # Signal completion
            # else: logger.debug(f"Received ContractDetailsEnd for non-pending or unknown ReqId {reqId}")


    def _on_error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        """Handles errors that might be related to contract qualification requests."""
        # Check if the error is relevant to qualification
        symbol = None
        is_qual_error = False
        with self._qual_lock:
            if reqId in self._pending_qualification_reqs:
                is_qual_error = True
                symbol = self._qualification_req_id_map.get(reqId, "Unknown")
                logger.error(f"Async Qualify: Error during qualification for {symbol} (ReqId {reqId}): Code {errorCode} - {errorString}")
                # Assume failure for this symbol on any error during qualification phase
                self._qualification_failed_symbols.add(symbol)
                self._qualification_results.pop(reqId, None) # Discard any partial results
                self._pending_qualification_reqs.discard(reqId)
                self._qualification_req_id_map.pop(reqId, None)

                # Check if all requests are now complete (due to this error completing one)
                if not self._pending_qualification_reqs:
                    should_signal = True
                else:
                    should_signal = False
            else:
                 should_signal = False # Error not related to pending qualification

        # Signal outside the lock if needed
        if is_qual_error and should_signal:
             self._signal_qualification_complete()


    def _signal_qualification_complete(self):
        """Internal: Signals that the qualification process is finished. Sets event and puts msg on queue."""
        # This method might be called from multiple places (_on_contract_details_end, _on_error, start_async...)
        # Ensure it only signals *once* per qualification attempt using the threading.Event state
        if self._initial_qualification_complete.is_set():
             # Already signaled for this attempt
             return

        logger.info("Async Contract Qualification: Finalizing results...")
        with self._qual_lock:
            if self._qualification_failed_symbols:
                 logger.error(f"Removing symbols that failed qualification: {sorted(list(self._qualification_failed_symbols))}")
                 # Update the main symbol list AND the contracts dict
                 self.symbols = [s for s in self.symbols if s not in self._qualification_failed_symbols]
                 # Also remove from the original trading symbols list if needed by portfolio? Maybe not necessary here.
                 self.trading_symbols = [s for s in self.trading_symbols if s not in self._qualification_failed_symbols]
                 for s in self._qualification_failed_symbols:
                     self.contracts.pop(s, None)
            # Log which volatility symbol failed if it did
            if self.volatility_symbol_details and self.volatility_symbol_details['symbol'] in self._qualification_failed_symbols:
                 logger.error(f"Volatility monitoring symbol '{self.volatility_symbol_details['symbol']}' failed qualification.")
                 self.volatility_symbol_details = None # Disable it
        
        logger.info(f"Async Contract Qualification process complete. Active symbols: {len(self.symbols)}")
        # --- Signal Completion ---
        # 1. Set the internal threading.Event (useful for quick status checks)
        self._initial_qualification_complete.set()

        # 2. Put the notification event onto the main event queue (Option A)
        completion_event = ContractQualificationCompleteEvent(
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            successful_symbols=sorted(list(self.contracts.keys())),
            failed_symbols=sorted(list(self._qualification_failed_symbols))
        )
        try:
             self.event_queue.put(completion_event, block=True, timeout=2)
             logger.info(f"Put {type(completion_event).__name__} onto event queue.")
        except Exception as e: logger.exception(f"Error putting {type(completion_event).__name__} onto queue: {e}")

    def is_qualification_complete(self) -> bool:
         """Checks if the initial qualification process has completed."""
         # Can be called without lock as Event.is_set() is thread-safe
         return self._initial_qualification_complete.is_set()

    # =========================================================================
    # MODIFICATION END: Asynchronous Contract Qualification Implementation
    # =========================================================================

    # --- subscribe_live_data --- (Requires check for qualification complete)
    def subscribe_live_data(self) -> bool:
         """Subscribes to live market data (5-second bars)."""
         if self._stopping: return False
         if not self.ib_wrapper.isConnected(): logger.warning("Cannot subscribe, not connected."); return False
         # MODIFICATION: Check the qualification complete flag/event
         if not self.is_qualification_complete():
              logger.warning("Cannot subscribe live data, contract qualification not yet complete.")
              return False
         # --- End Modification ---
         if self._subscriptions_active: logger.info("Live data subscriptions already marked active."); return True
         if not self.contracts: logger.warning("No qualified contracts to subscribe to."); self._subscriptions_active = True; return True # No contracts, so technically done.

         # --- MODIFICATION: Subscribe to all qualified contracts (including VIX) ---
         logger.info(f"Requesting 5-second bar subscriptions for {len(self.contracts)} qualified contracts...")
         # --- The existing loop iterates through self.contracts, which now includes VIX if qualified ---
         subscribed_count = 0
         failed_symbols = []
         for symbol, contract in self.contracts.items(): # Iterates over all qualified contracts
             # ... (rest of the subscription logic remains the same) ...
             symbol_already_subscribed = False
             with self._subscription_map_lock:
                  if symbol in self._subscription_req_map.values():
                       logger.debug(f"Subscription for {symbol} seems already active locally.")
                       symbol_already_subscribed = True
             if symbol_already_subscribed: subscribed_count += 1; continue

             if not contract or not contract.conId: logger.warning(f"Skipping {symbol}, invalid contract."); continue
             req_id = self.ib_wrapper.get_next_req_id()
             logger.debug(f"Requesting realTimeBars for {symbol} (SecType: {contract.secType}) with ReqId {req_id}")
             try:
                 self.ib_wrapper.reqRealTimeBars(
                     reqId=req_id, contract=contract, barSize=5, whatToShow="TRADES", # Use TRADES for VIX too? Or MIDPOINT? Check data needs.
                     useRTH=False, realTimeBarsOptions=[]
                 )
                 with self._subscription_map_lock: self._subscription_req_map[req_id] = symbol
                 time.sleep(0.1) # Keep brief pause
                 subscribed_count += 1
             except Exception as e:
                 logger.error(f"Failed subscription for {symbol} (ReqId {req_id}): {e}", exc_info=True)
                 failed_symbols.append(symbol)
         # ... (logging results) ...
         if subscribed_count > 0: logger.info(f"Initiated/confirmed {subscribed_count} subscriptions.")
         if failed_symbols: logger.error(f"Failed subscriptions for: {failed_symbols}")
         self._subscriptions_active = subscribed_count > 0 or not self.contracts
         return self._subscriptions_active
         # --- END MODIFICATION ---

    # --- request_historical_data_sync --- (Requires check/trigger for qualification)
    def request_historical_data_sync(self, symbol: str, duration='1 Y', bar_size='1 day',
                                 what_to_show='TRADES', use_rth=True,
                                 end_dt: Optional[datetime.datetime | str] = None,
                                 max_retries=2, timeout_per_req=60.0) -> Optional[pd.DataFrame]:
        """Requests historical bar data synchronously with retries and smarter error handling."""
        if self._stopping: return self._empty_dataframe()

        # --- MODIFICATION: Handle potential VIX request with specific contract ---
        contract = self.contracts.get(symbol)
        if not contract or not contract.conId:
            # If it's the volatility symbol, try creating the contract on the fly if not qualified
            # This is a fallback, qualification should ideally happen first.
            is_vol_symbol = self.volatility_symbol_details and symbol == self.volatility_symbol_details['symbol']
            if is_vol_symbol:
                 logger.warning(f"Volatility symbol {symbol} not found in qualified contracts. Attempting dynamic creation for historical request.")
                 details = self.volatility_symbol_details
                 contract = create_ibkr_contract(symbol, details['sec_type'], details['exchange'], details['currency'])
            else:
                  # ... (existing logic for missing non-volatility contracts) ...
                 if not self.is_qualification_complete():
                      logger.error(f"Cannot request historical data for {symbol}: Initial contract qualification not yet complete.")
                      return self._empty_dataframe()
                 else:
                      logger.error(f"Cannot request historical data for {symbol}: Contract not qualified/found.")
                      return self._empty_dataframe()
        # --- END MODIFICATION ---
       
        end_dt_str = ""
        if isinstance(end_dt, datetime.datetime):
            if end_dt.tzinfo is None: end_dt = end_dt.replace(tzinfo=datetime.timezone.utc)
            else: end_dt = end_dt.astimezone(datetime.timezone.utc)
            end_dt_str = end_dt.strftime("%Y%m%d %H:%M:%S UTC")
        elif isinstance(end_dt, str): end_dt_str = end_dt
        
        # --- Rate Limiting Check (Existing logic from previous step is good) ---
        wait_time = 0.0
        with self._hist_req_lock:
             now = time.monotonic()
             while self.hist_req_timestamps and now - self.hist_req_timestamps[0] > self.hist_req_window_sec:
                  self.hist_req_timestamps.popleft()
             if len(self.hist_req_timestamps) >= self.hist_req_limit:
                  wait_time = (self.hist_req_timestamps[0] + self.hist_req_window_sec) - now + 0.1
                  logger.warning(f"Historical data request limit ({self.hist_req_limit}/{self.hist_req_window_sec}s) reached. Waiting {wait_time:.2f}s before requesting {symbol}...")
        if wait_time > 0:
             time.sleep(wait_time)
        # --- End Rate Limiting Check ---

        logger.info(f"Requesting historical {bar_size} bars for {symbol}, duration {duration}, end {end_dt_str or 'Now'}...")
        # ** MODIFICATION START: Enhanced Error Handling / Retry **
        # Define critical error codes that should prevent retries immediately
        # (See IBKR API Error Codes documentation for more)
        NON_RETRYABLE_ERROR_CODES = {
            162,  # Historical Market Data Service error query message: Pacing violation
            200,  # No security definition has been found for the request
            203,  # The security definition is not available for the specified criteria.
            321,  # Error validating request:-'hm':- Please enter a valid data type
            322,  # Error processing request:No market data permissions
            354,  # Not subscribed to requested market data
            502,  # Couldn't connect to TWS. Confirm that "Enable ActiveX..." is checked...
            504,  # Not connected to TWS
            10167, # Requested market data is not subscribed. Displaying delayed market data... (may not be fatal depending on need)
            1100, # Connectivity between IB and Trader Workstation has been lost.
            1101, # Connectivity between Trader Workstation and IB has been restored data lost.
            1102, # Connectivity between Trader Workstation and IB has been restored data unchanged.
            # Add other codes deemed non-retryable for your use case
        }
        # ** MODIFICATION END **

        for attempt in range(max_retries):
            if not self.ib_wrapper.isConnected():
                logger.error("Cannot request historical data, not connected.")
                return self._empty_dataframe()

            req_id = self.ib_wrapper.get_next_req_id()
            request_event = threading.Event()
            request_error: Optional[str] = None
            bar_list: List[BarData] = []
            local_status = 'pending'
            # ** MODIFICATION START: Added should_retry flag **
            should_retry = True # Assume retry is possible unless a critical error occurs
            # ** MODIFICATION END **


            # --- Define Listener Functions ---
            def sync_historicalData(reqId_cb, bar):
                nonlocal bar_list
                if reqId_cb == req_id: bar_list.append(bar)

            def sync_historicalDataEnd(reqId_cb, start, end):
                nonlocal local_status
                if reqId_cb == req_id:
                    logger.debug(f"Sync Hist: HistoricalDataEnd received for {symbol} (ReqId {req_id})")
                    local_status = 'done'
                    request_event.set() # Signal completion

            # ** MODIFICATION START: Enhanced sync_error **
            def sync_error(reqId_cb, errorCode, errorString, advancedOrderRejectJson=""):
                nonlocal request_error, local_status, should_retry # Added should_retry
                if reqId_cb == req_id:
                    logger.error(f"Sync Hist: Error for {symbol} (ReqId {req_id}): Code {errorCode} - {errorString}")
                    request_error = f"Code {errorCode}: {errorString}"
                    local_status = 'error'

                    # Check if this error code should prevent retries
                    if errorCode in NON_RETRYABLE_ERROR_CODES:
                        logger.critical(f"Non-retryable error encountered (Code: {errorCode}). Aborting retries for this request.")
                        should_retry = False # Prevent further retries for this specific request

                    # Always set the event on error to stop the wait()
                    request_event.set()
            # ** MODIFICATION END **


            # --- Register Listeners ---
            self.ib_wrapper.register_listener('historicalData', sync_historicalData)
            self.ib_wrapper.register_listener('historicalDataEnd', sync_historicalDataEnd)
            self.ib_wrapper.register_listener('error', sync_error)

            try:
                with self._hist_data_lock:
                    self.historical_data_status[req_id] = local_status

                # --- MODIFICATION START: Record timestamp AFTER successful submission attempt ---
                # Place the request
                self.ib_wrapper.reqHistoricalData(
                    reqId=req_id, contract=contract, endDateTime=end_dt_str, durationStr=duration,
                    barSizeSetting=bar_size, whatToShow=what_to_show, useRTH=1 if use_rth else 0,
                    formatDate=1, keepUpToDate=False, chartOptions=[]
                )
                # Record timestamp *after* the request call succeeds without exception
                with self._hist_req_lock:
                    self.hist_req_timestamps.append(time.monotonic())
                # --- MODIFICATION END ---

                # Wait for completion (event set by End or Error) or timeout
                request_completed = request_event.wait(timeout=timeout_per_req)

                if not request_completed:
                    # Timeout occurred
                    logger.error(f"Timeout waiting for historical data for {symbol} (ReqId {req_id}) attempt {attempt+1}.")
                    request_error = "Timeout"
                    # ** MODIFICATION START: Don't necessarily prevent retry on timeout **
                    # should_retry = True # Allow timeout to potentially retry (default)
                    # ** MODIFICATION END **
                    try:
                        self.ib_wrapper.cancelHistoricalData(req_id)
                    except Exception as cancel_e:
                        logger.error(f"Error cancelling hist request {req_id} after timeout: {cancel_e}")

                elif request_error:
                    # Error occurred and was handled by sync_error
                    logger.error(f"Request failed for {symbol} (ReqId {req_id}) due to error: {request_error}")
                    # should_retry flag was set within sync_error based on the errorCode

                else: # Success (historicalDataEnd was received)
                    logger.info(f"Received {len(bar_list)} historical bars for {symbol} (ReqId {req_id}) attempt {attempt+1}")
                    # Clean up listeners immediately
                    self.ib_wrapper.unregister_listener('historicalData', sync_historicalData)
                    self.ib_wrapper.unregister_listener('historicalDataEnd', sync_historicalDataEnd)
                    self.ib_wrapper.unregister_listener('error', sync_error)
                    with self._hist_data_lock: self.historical_data_status.pop(req_id, None)
                    # Process and return the data
                    return self._process_historical_df_from_bardata(bar_list, symbol)

            except Exception as e:
                logger.exception(f"Exception during historical request submission/wait for {symbol} attempt {attempt+1}: {e}")
                request_error = str(e)
                should_retry = False # Don't retry on unexpected code exceptions

            finally:
                # Ensure listeners are always unregistered for this req_id
                self.ib_wrapper.unregister_listener('historicalData', sync_historicalData)
                self.ib_wrapper.unregister_listener('historicalDataEnd', sync_historicalDataEnd)
                self.ib_wrapper.unregister_listener('error', sync_error)
                with self._hist_data_lock: self.historical_data_status.pop(req_id, None)

            # --- Retry Logic ---
            # ** MODIFICATION START: Check should_retry flag **
            if not should_retry:
                 logger.error(f"Aborting retries for {symbol} (ReqId {req_id}) due to non-retryable error or exception.")
                 break
            if attempt < max_retries - 1:
                 wait_time_retry = 2 ** (attempt + 1)
                 logger.info(f"Waiting {wait_time_retry}s before retrying historical data for {symbol} (Attempt {attempt+2}/{max_retries})...")
                 time.sleep(wait_time_retry)
            else:
                 logger.error(f"Max retries ({max_retries}) reached for {symbol}. Last error: {request_error or 'N/A'}")

        # If loop finishes without returning data (due to errors/retries failing)
        logger.error(f"Failed to retrieve historical data for {symbol} after {max_retries} attempts.")
        return self._empty_dataframe()

    # --- MODIFICATION: Add getter for latest close price (used by PortfolioManager) ---
    def get_latest_close_price(self, symbol: str) -> Optional[float]:
         """Gets the last known closing price for a symbol from real-time bars."""
         return self.latest_bar_close.get(symbol)
    # --- END MODIFICATION ---

    # --- _process_historical_df_from_bardata, _empty_dataframe, _process_historical_df ---
    # (These helper methods remain the same)
    def _process_historical_df_from_bardata(self, bars: List[BarData], symbol: str) -> pd.DataFrame:
         """Processes a list of BarData objects into a pandas DataFrame."""
         if not bars: return self._empty_dataframe()
         try:
             data_list = []
             for bar in bars:
                 timestamp = None
                 if bar.date.isdigit():
                     try: timestamp = datetime.datetime.fromtimestamp(int(bar.date), tz=datetime.timezone.utc)
                     except ValueError: logger.warning(f"Could not parse epoch timestamp '{bar.date}' for {symbol}"); continue
                 else:
                     timestamp = parse_ib_datetime(bar.date)
                     if timestamp is None: logger.warning(f"Could not parse date string '{bar.date}' for {symbol}"); continue
                 try:
                    open_val = float(bar.open); high_val = float(bar.high); low_val = float(bar.low); close_val = float(bar.close)
                    volume_val = float(bar.volume) if bar.volume is not None and float(bar.volume) >= 0 else 0.0
                    if any(math.isnan(v) for v in [open_val, high_val, low_val, close_val, volume_val]): logger.warning(f"NaN value in historical bar for {symbol} at {timestamp}. Skipped."); continue
                 except (ValueError, TypeError): logger.warning(f"Non-numeric value in historical bar for {symbol} at {timestamp}. Skipped. Data: {bar}"); continue
                 data_list.append({'date': timestamp, 'open': open_val, 'high': high_val, 'low': low_val, 'close': close_val, 'volume': volume_val})
             if not data_list: return self._empty_dataframe()
             df = pd.DataFrame(data_list); df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True); df.dropna(subset=['date'], inplace=True)
             if df.empty: logger.warning(f"{symbol} hist data empty after date parse."); return self._empty_dataframe()
             df.set_index('date', inplace=True); return self._process_historical_df(df, symbol)
         except Exception as e: logger.exception(f"Error processing BarData list for {symbol}: {e}"); return self._empty_dataframe()
    def _empty_dataframe(self) -> pd.DataFrame: required_cols = ['open', 'high', 'low', 'close', 'volume']; index = pd.to_datetime([]).tz_localize('UTC'); return pd.DataFrame(columns=required_cols, index=index).astype(float)
    def _process_historical_df(self, df: Optional[pd.DataFrame], symbol: str) -> pd.DataFrame:
         if df is None or df.empty: return self._empty_dataframe()
         try:
             if not isinstance(df.index, pd.DatetimeIndex): logger.error(f"Internal error: Non-DatetimeIndex for {symbol}"); return self._empty_dataframe()
             if df.index.tz is None: df.index = df.index.tz_localize('UTC')
             elif df.index.tz != datetime.timezone.utc: df.index = df.index.tz_convert('UTC')
             required_cols = ['open', 'high', 'low', 'close', 'volume']; missing_cols = [col for col in required_cols if col not in df.columns]
             if missing_cols: logger.error(f"Hist data {symbol} missing: {missing_cols}. Has: {list(df.columns)}"); return self._empty_dataframe()
             df = df[required_cols].sort_index(); df = df.astype(float) # Ensure numeric types after selection
             initial_len = len(df); df.dropna(inplace=True) # Drop rows with NaN in any column
             if len(df) < initial_len: logger.warning(f"Dropped {initial_len - len(df)} NaN rows for {symbol}.")
             if df.empty: logger.warning(f"Hist data {symbol} empty after NaN handling.")
             df = df[~df.index.duplicated(keep='last')]; return df
         except Exception as e: logger.exception(f"Error processing historical DataFrame for {symbol}: {e}"); return self._empty_dataframe()

    # --- get_latest_price --- (Remains the same)
    def get_latest_price(self, symbol: str) -> Optional[float]:
         last_close = self.latest_bar_close.get(symbol)
         # Fallback might need adjustment if self.symbol_data is not populated
         # if last_close is None:
         #     data_df = self.symbol_data.get(symbol) # Where is self.symbol_data defined/populated? Assumed missing.
         #     if data_df is not None and not data_df.empty:
         #         try: last_close = data_df['close'].iloc[-1]
         #         except IndexError: pass
         return last_close

    # --- shutdown --- (Needs to unregister permanent listeners)
    def shutdown(self):
        """Shuts down the data handler: unregister listeners & cancel subscriptions."""
        logger.info("Shutting down Data Handler...")
        self._stopping = True

        # --- MODIFICATION: Unregister permanent listeners ---
        try:
             self.ib_wrapper.unregister_listener('contractDetails', self._on_contract_details)
             self.ib_wrapper.unregister_listener('contractDetailsEnd', self._on_contract_details_end)
             self.ib_wrapper.unregister_listener('error', self._on_error)
             logger.debug("Unregistered permanent DataHandler listeners.")
        except Exception as e:
             logger.exception(f"Error unregistering permanent listeners: {e}")
        # --- End Modification ---

        # Cancel live data subscriptions (existing logic)
        if self.ib_wrapper.isConnected():
             logger.info("Cancelling active real-time bar subscriptions...")
             req_ids_to_cancel = []
             with self._subscription_map_lock:
                  req_ids_to_cancel = list(self._subscription_req_map.keys())

             if not req_ids_to_cancel: logger.info("No active real-time subscriptions found to cancel.")
             else:
                 cancelled_count = 0
                 for req_id in req_ids_to_cancel:
                     # Skip low req_ids (heuristic for non-market data reqs)
                     # This heuristic might be too broad, consider if req_id > some_base_value
                     if req_id <= 10: continue

                     symbol = "Unknown"
                     with self._subscription_map_lock:
                         symbol = self._subscription_req_map.get(req_id, f"ReqId_{req_id}")

                     try:
                         logger.debug(f"Cancelling realTimeBars for {symbol} (ReqId {req_id})...")
                         self.ib_wrapper.cancelRealTimeBars(req_id)
                         # Remove mapping from local map after cancellation attempt
                         with self._subscription_map_lock:
                             self._subscription_req_map.pop(req_id, None)
                         cancelled_count += 1
                     except Exception as e:
                         logger.error(f"Error cancelling realTimeBars for {symbol} (ReqId {req_id}): {e}")
                         # Optionally remove from map even if cancel fails?
                         # with self._subscription_map_lock: self._subscription_req_map.pop(req_id, None)
                 logger.info(f"Attempted cancellation for {cancelled_count} real-time subscriptions.")
             self._subscriptions_active = False
        else: logger.info("Not connected, skipping real-time subscription cancellation.")
        logger.info("Data Handler shutdown complete.")
