# filename: data/ib_handler.py
# data/ib_handler.py (Revised: Use CallbackManager, Thread-safe map access)
import pandas as pd
import datetime
import time
import asyncio
import queue
import logging
import math
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
import threading

# --- Import from ibapi ---
from ibapi.contract import Contract as IbkrContract
from ibapi.common import BarData

# Import core components
from core.events import MarketEvent, ShutdownEvent
from core.ib_wrapper import IBWrapper, parse_ib_datetime # IBWrapper now has CallbackManager
from utils.logger import logger

# --- Helper to create IbkrContract ---
def create_ibkr_contract(symbol: str) -> IbkrContract:
    """Creates an ibapi Contract object."""
    contract = IbkrContract()
    contract.currency = 'USD'
    contract.exchange = 'SMART'
    if '/' in symbol: # Basic check for Forex
        parts = symbol.split('/')
        if len(parts) == 2:
            contract.symbol = parts[0]
            contract.secType = 'CASH'
            contract.currency = parts[1]
            contract.exchange = 'IDEALPRO'
        else:
            logger.warning(f"Cannot parse Forex symbol: {symbol}. Defaulting to Stock.")
            contract.symbol = symbol
            contract.secType = 'STK'
    elif symbol.isdigit() and len(symbol) > 5: # Basic check for conId
         contract.conId = int(symbol)
         # contract.exchange = '' # Exchange should often be empty if using conId, but SMART might work
    else: # Assume Stock
        contract.symbol = symbol
        contract.secType = 'STK'
    return contract

class IBDataHandler:
    """
    Handles connection state, contract qualification, market data subscriptions,
    and historical data requests via the IBWrapper and ibapi.
    Uses IBWrapper's CallbackManager for sync operations.
    """
    def __init__(self, event_q: queue.Queue, symbols: list, ib_wrapper: IBWrapper):
        if not isinstance(ib_wrapper, IBWrapper): raise TypeError("ib_wrapper must be an instance of IBWrapper")
        if not isinstance(event_q, queue.Queue): raise TypeError("event_q must be an instance of queue.Queue")
        self.ib_wrapper = ib_wrapper
        self.event_queue = event_q
        self.symbols = list(set(symbols))
        self.contracts: Dict[str, IbkrContract] = {} # Stores qualified contracts {symbol: IbkrContract}
        self.latest_bar_close: Dict[str, float] = {}

        # State/Tracking
        self._initial_qualification_complete = threading.Event()
        self._subscriptions_active = False
        self._stopping = False

        # For synchronous historical requests
        self.historical_data_results: Dict[int, List[BarData]] = {}
        self.historical_data_status: Dict[int, str] = {}
        self._hist_data_lock = threading.Lock() # Lock for historical data results/status

        # --- Defer initial qualification ---
        # self._qualify_contracts_sync()

    def start_initial_sync(self):
        """Initiates the synchronous contract qualification."""
        self._qualify_contracts_sync()

    def _qualify_contracts_sync(self):
         """Qualifies contracts synchronously (blocks until done or timeout) using listener registration."""
         if not self.ib_wrapper.isConnected():
             logger.error("Cannot qualify contracts, IB wrapper not connected.")
             self._initial_qualification_complete.set()
             return False
         logger.info(f"Qualifying contracts for {len(self.symbols)} symbols (synchronous)...")

         unqualified_symbols = [s for s in self.symbols if s not in self.contracts or not self.contracts[s].conId]
         if not unqualified_symbols:
             logger.info("All symbols appear to be already qualified.")
             self._initial_qualification_complete.set()
             return True

         details_received: Dict[int, List[Any]] = {}
         req_id_map: Dict[int, str] = {}
         expected_ends = set()
         details_event = threading.Event()
         listener_tag = f"qualify_{id(self)}" # Unique tag for these listeners

         # --- Define Listener Functions ---
         def sync_contractDetails(reqId, contractDetails):
             nonlocal details_received
             if reqId in req_id_map:
                 if reqId not in details_received: details_received[reqId] = []
                 details_received[reqId].append(contractDetails)
                 logger.debug(f"Sync Qualify: Received contract details for ReqId {reqId}")

         def sync_contractDetailsEnd(reqId):
             nonlocal expected_ends, details_event
             if reqId in req_id_map:
                 logger.debug(f"Sync Qualify: ContractDetailsEnd received for ReqId {reqId}")
                 expected_ends.discard(reqId)
                 if not expected_ends:
                     details_event.set()

         # --- Register Listeners ---
         self.ib_wrapper.register_listener('contractDetails', sync_contractDetails)
         self.ib_wrapper.register_listener('contractDetailsEnd', sync_contractDetailsEnd)

         # --- Request Details ---
         try:
             for symbol in unqualified_symbols:
                 temp_contract = create_ibkr_contract(symbol)
                 if not temp_contract.symbol and not temp_contract.conId:
                     logger.error(f"Cannot request details for '{symbol}', insufficient info.")
                     continue
                 req_id = self.ib_wrapper.get_next_req_id()
                 req_id_map[req_id] = symbol
                 expected_ends.add(req_id)
                 logger.debug(f"Requesting contract details for {symbol} with ReqId {req_id}")
                 try:
                    self.ib_wrapper.reqContractDetails(req_id, temp_contract)
                 except Exception as e:
                    logger.exception(f"Error requesting contract details for {symbol} (ReqId {req_id}): {e}")
                    expected_ends.discard(req_id) # Remove from expected if request fails

             # --- Wait for Completion ---
             timeout = 30.0
             logger.info(f"Waiting up to {timeout}s for contract details...")
             all_details_received = details_event.wait(timeout=timeout)

             if not all_details_received:
                 logger.error(f"Timeout waiting for contract details after {timeout}s. Expected ends remaining: {expected_ends}")

             # --- Process Results ---
             qualified_count = 0
             symbols_to_remove = []
             for symbol in unqualified_symbols:
                 req_id_found = None
                 for r_id, s in req_id_map.items():
                     if s == symbol:
                         req_id_found = r_id
                         break
                 if req_id_found is None: continue

                 results = details_received.get(req_id_found, [])
                 if len(results) == 1:
                     qualified_contract = results[0].contract
                     self.contracts[symbol] = qualified_contract
                     logger.info(f"Contract qualified for {symbol}: ID={qualified_contract.conId}, LocalSymbol={qualified_contract.localSymbol or qualified_contract.symbol}")
                     qualified_count += 1
                 elif len(results) > 1:
                     logger.warning(f"Ambiguous contract for {symbol} ({len(results)} matches). Trying primary...")
                     primary_found = None
                     for detail in results:
                         primary_exch = getattr(detail.contract, 'primaryExchange', None)
                         if primary_exch in ['NASDAQ', 'NYSE', 'ARCA', 'AMEX', 'BATS']:
                              primary_found = detail.contract
                              break
                     if primary_found:
                          self.contracts[symbol] = primary_found
                          logger.info(f"Selected primary exchange contract for {symbol}: ID={primary_found.conId}, Exch={primary_found.primaryExchange}")
                          qualified_count += 1
                     else:
                          logger.error(f"Could not determine primary contract for ambiguous {symbol}. Skipping.")
                          symbols_to_remove.append(symbol)
                 elif req_id_found not in expected_ends: # End received, but no details
                     logger.error(f"Failed qualification for {symbol} (ReqId {req_id_found}: No details received). Check symbol/permissions.")
                     symbols_to_remove.append(symbol)

             # Handle symbols that timed out
             timed_out_symbols = [req_id_map[req_id] for req_id in expected_ends if req_id in req_id_map]
             if timed_out_symbols:
                 logger.error(f"Contract detail requests timed out or did not complete for: {timed_out_symbols}")
                 symbols_to_remove.extend(timed_out_symbols)

             # Update symbol list
             if symbols_to_remove:
                 logger.error(f"Removing symbols that failed qualification: {symbols_to_remove}")
                 self.symbols = [s for s in self.symbols if s not in symbols_to_remove]
                 for s in symbols_to_remove: self.contracts.pop(s, None)

             logger.info(f"Contract qualification complete. Qualified {qualified_count}/{len(unqualified_symbols)} requested. Active symbols: {len(self.symbols)}")
             self._initial_qualification_complete.set()
             return qualified_count > 0 or len(self.symbols) > 0 # Return true if any qualified or none needed qualification

         finally:
             # --- Unregister Listeners ---
             self.ib_wrapper.unregister_listener('contractDetails', sync_contractDetails)
             self.ib_wrapper.unregister_listener('contractDetailsEnd', sync_contractDetailsEnd)

    def wait_for_contract_qualification(self, timeout=30.0) -> bool:
         """Blocks until initial contract qualification is complete or timeout."""
         logger.info(f"Waiting up to {timeout}s for contract qualification signal...")
         return self._initial_qualification_complete.wait(timeout=timeout)

    def subscribe_live_data(self) -> bool:
         """Subscribes to live market data (5-second bars)."""
         if self._stopping: return False
         if not self.ib_wrapper.isConnected(): logger.warning("Cannot subscribe, not connected."); return False
         if not self._initial_qualification_complete.is_set(): logger.warning("Cannot subscribe, contracts not qualified/ready."); return False
         if self._subscriptions_active: logger.info("Live data subscriptions already marked active."); return True
         if not self.contracts: logger.warning("No qualified contracts to subscribe."); self._subscriptions_active = True; return True

         logger.info(f"Requesting 5-second bar subscriptions for {len(self.contracts)} contracts...")
         subscribed_count = 0
         failed_symbols = []

         for symbol, contract in self.contracts.items():
             # Check wrapper's mapping for existing subscription
             symbol_already_subscribed = False
             # Need to iterate safely if wrapper modifies map concurrently (though unlikely here)
             # Use thread-safe getter/iteration if wrapper map is modified by IB thread
             with self.ib_wrapper._req_map_lock: # Use lock for read iteration
                  for req_id, sub_symbol in self.ib_wrapper.req_id_to_symbol.items():
                      if sub_symbol == symbol and req_id > 10: # Basic check
                           symbol_already_subscribed = True
                           logger.debug(f"Subscription for {symbol} seems already active (ReqId {req_id}).")
                           break
             if symbol_already_subscribed:
                  subscribed_count += 1
                  continue

             if not contract or not contract.conId: logger.warning(f"Skipping {symbol}, invalid contract."); continue

             req_id = self.ib_wrapper.get_next_req_id()
             self.ib_wrapper.add_req_id_symbol_mapping(req_id, symbol) # Use thread-safe method
             logger.debug(f"Requesting realTimeBars for {symbol} with ReqId {req_id}")
             try:
                 self.ib_wrapper.reqRealTimeBars(
                     reqId=req_id, contract=contract, barSize=5, whatToShow="TRADES",
                     useRTH=False, realTimeBarsOptions=[]
                 )
                 time.sleep(0.1) # Brief pause
                 subscribed_count += 1
             except Exception as e:
                 logger.error(f"Failed subscription for {symbol} (ReqId {req_id}): {e}", exc_info=True)
                 self.ib_wrapper.remove_req_id_symbol_mapping(req_id) # Use thread-safe method
                 failed_symbols.append(symbol)

         if subscribed_count > 0: logger.info(f"Initiated/confirmed {subscribed_count} subscriptions.")
         if failed_symbols: logger.error(f"Failed subscriptions for: {failed_symbols}")

         self._subscriptions_active = subscribed_count > 0 or not self.contracts
         return self._subscriptions_active

    def request_historical_data_sync(self, symbol: str, duration='1 Y', bar_size='1 day',
                                 what_to_show='TRADES', use_rth=True,
                                 end_dt: Optional[datetime.datetime | str] = None,
                                 max_retries=2, timeout_per_req=60.0) -> Optional[pd.DataFrame]:
        """Requests historical bar data synchronously with retries using listeners."""
        if self._stopping: return self._empty_dataframe()

        contract = self.contracts.get(symbol)
        if not contract or not contract.conId:
             logger.warning(f"Contract for {symbol} not qualified. Attempting sync qualification...")
             # Run qualification - this blocks until done/timeout
             if not self._qualify_contracts_sync():
                 logger.error(f"Cannot request historical data: Re-qualification failed for {symbol}")
                 return self._empty_dataframe()
             # Re-check contract
             contract = self.contracts.get(symbol)
             if not contract or not contract.conId:
                 logger.error(f"Cannot request historical data: Contract still not qualified for {symbol} after re-attempt.")
                 return self._empty_dataframe()

        # Format endDateTime string
        end_dt_str = ""
        if isinstance(end_dt, datetime.datetime):
            # Ensure timezone-aware UTC
            if end_dt.tzinfo is None: end_dt = end_dt.replace(tzinfo=datetime.timezone.utc)
            else: end_dt = end_dt.astimezone(datetime.timezone.utc)
            end_dt_str = end_dt.strftime("%Y%m%d %H:%M:%S UTC")
        elif isinstance(end_dt, str): end_dt_str = end_dt

        logger.info(f"Requesting historical {bar_size} bars for {symbol}, duration {duration}, end {end_dt_str or 'Now'}...")

        for attempt in range(max_retries):
            if not self.ib_wrapper.isConnected():
                logger.error("Cannot request historical data, not connected.")
                return self._empty_dataframe()

            req_id = self.ib_wrapper.get_next_req_id()
            request_event = threading.Event()
            request_error: Optional[str] = None
            bar_list: List[BarData] = []
            local_status = 'pending' # Local status tracking

            # --- Define Listener Functions ---
            def sync_historicalData(reqId_cb, bar):
                nonlocal bar_list
                if reqId_cb == req_id: bar_list.append(bar)

            def sync_historicalDataEnd(reqId_cb, start, end):
                nonlocal local_status
                if reqId_cb == req_id:
                    logger.debug(f"Sync Hist: HistoricalDataEnd received for {symbol} (ReqId {req_id})")
                    local_status = 'done'
                    request_event.set()

            def sync_error(reqId_cb, errorCode, errorString):
                nonlocal request_error, local_status
                # Note: Cannot call original error easily from here without passing it in.
                # Rely on wrapper's main error handler for logging.
                if reqId_cb == req_id:
                    logger.error(f"Sync Hist: Error for {symbol} (ReqId {req_id}): Code {errorCode} - {errorString}")
                    failure_codes = {162, 200, 320, 321, 354, 502, 504, 10167, 1100, 1101, 1102}
                    if errorCode in failure_codes:
                        request_error = f"Code {errorCode}: {errorString}"
                        local_status = 'error'
                        request_event.set()

            # --- Register Listeners ---
            self.ib_wrapper.register_listener('historicalData', sync_historicalData)
            self.ib_wrapper.register_listener('historicalDataEnd', sync_historicalDataEnd)
            self.ib_wrapper.register_listener('error', sync_error)

            try:
                with self._hist_data_lock: # Lock status dictionary
                    self.historical_data_status[req_id] = local_status
                # Submit the request
                self.ib_wrapper.reqHistoricalData(
                    reqId=req_id, contract=contract, endDateTime=end_dt_str, durationStr=duration,
                    barSizeSetting=bar_size, whatToShow=what_to_show, useRTH=1 if use_rth else 0,
                    formatDate=1, keepUpToDate=False, chartOptions=[]
                )

                # Wait for completion or timeout
                if not request_event.wait(timeout=timeout_per_req):
                    logger.error(f"Timeout waiting for historical data for {symbol} (ReqId {req_id}) attempt {attempt+1}.")
                    request_error = "Timeout"
                    try: self.ib_wrapper.cancelHistoricalData(req_id)
                    except Exception as cancel_e: logger.error(f"Error cancelling hist request {req_id}: {cancel_e}")
                elif request_error:
                    logger.error(f"Request failed for {symbol} (ReqId {req_id}): {request_error}")
                else: # Success
                    logger.info(f"Received {len(bar_list)} historical bars for {symbol} (ReqId {req_id}) attempt {attempt+1}")
                    # Unregister listeners *before* processing data
                    self.ib_wrapper.unregister_listener('historicalData', sync_historicalData)
                    self.ib_wrapper.unregister_listener('historicalDataEnd', sync_historicalDataEnd)
                    self.ib_wrapper.unregister_listener('error', sync_error)
                    with self._hist_data_lock: self.historical_data_status.pop(req_id, None)
                    return self._process_historical_df_from_bardata(bar_list, symbol)

            except Exception as e:
                logger.exception(f"Exception submitting historical request for {symbol} attempt {attempt+1}: {e}")
                request_error = str(e)
            finally:
                # --- Unregister Listeners (ensure cleanup) ---
                self.ib_wrapper.unregister_listener('historicalData', sync_historicalData)
                self.ib_wrapper.unregister_listener('historicalDataEnd', sync_historicalDataEnd)
                self.ib_wrapper.unregister_listener('error', sync_error)
                with self._hist_data_lock: self.historical_data_status.pop(req_id, None)

            # --- Retry Logic ---
            if attempt < max_retries - 1:
                unrecoverable_errors = ["pacing violation", "permissions", "No security definition", "HMDS subscription", "HMDS connection", "unknown contract"]
                if request_error and any(e.lower() in request_error.lower() for e in unrecoverable_errors):
                    logger.critical(f"Unrecoverable error for {symbol}: '{request_error}'. Aborting request.")
                    return self._empty_dataframe()
                wait_time = 2 ** (attempt + 1)
                logger.info(f"Waiting {wait_time}s before retrying historical data for {symbol}...")
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries ({max_retries}) reached for {symbol}. Last error: {request_error}")

        return self._empty_dataframe()


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
                    open_val = float(bar.open)
                    high_val = float(bar.high)
                    low_val = float(bar.low)
                    close_val = float(bar.close)
                    volume_val = float(bar.volume) if bar.volume is not None and float(bar.volume) >= 0 else 0.0
                    if any(math.isnan(v) for v in [open_val, high_val, low_val, close_val, volume_val]):
                        logger.warning(f"NaN value in historical bar for {symbol} at {timestamp}. Skipped."); continue
                 except (ValueError, TypeError):
                     logger.warning(f"Non-numeric value in historical bar for {symbol} at {timestamp}. Skipped. Data: {bar}"); continue
                 data_list.append({'date': timestamp, 'open': open_val, 'high': high_val, 'low': low_val, 'close': close_val, 'volume': volume_val})
             if not data_list: return self._empty_dataframe()
             df = pd.DataFrame(data_list)
             if df.empty: return self._empty_dataframe()
             df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
             df.dropna(subset=['date'], inplace=True)
             if df.empty: logger.warning(f"{symbol} hist data empty after date parse."); return self._empty_dataframe()
             df.set_index('date', inplace=True)
             return self._process_historical_df(df, symbol)
         except Exception as e:
             logger.exception(f"Error processing BarData list for {symbol}: {e}")
             return self._empty_dataframe()

    def _empty_dataframe(self) -> pd.DataFrame:
        """Creates an empty DataFrame with the expected structure."""
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        index = pd.to_datetime([]).tz_localize('UTC')
        return pd.DataFrame(columns=required_cols, index=index).astype(float)

    def _process_historical_df(self, df: Optional[pd.DataFrame], symbol: str) -> pd.DataFrame:
         """Ensures the DataFrame has the correct structure, index, and types."""
         if df is None or df.empty: return self._empty_dataframe()
         try:
             if not isinstance(df.index, pd.DatetimeIndex): logger.error(f"Internal error: Non-DatetimeIndex for {symbol}"); return self._empty_dataframe()
             if df.index.tz is None: df.index = df.index.tz_localize('UTC')
             elif df.index.tz != datetime.timezone.utc: df.index = df.index.tz_convert('UTC')
             required_cols = ['open', 'high', 'low', 'close', 'volume']
             missing_cols = [col for col in required_cols if col not in df.columns]
             if missing_cols: logger.error(f"Hist data {symbol} missing: {missing_cols}. Has: {list(df.columns)}"); return self._empty_dataframe()
             df = df[required_cols].sort_index()
             for col in required_cols: df[col] = pd.to_numeric(df[col], errors='coerce')
             initial_len = len(df)
             df.dropna(subset=['open', 'high', 'low', 'close', 'volume'], inplace=True)
             if len(df) < initial_len: logger.warning(f"Dropped {initial_len - len(df)} NaN rows for {symbol}.")
             if df.empty: logger.warning(f"Hist data {symbol} empty after NaN handling.")
             df = df[~df.index.duplicated(keep='last')]
             return df
         except Exception as e:
             logger.exception(f"Error processing historical DataFrame for {symbol}: {e}")
             return self._empty_dataframe()

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Gets the latest available closing price for a symbol from stored bars."""
        # Get from latest_bar_close dictionary which should be updated by market events
        # Or directly access the last row of the stored dataframe if needed as fallback
        last_close = self.latest_bar_close.get(symbol)
        if last_close is None:
            data_df = self.symbol_data.get(symbol)
            if data_df is not None and not data_df.empty:
                try:
                    last_close = data_df['close'].iloc[-1]
                except IndexError:
                    pass # Empty dataframe
        return last_close


    def shutdown(self):
        """Shuts down the data handler by cancelling subscriptions."""
        logger.info("Shutting down Data Handler...")
        self._stopping = True
        if self.ib_wrapper.isConnected():
             logger.info("Cancelling active real-time bar subscriptions...")
             req_ids_to_cancel = []
             # Safely get keys from wrapper's map
             with self.ib_wrapper._req_map_lock:
                  req_ids_to_cancel = list(self.ib_wrapper.req_id_to_symbol.keys())

             if not req_ids_to_cancel: logger.info("No active subscriptions found.")
             else:
                 cancelled_count = 0
                 for req_id in req_ids_to_cancel:
                     if req_id <= 10: continue # Skip low req_ids (heuristic)
                     symbol = self.ib_wrapper.get_symbol_for_req_id(req_id) or f"ReqId_{req_id}"
                     try:
                         logger.debug(f"Cancelling realTimeBars for {symbol} (ReqId {req_id})...")
                         self.ib_wrapper.cancelRealTimeBars(req_id)
                         self.ib_wrapper.remove_req_id_symbol_mapping(req_id) # Remove mapping
                         cancelled_count += 1
                     except Exception as e:
                         logger.error(f"Error cancelling realTimeBars for {symbol} (ReqId {req_id}): {e}")
                 logger.info(f"Attempted cancellation for {cancelled_count} subscriptions.")
             self._subscriptions_active = False
        else: logger.info("Not connected, skipping subscription cancellation.")
        logger.info("Data Handler shutdown complete.")