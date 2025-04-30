# filename: core/ib_wrapper.py
# core/ib_wrapper.py
import logging
import threading
import time
import queue
import datetime
from typing import Optional, Callable, Dict, List
from concurrent.futures import ThreadPoolExecutor

try:                     # Python 3.9+: stdlib zoneinfo
    from zoneinfo import ZoneInfo
except ImportError:      # <3.9 – will treat tz as naive
    ZoneInfo = None      # type: ignore

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.common import OrderId
from ibapi.connection import Connection # Needed for disconnect check

logger = logging.getLogger("TradingSystem")


class IBWrapper(EWrapper, EClient):
    """Combined EWrapper/EClient with robustness helpers."""
    # --------------------------------------------------
    # construction
    # --------------------------------------------------
    def __init__(self, inbound_queue: Optional[queue.Queue] = None) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)          # type: ignore

        self._lock                    = threading.RLock()
        self._client_thread: Optional[threading.Thread]   = None
        self._watchdog_thread: Optional[threading.Thread] = None

        # Create a thread pool for running listener callbacks asynchronously
        # Adjust max_workers based on expected load and listener complexity
        self._listener_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix='IBWrapperListener')
        logger.info(f"Initialized listener executor with max_workers={self._listener_executor._max_workers}")

        self._inside_error_handler = False

        self._reconnect_enabled       = True
        self._reconnect_attempt       = 0
        self._max_reconnect_delay     = 300           # 5 min cap

        self._host: str               = "127.0.0.1"
        self._port: int               = 7497
        self._client_id: int          = 1

        self.connection_active        = False
        self.connected_event          = threading.Event()

        self.next_valid_order_id: Optional[OrderId] = None
        self._next_req_id: int        = 1

        # ---------------------------------------------------------------- #
        # Request‑ID helpers expected by data/ib_handler.py                #
        # ---------------------------------------------------------------- #
        self._req_id_lock      = threading.Lock()          # used by get_next_req_id
        self.req_id_to_symbol: Dict[int, str] = {}         # shared tracking map

        self.inbound_queue            = inbound_queue
        self._callback_chains: Dict[str, List[Callable]] = {}

        # ---------------------------------------------------------------- #
        # Back‑compat public helpers expected by other modules             # <<<
        # ---------------------------------------------------------------- #
        self._req_map_lock = threading.RLock()      # used by IBDataHandler # <<<

    def _run_listener_safely(self, func: Callable, *args, **kwargs):
        """Executes a listener function in a try-except block to catch errors."""
        try:
            func(*args, **kwargs)
        except Exception:
            logger.exception(f"Exception caught in background listener thread executing {func.__name__}")

    # ------------------------------------------------------------------ #
    # compatibility wrapper around add_callback                          # <<<
    # ------------------------------------------------------------------ #
    def register_listener(self, name: str, func: Callable) -> None:      # <<<
        """Alias kept for older components (maps to add_callback)."""     # <<<
        self.add_callback(name, func)                                     # <<<
                                                                          # <<<
    def unregister_listener(self, name: str, func: Callable) -> None:     # <<<
        """Remove *func* from the callback chain of *name* (noop if absent)."""  # <<<
        lst = self._callback_chains.get(name)                             # <<<
        if lst and func in lst:                                           # <<<
            try:                                                          # <<< FIX: Add try-except
                lst.remove(func)                                          # <<<
            except ValueError:                                            # <<< FIX: Handle case where func might already be removed
                logger.debug(f"Listener {name} func {func} already removed, skipping unregister.") # <<<
            if not lst:                                                   # <<<
                try:                                                      # <<< FIX: Add try-except
                    del self._callback_chains[name]                       # <<<
                except KeyError:                                          # <<< FIX: Handle case where key might already be removed
                    logger.debug(f"Callback chain {name} already removed, skipping delete.") # <<<

    # --------------------------------------------------
    # public helpers
    # --------------------------------------------------
    def add_callback(self, name: str, func: Callable) -> None:
        """Register *func* to be called after the built‑in callback *name*."""
        with self._lock: # FIX: Ensure thread-safe modification
            if func not in self._callback_chains.setdefault(name, []):
                 self._callback_chains.setdefault(name, []).append(func)
            else:
                 logger.debug(f"Listener {name} func {func} already registered.")

    # ------------------------------------------------------------------ #
    # back‑compat helper – monotonic request‑id generator                #
    # ------------------------------------------------------------------ #
    def get_next_req_id(self) -> int:
        """Return a new, thread‑safe incremental request ID."""
        with self._req_id_lock:
            req_id = self._next_req_id
            self._next_req_id += 1
            return req_id

    # --------------------------------------------------
    # connection management
    # --------------------------------------------------
    def start_connection(self,
                         host: str = "127.0.0.1",
                         port: int = 7497,
                         client_id: int = 1) -> None:
        """Open the socket and start the network + watchdog threads."""
        with self._lock:
            if self.connection_active:
                logger.info("start_connection() – already connected.")
                return

            self._host, self._port, self._client_id = host, port, client_id
            logger.info(f"Connecting to IB Gateway/TWS {host}:{port} (client‑id={client_id}) …")

            # FIX: Reset connection state before connecting
            self.connection_active = False
            self.connected_event.clear()

            try:
                self.connect(host, port, client_id)
            except Exception as e:
                 logger.exception(f"Connection attempt failed during EClient.connect: {e}")
                 self.stop_connection() # Ensure cleanup if connect fails
                 raise ConnectionError(f"Failed to connect to IB: {e}")

            # ---- network thread ---------------------------------------
            if self._client_thread is None or not self._client_thread.is_alive():
                self._client_thread = threading.Thread(
                    target=self.run,
                    name="IBAPI‑NetworkThread",
                    daemon=True
                )
                self._client_thread.start()
            else:
                 logger.warning("start_connection: Network thread already running?")

            # ---- wait for nextValidId ---------------------------------
            if not self.connected_event.wait(timeout=15):
                logger.error("Timed‑out waiting for nextValidId – aborting connect.")
                self.stop_connection()
                raise ConnectionError("Could not establish IB connection (nextValidId timeout).")

            # ---- watchdog ---------------------------------------------
            if self._watchdog_thread is None or not self._watchdog_thread.is_alive():
                self._reconnect_enabled = True # Ensure reconnect is enabled on new connection start
                self._watchdog_thread = threading.Thread(
                    target=self._watchdog_loop,
                    name="IBAPI‑Watchdog",
                    daemon=True
                )
                self._watchdog_thread.start()
            else:
                 logger.warning("start_connection: Watchdog thread already running?")

            # Check connection state again after waiting for nextValidId
            if self.isConnected():
                self.connection_active = True
                logger.info("IB connection established.")
            else:
                logger.error("Connection check failed *after* nextValidId received/timeout.")
                self.stop_connection()
                raise ConnectionError("IB Connection failed post-nextValidId.")

    # ---- FIX: MODIFIED stop_connection ----
    def stop_connection(self) -> None:
        """Gracefully close the socket, shutdown executor, and join helper threads."""
        with self._lock:
            # Signal threads to stop first
            self._reconnect_enabled = False
            # connection_active is set to False *after* disconnect attempt or in connectionClosed

            if not self.isConnected():
                logger.info("stop_connection: Already disconnected.")
                # Ensure flags are set even if already disconnected
                self.connection_active = False
                self.connected_event.clear()
            else:
                logger.info("Disconnecting from IB (stop_connection)...")
                try:
                    # Call the now idempotent disconnect method
                    self.disconnect()
                except Exception as exc:
                    logger.warning(f"Exception during disconnect() call in stop_connection: {exc}", exc_info=True)
                finally:
                    # Ensure flags are cleared even if disconnect had issues
                    self.connection_active = False
                    self.connected_event.clear()

        logger.debug("stop_connection: Shutting down listener executor...")
        # Shutdown non-blockingly, don't wait for pending listeners to finish during shutdown
        self._listener_executor.shutdown(wait=False, cancel_futures=True) # cancel_futures added in Python 3.9
        logger.debug("stop_connection: Listener executor shutdown initiated.")

        # Join threads *outside* the lock
        logger.debug("stop_connection: Joining client thread...")
        if self._client_thread and self._client_thread.is_alive():
            self._client_thread.join(timeout=5)
            if self._client_thread.is_alive():
                 logger.warning("Client thread did not join within timeout.")
        logger.debug("stop_connection: Joining watchdog thread...")
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            # No need to explicitly stop watchdog, _reconnect_enabled=False handles it
            self._watchdog_thread.join(timeout=1) # Shorter timeout is fine
            if self._watchdog_thread.is_alive():
                 logger.warning("Watchdog thread did not join within timeout.")

        logger.info("IB connection shutdown sequence complete.")

    # ---- FIX: ADDED Idempotent Disconnect Method ----
    def disconnect(self):
        """Disconnects from TWS/Gateway. Safe to call multiple times."""
        # Check the actual socket connection state if possible
        # self.conn is the socket object in EClient
        is_really_connected = isinstance(self.conn, Connection) and self.conn.isConnected()

        if not is_really_connected:
            # logger.debug("disconnect() called but already disconnected (socket check). Skipping.")
            # Still ensure flags are consistent
            self.connection_active = False
            self.connected_event.clear()
            return

        logger.info("Disconnecting from IB socket...")
        try:
            # Call the original EClient disconnect logic
            # Directly call base class method if possible, otherwise replicate key parts
            if hasattr(super(), 'disconnect'):
                 super().disconnect()
            else:
                 # Replicate necessary parts of EClient.disconnect if super() call isn't feasible
                 # This is fragile if the base class changes.
                 if isinstance(self.conn, Connection):
                     self.conn.disconnect()
                 # self.reset() # Call reset if necessary, based on EClient source
                 logger.info("Manual EClient disconnect logic executed.")

        except Exception as e:
            logger.exception(f"Exception during EClient disconnect logic: {e}")
        finally:
             # Ensure state is updated regardless of exceptions during disconnect
             self.connection_active = False
             self.connected_event.clear()
             # Reset internal state if necessary (check EClient.reset() or EClient.disconnect())
             # self.reset()

    # --------------------------------------------------
    # watchdog logic
    # --------------------------------------------------
    def _watchdog_loop(self) -> None:
        """Reconnect automatically if the socket drops unexpectedly."""
        logger.info("IBAPI Watchdog thread started.")
        while self._reconnect_enabled:
            if not self.isConnected():
                 # Check if we are *supposed* to be connected
                 # Use self.connection_active flag which is managed during connect/disconnect
                 if self.connection_active: # We think we should be connected, but aren't
                      logger.warning("Watchdog: Lost connection to IB – attempting reconnect …")
                      self.connection_active = False # Mark as inactive until reconnected
                      self.connected_event.clear()
                      self._attempt_reconnect()
                 # else: # We are not supposed to be connected (e.g., during shutdown), do nothing
                 #    pass
            time.sleep(5) # Check every 5 seconds
        logger.info("IBAPI Watchdog thread stopped.")

    def _attempt_reconnect(self) -> None:
        # Check again if reconnect is still enabled, might have been disabled during wait
        if not self._reconnect_enabled:
            logger.info("Reconnect aborted as reconnect is now disabled.")
            return

        delay = min(2 ** self._reconnect_attempt, self._max_reconnect_delay)
        logger.info(f"Waiting {delay}s before reconnect attempt #{self._reconnect_attempt + 1} …")
        time.sleep(delay)

        # Check *again* before attempting connection
        if not self._reconnect_enabled:
            logger.info("Reconnect aborted as reconnect is now disabled (post-wait).")
            return
        if self.isConnected():
             logger.info("Reconnect aborted, connection re-established during wait.")
             self.connection_active = True # Mark active again
             self.connected_event.set() # Ensure event is set
             self._reconnect_attempt = 0
             return

        try:
            logger.info(f"Attempting reconnect #{self._reconnect_attempt + 1}...")
            # Use the main start_connection method which handles thread creation etc.
            self.start_connection(self._host, self._port, self._client_id)
            # If start_connection succeeds, it sets flags correctly
            logger.info(f"Reconnect attempt #{self._reconnect_attempt + 1} successful.")
            self._reconnect_attempt = 0                 # success
        except Exception as exc:
            logger.error(f"Reconnect attempt #{self._reconnect_attempt + 1} failed: {exc}")
            self._reconnect_attempt += 1
            # No need to call stop_connection here, start_connection handles its own cleanup on failure

    # --------------------------------------------------
    # required IB callbacks
    # --------------------------------------------------
    def nextValidId(self, orderId: OrderId):
        """Callback receiving the next valid order ID at connection time."""
        logger.info(f"nextValidId received ⇒ {orderId}")
        self.next_valid_order_id = orderId
        # Only set connected_event if we are not already marked as active?
        # Or just always set it, start_connection waits for it anyway.
        self.connected_event.set()

        # Dispatch to listeners
        listeners = self._callback_chains.get("nextValidId", [])
        for fn in listeners[:]: # Iterate copy in case listener modifies list
            try:
                fn(orderId)
            except Exception as exc:
                logger.exception(f"User nextValidId hook {fn} raised: {exc}")

    def connectionClosed(self):
        """Callback invoked when TWS/Gateway closes the socket connection."""
        logger.warning("IB connectionClosed() callback fired.")
        # Set flags to indicate connection is down
        self.connection_active = False
        self.connected_event.clear()

        # Dispatch to listeners
        listeners = self._callback_chains.get("connectionClosed", [])
        for fn in listeners[:]: # Iterate copy
            try:
                fn()
            except Exception as exc:
                logger.exception(f"User connectionClosed hook {fn} raised: {exc}")

        # Watchdog thread will handle the reconnect logic if enabled

    def error(self, *args):
        """Callback receiving error messages from TWS/Gateway.
           Includes re-entry guard, robust parsing, classification,
           and non-blocking listener dispatch.
        """
        # --- 1. Re-entry Guard Check ---
        # Check if attribute exists first for robustness during init/shutdown
        if hasattr(self, '_inside_error_handler') and self._inside_error_handler:
            # logger.debug("Re-entry detected in error handler, skipping.") # Optional debug log
            return
        # Ensure flag exists if called very early/late (should be set by __init__)
        if not hasattr(self, '_inside_error_handler'):
            self._inside_error_handler = False

        try:
            # --- 2. Set the Guard Flag ---
            self._inside_error_handler = True

            # --- 3. ALL CORE LOGIC (Parsing, Classification, Logging, Dispatch) ---

            # --- REVISED PARSING LOGIC (Handles known signatures and fallback) ---
            reqId = errorCode = -1
            errorString = advancedJson = ""
            errorTime = None
            extra_args = None
            arg_len = len(args)
            # logger.debug(f"DEBUG: Raw error args received (len={arg_len}): {args}")

            try:
                # --- Explicit Signature Handling ---
                if arg_len >= 6:
                    reqId, errorTime, errorCode, errorString, advancedJson = args[:5]
                    extra_args = args[5:]
                    # logger.debug(f"Parsed as new 6+ arg signature. Extra args: {extra_args}")
                elif arg_len == 5:
                    reqId, errorTime, errorCode, errorString, advancedJson = args
                    # logger.debug("Parsed as new 5-arg signature.")
                elif arg_len == 4:
                    reqId, errorCode, errorString, advancedJson = args
                    # logger.debug("Parsed as old 4-arg signature.")
                elif arg_len == 3:
                    reqId, errorCode, errorString = args
                    # logger.debug("Parsed as old 3-arg signature.")
                else:
                    # --- Safer Fallback Parsing ---
                    logger.warning(f"Unhandled error tuple length ({arg_len}). Attempting fallback parsing: {args}")
                    ints_found = [a for a in args if isinstance(a, int)]
                    strs_found = [a for a in args if isinstance(a, str)]
                    plausible_codes = [i for i in ints_found if 100 <= i <= 6000]
                    if plausible_codes:
                        errorCode = plausible_codes[0]
                        remaining_ints = [i for i in ints_found if i != errorCode]
                        if -1 in remaining_ints: reqId = -1
                        elif remaining_ints: reqId = remaining_ints[0]
                        else: reqId = -1
                    elif ints_found:
                        if -1 in ints_found: reqId = -1
                        else: reqId = ints_found[0]
                        errorCode = -1
                    else: reqId = errorCode = -1
                    if len(strs_found) >= 1: errorString = strs_found[0]
                    if len(strs_found) >= 2: advancedJson = " ".join(strs_found[1:])

            except Exception as parse_exc:
                logger.exception(f"Exception during argument parsing in error callback: {args} -> {parse_exc}")
                # --- Safer Fallback Parsing on Exception ---
                reqId = errorCode = -1; errorString = advancedJson = ""; errorTime = None; extra_args = None
                try:
                    ints_found = [a for a in args if isinstance(a, int)]
                    strs_found = [a for a in args if isinstance(a, str)]
                    plausible_codes = [i for i in ints_found if 100 <= i <= 6000]
                    if plausible_codes: errorCode = plausible_codes[0]
                    remaining_ints = [i for i in ints_found if i != errorCode]
                    if -1 in remaining_ints: reqId = -1
                    elif remaining_ints: reqId = remaining_ints[0]
                    if len(strs_found) >= 1: errorString = strs_found[0]
                    if len(strs_found) >= 2: advancedJson = " ".join(strs_found[1:])
                except Exception as fallback_exc: logger.error(f"Exception during fallback parsing: {fallback_exc}")
            # --- END OF REVISED PARSING LOGIC ---

            # --- Classification Logic (Uses correctly parsed errorCode) ---
            INFO_CODES = { 2104, 2106, 2108, 2158, 2103, 2105, 2107, 2119, 2100, 2150, 2109 }
            WARNING_CODES = { 366, 1101, 2110, 10000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10010, 10011, 10012, 10013, 10014, 10015, 10016, 10017 } # 366: Expected after cancel/timeout?
            CRITICAL_CODES = { 502, 1300 }
            default_level = logging.ERROR
            level = default_level

            if errorCode in INFO_CODES: level = logging.INFO
            elif errorCode in WARNING_CODES: level = logging.WARNING
            elif errorCode in CRITICAL_CODES: level = logging.CRITICAL
            elif 1100 <= errorCode <= 1102:
                if errorCode == 1100:
                    level = logging.ERROR
                    if hasattr(self, 'connection_active'): self.connection_active = False
                    if hasattr(self, 'connected_event'): self.connected_event.clear()
                elif errorCode == 1101: level = logging.WARNING
                elif errorCode == 1102:
                    level = logging.INFO
                    if hasattr(self, 'connection_active'): self.connection_active = True
            elif errorCode == 504:
                level = logging.ERROR
                if hasattr(self, 'connection_active'): self.connection_active = False
                if hasattr(self, 'connected_event'): self.connected_event.clear()

            # --- Construct Log Message ---
            level_name = logging.getLevelName(level)
            log_prefix = f"IB-{level_name:<8}"
            time_str = f" Time: {errorTime}" if errorTime else ""
            log_msg = f"{log_prefix}{time_str} Code={errorCode} | ReqId={reqId}: {errorString}"
            if advancedJson: log_msg += f" | RejectInfo: {advancedJson}"
            if extra_args: log_msg += f" | ExtraArgs: {extra_args}"

            # --- Log the message ---
            logger.log(level, log_msg)

            # --- Dispatch to Listeners (Use ThreadPoolExecutor) ---
            listeners = self._callback_chains.get("error", [])
            if listeners:
                for fn in listeners[:]:
                    try:
                        # Submit helper which calls listener with optional errorTime kwarg
                        self._listener_executor.submit(
                            self._run_listener_safely,
                            fn,
                            reqId,
                            errorCode,
                            errorString,
                            advancedJson, # Pass the parsed variable
                            errorTime=errorTime # Pass errorTime as keyword
                        )
                    except Exception as submit_exc:
                        logger.exception(f"Failed to submit listener {fn.__name__} to executor: {submit_exc}")
            # --- END Dispatch ---
       
        finally:
            # --- 4. Reset the Guard Flag ---
            self._inside_error_handler = False

    # ==================================================
    # Utility – parse IB date/time strings (Unchanged)
    # ==================================================
    _IB_DT_FORMATS = [
        "%Y%m%d %H:%M:%S %Z",     # 20250103 14:30:00 EST
        "%Y%m%d %H:%M:%S",        # 20250103 14:30:00
        "%Y%m%d",                 # 20250103
        "%Y-%m-%d %H:%M:%S.%f",   # 2025-01-03 14:30:00.000123
        "%Y-%m-%d %H:%M:%S",      # 2025-01-03 14:30:00
        "%Y%m%d-%H:%M:%S",        # 20250103-14:30:00
        "%Y%m%d %H:%M:%S%z",      # 20250103 14:30:00+0000
    ]

# ------------------------------------------------------------------ #
# Chain every IB callback to user‑registered listeners.              #
# This wraps the original method (which might be a no‑op stub) so    #
# both the original behaviour *and* listeners run.                   #
# ------------------------------------------------------------------ #

_IB_CALLBACK_NAMES = [
    # Add historical data callbacks here
    "historicalData", "historicalDataEnd", "historicalDataUpdate",
    # contract / market‑data
    "contractDetails", "contractDetailsEnd", "realtimeBar", # Added realtimeBar
    "tickPrice", "tickSize", "tickString", "tickGeneric", # Added tick data
    "marketDataType", # Added market data type

    # orders / executions
    "openOrder", "openOrderEnd", "orderStatus",
    "execDetails", "execDetailsEnd", "commissionReport", # Added execDetailsEnd

    # account / portfolio
    "updateAccountValue", "updatePortfolio", "updateAccountTime",
    "accountDownloadEnd", "position", "positionEnd",
    "accountSummary", "accountSummaryEnd", # Added account summary

    # Others often used
    "managedAccounts", "scannerParameters", "scannerData", "scannerDataEnd",
    "verifyMessageAPI", "verifyCompleted", "verifyAndAuthMessageAPI", "verifyAndAuthCompleted",
    "displayGroupList", "displayGroupUpdated",
    "positionMulti", "positionMultiEnd", "accountUpdateMulti", "accountUpdateMultiEnd",
    "securityDefinitionOptionalParameter", "securityDefinitionOptionalParameterEnd",
    "softDollarTiers",
    "familyCodes", "symbolSamples",
    "mktDepth", "mktDepthL2", "updateMktDepth", "updateMktDepthL2",
    "tickOptionComputation",
    "tickSnapshotEnd", "marketRule",
    "pnl", "pnlSingle",
    "historicalTicks", "historicalTicksBidAsk", "historicalTicksLast",
    "tickByTickAllLast", "tickByTickBidAsk", "tickByTickMidPoint",
    "orderBound", "completedOrder", "completedOrdersEnd",
    "replaceFAEnd",
    "wshMetaData", "wshEventData",
    "historicalSchedule",
    "userInfo",
]

# Use introspection to find all EWrapper methods to avoid manual list
import inspect
_WRAPPER_METHODS = [name for name, func in inspect.getmembers(EWrapper, inspect.isfunction)
                    if not name.startswith("_") and name != 'python_do_handshake'] # Add any other internal methods to exclude

def _install_chain_method(cls, name):
    original = getattr(cls, name, None)
    if original is None and name in _WRAPPER_METHODS: # If it's a known EWrapper method but maybe not implemented in EClient
        # Provide a default stub that just calls listeners
        def _stub_wrapper(self, *args, **kwargs):
             # forward to listeners
             listeners = self._callback_chains.get(name, [])
             # logger.debug(f"Stub wrapper called for {name} with {len(listeners)} listeners")
             for fn in listeners[:]: # Iterate copy
                try:
                    fn(*args, **kwargs)
                except Exception:
                    logger.exception(f"Listener {fn} for {name} raised")
        _stub_wrapper.__name__ = name
        setattr(cls, name, _stub_wrapper)

    elif callable(original): # If method exists in base class (EClient likely)
        # Wrap the original method
        def _wrapper(self, *args, **kwargs):
            # call the original (if any)
            try:
                original(self, *args, **kwargs)
            except Exception as e:
                 logger.exception(f"Original EWrapper callback {name} raised: {e}")


            # forward to listeners
            listeners = self._callback_chains.get(name, [])
            # logger.debug(f"Chain wrapper called for {name} with {len(listeners)} listeners")
            for fn in listeners[:]: # Iterate copy
                try:
                    fn(*args, **kwargs)
                except Exception:
                    logger.exception(f"Listener {fn} for {name} raised")

        _wrapper.__name__ = name
        setattr(cls, name, _wrapper)
    # else: logger.debug(f"Skipping method chaining for {name}, not callable or not found.")

# Install wrappers dynamically for all known EWrapper methods
for _cb_name in _WRAPPER_METHODS:
    _install_chain_method(IBWrapper, _cb_name)
# Manually add any methods missed by introspection if necessary
# _install_chain_method(IBWrapper, 'some_missed_callback')

def parse_ib_datetime(dt_str: str) -> Optional[datetime.datetime]:
    """
    Convert the many different date/time strings emitted by IB into a
    `datetime` object (timezone‑aware where possible).

    Returns **None** if parsing fails.
    """
    if not dt_str:
        return None

    # Handle Unix timestamp first (often used for historical data)
    if dt_str.isdigit():
        try:
            ts = int(dt_str)
            # Check for reasonable range if needed (e.g., > 1970)
            return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        except (OverflowError, ValueError):
            pass # Fall through to string formats if not a valid timestamp

    # Then try the known explicit string formats
    for fmt in IBWrapper._IB_DT_FORMATS:
        try:
            dt = datetime.datetime.strptime(dt_str, fmt)
            # If the format included timezone, make sure it's UTC or convert
            if dt.tzinfo is not None:
                 if dt.tzinfo != datetime.timezone.utc:
                      return dt.astimezone(datetime.timezone.utc)
                 else:
                      return dt # Already UTC
            else:
                 # Assume TWS timezone (often EST/EDT) if no tz info and ZoneInfo available
                 # THIS IS RISKY - IB can send different formats. UTC is safer if possible.
                 # Defaulting to UTC if format has no TZ info might be better.
                 # return dt.replace(tzinfo=datetime.timezone.utc) # Safer default?
                 if ZoneInfo:
                      try:
                           # Attempt to assume local US/Eastern if no TZ provided
                           return dt.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(datetime.timezone.utc)
                      except Exception: # Fallback to UTC if zoneinfo fails
                            return dt.replace(tzinfo=datetime.timezone.utc)
                 else: # No zoneinfo, treat as naive or assume UTC
                      return dt.replace(tzinfo=datetime.timezone.utc)

        except ValueError:
            continue

    logger.debug(f"parse_ib_datetime() – could not parse '%s' with known formats.", dt_str)
    return None