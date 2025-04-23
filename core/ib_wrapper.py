# core/ib_wrapper.py
import logging
import threading
import time
import queue
import datetime
from typing import Optional, Callable, Dict, List

try:                     # Python 3.9+: stdlib zoneinfo
    from zoneinfo import ZoneInfo
except ImportError:      # <3.9 – will treat tz as naive
    ZoneInfo = None      # type: ignore

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.common import OrderId

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
            lst.remove(func)                                              # <<<
            if not lst:                                                   # <<<
                del self._callback_chains[name]                           # <<<

    # --------------------------------------------------
    # public helpers
    # --------------------------------------------------
    def add_callback(self, name: str, func: Callable) -> None:
        """Register *func* to be called after the built‑in callback *name*."""
        self._callback_chains.setdefault(name, []).append(func)

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
            self.connect(host, port, client_id)

            # ---- network thread ---------------------------------------
            if self._client_thread is None or not self._client_thread.is_alive():
                self._client_thread = threading.Thread(
                    target=self.run,
                    name="IBAPI‑NetworkThread",
                    daemon=True
                )
                self._client_thread.start()

            # ---- wait for nextValidId ---------------------------------
            if not self.connected_event.wait(timeout=15):
                logger.error("Timed‑out waiting for nextValidId – aborting connect.")
                self.stop_connection()
                raise ConnectionError("Could not establish IB connection (timeout).")

            # ---- watchdog ---------------------------------------------
            if self._watchdog_thread is None or not self._watchdog_thread.is_alive():
                self._watchdog_thread = threading.Thread(
                    target=self._watchdog_loop,
                    name="IBAPI‑Watchdog",
                    daemon=True
                )
                self._watchdog_thread.start()

            self.connection_active = True
            logger.info("IB connection established.")

    def stop_connection(self) -> None:
        """Gracefully close the socket and join helper threads."""
        with self._lock:
            self._reconnect_enabled = False  # turn off auto‑reconnect
            if self.isConnected():
                logger.info("Disconnecting from IB …")
                try:
                    self.disconnect()
                except Exception as exc:
                    logger.warning("Exception during disconnect(): %s", exc)

            self.connected_event.clear()
            self.connection_active = False

            if self._client_thread and self._client_thread.is_alive():
                self._client_thread.join(timeout=5)

            if self._watchdog_thread and self._watchdog_thread.is_alive():
                self._watchdog_thread.join(timeout=5)

            logger.info("IB connection fully closed.")

    # --------------------------------------------------
    # watchdog logic
    # --------------------------------------------------
    def _watchdog_loop(self) -> None:
        """Reconnect automatically if the socket drops unexpectedly."""
        while self._reconnect_enabled:
            time.sleep(5)
            if not self.isConnected():
                if not self.connection_active:
                    continue                     # already shutting down
                logger.warning("Lost connection to IB – attempting reconnect …")
                self.connection_active = False
                self.connected_event.clear()
                self._attempt_reconnect()

    def _attempt_reconnect(self) -> None:
        delay = min(2 ** self._reconnect_attempt, self._max_reconnect_delay)
        logger.info("Waiting %ds before reconnect attempt #%d …",
                    delay, self._reconnect_attempt + 1)
        time.sleep(delay)
        try:
            self.start_connection(self._host, self._port, self._client_id)
            self._reconnect_attempt = 0                 # success
        except Exception as exc:
            logger.error("Reconnect attempt failed: %s", exc)
            self._reconnect_attempt += 1

    # --------------------------------------------------
    # required IB callbacks
    # --------------------------------------------------
    def nextValidId(self, orderId: OrderId):
        logger.info("nextValidId received ⇒ %s", orderId)
        self.next_valid_order_id = orderId
        self.connected_event.set()

        for fn in self._callback_chains.get("nextValidId", []):
            try:
                fn(orderId)
            except Exception as exc:
                logger.exception("User nextValidId hook raised: %s", exc)

    def connectionClosed(self):
        logger.warning("IB connectionClosed() callback fired.")
        self.connection_active = False
        self.connected_event.clear()

        for fn in self._callback_chains.get("connectionClosed", []):
            try:
                fn()
            except Exception as exc:
                logger.exception("User connectionClosed hook raised: %s", exc)

        # watchdog thread will handle the reconnect

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        level = logging.ERROR if errorCode < 2100 else logging.INFO
        logger.log(level, "IB‑ERR %s | reqId=%s: %s",
                   errorCode, reqId, errorString)

        for fn in self._callback_chains.get("error", []):
            try:
                fn(reqId, errorCode, errorString, advancedOrderRejectJson)
            except Exception as exc:
                logger.exception("User error hook raised: %s", exc)

    # ==================================================
    # Utility – parse IB date/time strings
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
    # contract / market‑data
    "contractDetails", "contractDetailsEnd",

    # orders / executions
    "openOrder", "openOrderEnd", "orderStatus",
    "execDetails", "commissionReport",

    # account / portfolio
    "updateAccountValue", "updatePortfolio", "updateAccountTime",
    "accountDownloadEnd", "position", "positionEnd",

    # add more names if future modules need them
]

def _install_chain_method(cls, name):
    original = getattr(cls, name, None)

    def _wrapper(self, *args, **kwargs):
        # call the original (if any)
        if original is not None:
            original(self, *args, **kwargs)

        # forward to listeners
        for fn in self._callback_chains.get(name, []):
            try:
                fn(*args, **kwargs)
            except Exception:
                logger.exception("Listener %s raised", name)

    _wrapper.__name__ = name
    setattr(cls, name, _wrapper)

for _cb in _IB_CALLBACK_NAMES:
    _install_chain_method(IBWrapper, _cb)

def parse_ib_datetime(dt_str: str) -> Optional[datetime.datetime]:
    """
    Convert the many different date/time strings emitted by IB into a
    `datetime` object (timezone‑aware where possible).

    Returns **None** if parsing fails.
    """
    if not dt_str:
        return None

    # First try the known explicit formats
    for fmt in IBWrapper._IB_DT_FORMATS:
        try:
            dt = datetime.datetime.strptime(dt_str, fmt)
            # If the format lacked tz info but system has zoneinfo
            if dt.tzinfo is None and ZoneInfo:
                dt = dt.replace(tzinfo=ZoneInfo("America/New_York"))
            return dt
        except ValueError:
            continue

    # As a last resort, try interpreting it as a pure epoch seconds string
    if dt_str.isdigit():
        try:
            ts = int(dt_str)
            return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        except (OverflowError, ValueError):
            pass

    logger.debug("parse_ib_datetime() – could not parse '%s'", dt_str)
    return None
