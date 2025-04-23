# filename: execution/ib_executor.py
# execution/ib_executor.py (Revised: Use CallbackManager for sync, Fetch Contract)
import logging
logger = logging.getLogger("TradingSystem")

# --- Import from ibapi ---
from ibapi.order import Order as IbkrOrder
from ibapi.contract import Contract as IbkrContract
from ibapi.order_state import OrderState # Added import

import queue
import datetime
import time
import math
import uuid
import threading # Use threading Event
from typing import Optional, List, Set, Dict, Any

# Import core components
from core.events import OrderEvent, FillEvent, ShutdownEvent, OrderFailedEvent
from core.ib_wrapper import IBWrapper # Import the revised wrapper
# --- FIX: Need access to data_handler instance to get contracts ---
from data.ib_handler import IBDataHandler, create_ibkr_contract


class IBExecutionHandler:
    """
    Handles order execution via IB API using IBWrapper.
    Uses IBWrapper's CallbackManager for initial sync.
    """
    # --- FIX: Add data_handler to __init__ ---
    def __init__(self, event_q: queue.Queue, ib_wrapper: IBWrapper, data_handler: IBDataHandler):
        if not isinstance(ib_wrapper, IBWrapper): raise TypeError("ib_wrapper must be IBWrapper")
        if not isinstance(event_q, queue.Queue): raise TypeError("event_q must be Queue")
        if not isinstance(data_handler, IBDataHandler): raise TypeError("data_handler must be IBDataHandler") # Add check

        self.ib_wrapper = ib_wrapper
        self.event_queue = event_q
        self.data_handler = data_handler # Store data_handler instance
        self.open_order_ids: Set[int] = set()
        self.order_id_to_ref: Dict[int, str] = {}
        self.order_ref_to_id: Dict[str, int] = {}

        # Rate Limiting & Control
        self.order_timestamps: List[float] = []
        self.MAX_ORDERS_PER_SEC = 10
        self.MAX_ORDER_QTY_SANITY = 100000
        self._stopping = False
        self._initial_sync_complete = threading.Event()

    def start_initial_sync(self):
        """Initiates the synchronous request for open orders."""
        self._request_initial_sync()

    def _request_initial_sync(self):
         """Requests open orders on startup using listener registration."""
         if self._stopping or not self.ib_wrapper.isConnected():
             logger.warning("Executor: Cannot sync, wrapper not connected.")
             self._initial_sync_complete.set(); return

         logger.info("Executor requesting open orders for initial sync...")
         self.open_order_ids.clear(); self.order_id_to_ref.clear(); self.order_ref_to_id.clear()

         sync_finished_event = threading.Event()
         listener_tag = f"executor_sync_{id(self)}" # Unique tag

         # --- Define Listener Functions ---
         def sync_openOrder(orderId, contract, order, orderState):
             # Process the order for executor sync
             if orderState.status not in ['Cancelled', 'ApiCancelled', 'Filled', 'Inactive', 'PendingCancel', 'ApiPending', 'PendingSubmit']:
                 if orderId not in self.open_order_ids:
                     self.open_order_ids.add(orderId)
                     ref = order.orderRef or f"SYNC_{order.action[:1]}{int(order.totalQuantity)}_{contract.symbol}_{orderId}"
                     self.order_id_to_ref[orderId] = ref
                     self.order_ref_to_id[ref] = orderId
                     logger.info(f"Executor tracking initially open order: {ref} (ID {orderId}), Status: {orderState.status}")

         def sync_openOrderEnd():
             logger.info(f"Executor: OpenOrderEnd received for initial sync.")
             sync_finished_event.set()

         # --- Register Listeners ---
         self.ib_wrapper.register_listener('openOrder', sync_openOrder)
         self.ib_wrapper.register_listener('openOrderEnd', sync_openOrderEnd)

         try:
             # Request all open orders for the account
             self.ib_wrapper.reqAllOpenOrders()
             logger.info("Executor: Requested all open orders via reqAllOpenOrders().")

             # Wait for openOrderEnd
             timeout = 15.0
             logger.info(f"Executor waiting up to {timeout}s for OpenOrderEnd signal...")
             if not sync_finished_event.wait(timeout=timeout):
                 logger.warning("Executor: Timeout waiting for OpenOrderEnd during initial sync.")

         except Exception as e:
             logger.exception(f"Executor: Exception during open order request: {e}")
         finally:
             # --- Unregister Listeners ---
             self.ib_wrapper.unregister_listener('openOrder', sync_openOrder)
             self.ib_wrapper.unregister_listener('openOrderEnd', sync_openOrderEnd)

             self._initial_sync_complete.set() # Signal completion (even on timeout/error)
             logger.info(f"Executor initial open orders sync complete. Tracking {len(self.open_order_ids)} orders.")

    def wait_for_initial_sync(self, timeout=15.0) -> bool:
         """Blocks until initial open order sync is complete or timeout."""
         logger.info(f"Executor waiting up to {timeout}s for initial sync signal...")
         return self._initial_sync_complete.wait(timeout=timeout)

    def _get_next_order_id(self) -> Optional[int]:
        """Gets the next available order ID from the wrapper."""
        if self.ib_wrapper.next_valid_order_id is None: logger.error("Next order ID unavailable."); return None
        order_id = self.ib_wrapper.next_valid_order_id
        self.ib_wrapper.next_valid_order_id += 1
        return order_id

    def process_order_event(self, order_event: OrderEvent):
        """Converts OrderEvent into IB Order(s) and places them."""
        if self._stopping: logger.warning(f"Executor stopping, dropping order: {order_event}"); return

        # Rate Limiting & Sanity Checks
        now = time.time()
        self.order_timestamps = [t for t in self.order_timestamps if now - t <= 1.0]
        if len(self.order_timestamps) >= self.MAX_ORDERS_PER_SEC:
            logger.critical("ORDER RATE LIMIT EXCEEDED (%d/sec). Halting.", self.MAX_ORDERS_PER_SEC)
            self.event_queue.put(ShutdownEvent("Order rate limit exceeded")); self._stopping = True; return
        if not isinstance(order_event.quantity, (int, float)) or order_event.quantity <= 0 or order_event.quantity > self.MAX_ORDER_QTY_SANITY:
            logger.critical(f"INSANE ORDER QTY: {order_event.quantity}. Halting.")
            self.event_queue.put(ShutdownEvent(f"Insane order quantity: {order_event.quantity}")); self._stopping = True; return
        if not self.ib_wrapper.isConnected():
            logger.error(f"Cannot place order {order_event.symbol}, not connected.")
            self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), None, order_event.symbol, 0, "Executor not connected")); return

        # --- FIX: Get qualified contract from DataHandler ---
        contract = self.data_handler.contracts.get(order_event.symbol)
        if not contract or not contract.conId:
            logger.error(f"Cannot place order, contract missing/unqualified for {order_event.symbol}.")
            # Optionally try re-qualifying? For now, fail fast.
            self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), None, order_event.symbol, 0, "Contract missing/unqualified")); return

        # Get next order ID
        ib_order_id = self._get_next_order_id()
        if ib_order_id is None:
             logger.error("Failed to get next order ID. Cannot place order."); self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), None, order_event.symbol, 0, "Failed to get OrderID")); return

        # Create unique reference
        ts_part = order_event.timestamp.strftime('%H%M%S%f')[:-3]
        uuid_part = uuid.uuid4().hex[:6]
        unique_ref = f"{order_event.direction[0]}{int(order_event.quantity)}_{order_event.symbol}_{ts_part}_{uuid_part}"[:60]

        # Create ibapi Order object
        ib_order = IbkrOrder()
        ib_order.action = order_event.direction.upper()
        ib_order.totalQuantity = order_event.quantity
        ib_order.orderRef = unique_ref
        if self.ib_wrapper.accountName: ib_order.account = self.ib_wrapper.accountName
        ib_order.tif = "GTC"

        # Set order type and prices
        if order_event.order_type == 'MKT': ib_order.orderType = "MKT"
        elif order_event.order_type == 'LMT' and order_event.limit_price:
            ib_order.orderType = "LMT"; ib_order.lmtPrice = float(order_event.limit_price)
        else:
            logger.error(f"Unsupported order type '{order_event.order_type}' or missing price for {unique_ref}."); self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), ib_order_id, order_event.symbol, 0, f"Unsupported order type/price: {order_event.order_type}")); return

        # --- Handle Bracket Orders ---
        is_bracket_order = (order_event.direction == 'BUY' and hasattr(order_event, 'stop_price') and
                            order_event.stop_price is not None and order_event.stop_price > 0)

        if is_bracket_order:
             if ib_order.orderType != 'MKT': logger.error(f"Bracket only supports MKT entry for now. Order {unique_ref} aborted."); self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), ib_order_id, order_event.symbol, 0, "Bracket only supports MKT entry")); return
             logger.info(f"Placing bracket order: {unique_ref} (Entry ID {ib_order_id}, Stop @ {order_event.stop_price:.2f})")
             stop_order_id = None # Define before try block
             stop_ref = None
             try:
                 # Parent Order
                 ib_order.transmit = False
                 self.ib_wrapper.placeOrder(ib_order_id, contract, ib_order)
                 logger.info(f"Placed PARENT {unique_ref}: ID {ib_order_id} (Transmit=False)")
                 self.open_order_ids.add(ib_order_id); self.order_id_to_ref[ib_order_id] = unique_ref; self.order_ref_to_id[unique_ref] = ib_order_id
                 self.order_timestamps.append(now)
                 # Stop Order
                 stop_order_id = self._get_next_order_id()
                 if stop_order_id is None: raise ValueError("Failed to get ID for stop order")
                 stop_order = IbkrOrder()
                 stop_order.action = "SELL"; stop_order.orderType = "STP"
                 stop_order.auxPrice = float(order_event.stop_price); stop_order.totalQuantity = order_event.quantity
                 stop_order.parentId = ib_order_id; stop_order.transmit = True; stop_order.tif = "GTC"
                 stop_ref = f"STOP_{unique_ref}"[:60]; stop_order.orderRef = stop_ref
                 if self.ib_wrapper.accountName: stop_order.account = self.ib_wrapper.accountName
                 self.ib_wrapper.placeOrder(stop_order_id, contract, stop_order)
                 logger.info(f"Placed CHILD stop loss linked to {ib_order_id}: ID {stop_order_id} (Transmit=True)")
                 self.open_order_ids.add(stop_order_id); self.order_id_to_ref[stop_order_id] = stop_ref; self.order_ref_to_id[stop_ref] = stop_order_id
                 self.order_timestamps.append(time.time())
             except Exception as e:
                 logger.exception(f"Error placing bracket order {unique_ref}: {e}")
                 self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), ib_order_id, order_event.symbol, 0, f"Exception placing bracket: {e}"))
                 self.open_order_ids.discard(ib_order_id); self.order_id_to_ref.pop(ib_order_id, None); self.order_ref_to_id.pop(unique_ref, None)
                 if stop_order_id: self.open_order_ids.discard(stop_order_id); self.order_id_to_ref.pop(stop_order_id, None)
                 if stop_ref: self.order_ref_to_id.pop(stop_ref, None)
        # --- Simple Order Logic ---
        else:
             logger.info(f"Placing simple order: {unique_ref} (ID: {ib_order_id})")
             try:
                 ib_order.transmit = True
                 self.ib_wrapper.placeOrder(ib_order_id, contract, ib_order)
                 self.open_order_ids.add(ib_order_id); self.order_id_to_ref[ib_order_id] = unique_ref; self.order_ref_to_id[unique_ref] = ib_order_id
                 self.order_timestamps.append(now)
                 logger.info(f"Placed simple order {unique_ref}: ID {ib_order_id}")
             except Exception as e:
                 logger.exception(f"Error placing simple order {unique_ref}: {e}")
                 self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), ib_order_id, order_event.symbol, 0, f"Exception placing simple order: {e}"))

    def process_fill_event(self, fill_event: FillEvent):
         """Handles FillEvents coming from the IBWrapper."""
         order_id = fill_event.order_id
         order_ref = self.order_id_to_ref.get(order_id)
         logger.debug(f"Executor observed FillEvent: Ref={order_ref}, ID={order_id}, ExecID={fill_event.exec_id}")
         # PortfolioManager handles the main fill logic. Executor might remove order from tracking if fully filled.
         # This requires orderStatus updates to know remaining quantity. For simplicity, let's assume
         # PortfolioManager manages the active state based on fills.

    def process_order_failed_event(self, fail_event: OrderFailedEvent):
         """Handles OrderFailedEvents coming from the IBWrapper or generated internally."""
         order_id = fail_event.order_id
         order_ref = self.order_id_to_ref.get(order_id)
         logger.warning(f"Executor observed OrderFailedEvent: Ref={order_ref}, ID={order_id}, Code={fail_event.error_code}, Msg={fail_event.error_msg}")
         if order_id is not None:
             if order_id in self.open_order_ids: self.open_order_ids.discard(order_id)
             if order_id in self.order_id_to_ref:
                 ref = self.order_id_to_ref.pop(order_id)
                 self.order_ref_to_id.pop(ref, None)
                 logger.info(f"Removed failed order {ref} (ID: {order_id}) from tracking.")

    def shutdown(self):
        """Cleans up executor resources."""
        logger.info("Shutting down Execution Handler...")
        self._stopping = True
        if self.ib_wrapper.isConnected() and self.open_order_ids:
            logger.warning(f"Executor shutdown: {len(self.open_order_ids)} order(s) potentially open: {list(self.open_order_ids)}")
        logger.info("Execution Handler shutdown complete.")