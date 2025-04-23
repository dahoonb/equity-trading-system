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
from core.events import OrderEvent, FillEvent, ShutdownEvent, OrderFailedEvent, InternalFillProcessedEvent
from core.ib_wrapper import IBWrapper # Import the revised wrapper
# --- FIX: Need access to data_handler instance to get contracts ---
from data.ib_handler import IBDataHandler, create_ibkr_contract


class IBExecutionHandler:
    """
    Handles order execution via IB API using IBWrapper.
    Uses IBWrapper's CallbackManager for initial sync.
    """
    # --- FIX: Add data_handler to __init__ ---
    def __init__(self, event_q: queue.Queue, ib_wrapper: IBWrapper, data_handler: IBDataHandler, config: Dict[str, Any]):
        if not isinstance(ib_wrapper, IBWrapper): raise TypeError("ib_wrapper must be IBWrapper")
        if not isinstance(event_q, queue.Queue): raise TypeError("event_q must be Queue")
        if not isinstance(data_handler, IBDataHandler): raise TypeError("data_handler must be IBDataHandler") # Add check

        self.ib_wrapper = ib_wrapper
        self.event_queue = event_q
        self.data_handler = data_handler # Store data_handler instance
        self.open_order_ids: Set[int] = set()
        self.order_id_to_ref: Dict[int, str] = {}
        self.order_ref_to_id: Dict[str, int] = {}
        self.config = config # Store the passed-in config dictionary
        
        # Rate Limiting & Control
        self.order_timestamps: List[float] = []
        self.MAX_ORDERS_PER_SEC = 10
        self.MAX_ORDER_QTY_SANITY = 100000
        self._stopping = False
        self._initial_sync_complete = threading.Event()
        
        # --- OER Monitoring Attributes ---
        self._oer_lock = threading.Lock()
        self.oer_order_actions_count = 0
        self.oer_fill_count = 0
        # Load OER config with defaults
        monitor_config = self.config.get('monitoring', {}) # Use self.config here
        self.oer_threshold = monitor_config.get('oer_threshold', 0.02) # Default 2%
        self.oer_check_interval = monitor_config.get('oer_check_interval_seconds', 3600) # Default 1 hour
        self.oer_min_actions = monitor_config.get('oer_min_actions', 50) # Default 50 actions
        self.last_oer_check_time = time.monotonic()
        logger.info(f"OER Monitoring Initialized: Threshold={self.oer_threshold:.2%}, Interval={self.oer_check_interval}s, Min Actions={self.oer_min_actions}")
        # --- End OER ---

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

        # --- Determine if it's a full bracket order ---
        # Check if it's a BUY order with BOTH stop and take profit prices
        is_full_bracket = (
            order_event.direction == 'BUY' and
            order_event.stop_price is not None and order_event.stop_price > 0 and
            hasattr(order_event, 'take_profit_price') and # Check attribute exists (due to event mod)
            order_event.take_profit_price is not None and order_event.take_profit_price > 0
        )
        # Check if take profit is reasonably above stop
        if is_full_bracket and order_event.take_profit_price <= order_event.stop_price:
             logger.error(f"Take Profit price ({order_event.take_profit_price:.2f}) not above Stop price ({order_event.stop_price:.2f}) for bracket {order_event.symbol}. Aborting.")
             self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), None, order_event.symbol, 0, "TP not above SL")); return

        # Get next order ID
        parent_order_id = self._get_next_order_id()
        if parent_order_id is None: logger.error("Failed to get next order ID. Cannot place order."); self.event_queue.put(OrderFailedEvent("Failed to get OrderID")); return
        stop_order_id = self._get_next_order_id() if is_full_bracket else None
        profit_order_id = self._get_next_order_id() if is_full_bracket else None
        if is_full_bracket and (stop_order_id is None or profit_order_id is None):
             logger.error("Failed to get sufficient order IDs for bracket order. Aborting."); self.event_queue.put(OrderFailedEvent("Failed to get OrderID")); return

        # --- Create unique reference (can be shared for bracket group) ---
        ts_part = order_event.timestamp.strftime('%H%M%S%f')[:-3]
        uuid_part = uuid.uuid4().hex[:6]
        # Base reference for logging/tracking the group
        base_ref = f"{order_event.direction[0]}{int(order_event.quantity)}_{order_event.symbol}_{ts_part}_{uuid_part}"[:50] # Shorter base

        # --- Create Parent/Entry Order ---
        ib_order = IbkrOrder()
        ib_order.action = order_event.direction.upper()
        ib_order.totalQuantity = order_event.quantity
        ib_order.orderRef = f"{order_event.order_type[0]}_{base_ref}"[:60] # Include type hint in ref
        if self.ib_wrapper.accountName: ib_order.account = self.ib_wrapper.accountName
        ib_order.tif = "GTC" # Good Till Cancelled is typical for exits

        # --- Set order type and specific parameters ---
        order_type_ok = False
        if order_event.order_type == 'MKT':
            ib_order.orderType = "MKT"
            order_type_ok = True
        elif order_event.order_type == 'LMT' and order_event.limit_price:
            ib_order.orderType = "LMT"
            ib_order.lmtPrice = float(order_event.limit_price)
            order_type_ok = True
        # --- SOLUTION C: Handle TRAIL Order Type ---
        elif order_event.order_type == 'TRAIL':
            ib_order.orderType = "TRAIL"
            # Set EITHER trailing amount (auxPrice) OR trailing percent
            if order_event.trailing_amount is not None and order_event.trailing_amount > 0:
                ib_order.auxPrice = float(order_event.trailing_amount) # Trailing amount
                order_type_ok = True
            elif order_event.trailing_percent is not None and order_event.trailing_percent > 0:
                ib_order.trailingPercent = float(order_event.trailing_percent) # Trailing percent
                # Note: TWS API documentation suggests auxPrice might still be needed
                # even with trailingPercent, often set to limit price of stop trigger.
                # Check API docs carefully for the exact TRAIL variant you need.
                # For simple TRAIL stop loss, auxPrice is the amount/percent.
                order_type_ok = True
            else:
                 logger.error(f"TRAIL order requires either trailing_amount or trailing_percent for {ib_order.orderRef}")
            # Set optional initial stop trigger price
            if order_event.trail_stop_price is not None and order_event.trail_stop_price > 0:
                 ib_order.trailStopPrice = float(order_event.trail_stop_price)
        # --- END SOLUTION C ---
        # --- Add elif blocks here for other types like STP LMT, REL, etc. ---
        # Example for STP LMT (Stop Limit) - Needs stop_price and limit_price from OrderEvent
        # elif order_event.order_type == 'STP LMT' and order_event.stop_price and order_event.limit_price:
        #     ib_order.orderType = "STP LMT"
        #     ib_order.auxPrice = float(order_event.stop_price) # Stop trigger price
        #     ib_order.lmtPrice = float(order_event.limit_price) # Limit price for the triggered order
        #     order_type_ok = True

        else: # Fallback for unsupported types or missing prices
            logger.error(f"Unsupported order type '{order_event.order_type}' or missing required price(s) for {ib_order.orderRef}.")
            self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), parent_order_id, order_event.symbol, 0, f"Unsupported order type/price: {order_event.order_type}")); return

        # Abort if order type setup failed
        if not order_type_ok:
             self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), parent_order_id, order_event.symbol, 0, f"Order type setup failed: {order_event.order_type}")); return

        # --- Initialize child order variables ---
        stop_order: Optional[IbkrOrder] = None
        profit_order: Optional[IbkrOrder] = None
        actions_placed_count = 0 # Count actions placed in this attempt

         # --- Place Order(s) ---
        try:
            if is_full_bracket:
                # --- Full Bracket Logic ---
                # Use 'ib_order' for the parent configuration
                ib_order.orderRef = f"P_{base_ref}"[:60] # Parent specific ref
                logger.info(f"Placing Full Bracket Order Group: {base_ref} "
                            f"(Entry ID {parent_order_id}, SL ID {stop_order_id}, TP ID {profit_order_id}) - "
                            f"Entry: {ib_order.orderType} @ {ib_order.lmtPrice if ib_order.orderType == 'LMT' else 'MKT'}, "
                            f"SL: {order_event.stop_price:.2f}, TP: {order_event.take_profit_price:.2f}")

                oca_group_name = f"OCA_{base_ref}"
                ib_order.ocaGroup = oca_group_name
                ib_order.ocaType = 3 # Cancel other non-block orders

                # 1. Place Parent Order (Transmit = False)
                ib_order.transmit = False
                self.ib_wrapper.placeOrder(parent_order_id, contract, ib_order)
                actions_placed_count += 1
                logger.debug(f"Placed PARENT order {ib_order.orderRef} (ID {parent_order_id}), Transmit=False")
                # Track immediately
                self.open_order_ids.add(parent_order_id); self.order_id_to_ref[parent_order_id] = ib_order.orderRef; self.order_ref_to_id[ib_order.orderRef] = parent_order_id
                self.order_timestamps.append(time.time())

                # 2. Create and Place Stop Loss Order (Transmit = False)
                stop_order = IbkrOrder() # Assign to initialized variable
                stop_order.action = "SELL"; stop_order.orderType = "STP"
                stop_order.auxPrice = float(order_event.stop_price); stop_order.totalQuantity = order_event.quantity
                stop_order.parentId = parent_order_id
                stop_order.ocaGroup = oca_group_name; stop_order.ocaType = 3
                stop_order.transmit = False
                stop_order.tif = "GTC"
                stop_order.orderRef = f"SL_{base_ref}"[:60]
                if self.ib_wrapper.accountName: stop_order.account = self.ib_wrapper.accountName

                self.ib_wrapper.placeOrder(stop_order_id, contract, stop_order)
                actions_placed_count += 1
                logger.debug(f"Placed CHILD SL order {stop_order.orderRef} (ID {stop_order_id}), ParentID={parent_order_id}, Transmit=False")
                # Track immediately
                self.open_order_ids.add(stop_order_id); self.order_id_to_ref[stop_order_id] = stop_order.orderRef; self.order_ref_to_id[stop_order.orderRef] = stop_order_id

                # 3. Create and Place Take Profit Order (Transmit = True)
                profit_order = IbkrOrder() # Assign to initialized variable
                profit_order.action = "SELL"; profit_order.orderType = "LMT"
                profit_order.lmtPrice = float(order_event.take_profit_price); profit_order.totalQuantity = order_event.quantity
                profit_order.parentId = parent_order_id
                profit_order.ocaGroup = oca_group_name; profit_order.ocaType = 3
                profit_order.transmit = True # Transmit=True ONLY for the last order
                profit_order.tif = "GTC"
                profit_order.orderRef = f"TP_{base_ref}"[:60]
                if self.ib_wrapper.accountName: profit_order.account = self.ib_wrapper.accountName

                self.ib_wrapper.placeOrder(profit_order_id, contract, profit_order)
                actions_placed_count += 1
                logger.info(f"Placed CHILD TP order {profit_order.orderRef} (ID {profit_order_id}), ParentID={parent_order_id}, Transmit=True - Bracket Submitted")
                # Track immediately
                self.open_order_ids.add(profit_order_id); self.order_id_to_ref[profit_order_id] = profit_order.orderRef; self.order_ref_to_id[profit_order.orderRef] = profit_order_id

            else: # Simple Order (Not a full bracket)
                # Use 'ib_order' directly
                ib_order.orderRef = f"{ib_order.orderType[0]}_{base_ref}"[:60] # Assign ref for simple order
                logger.info(f"Placing simple order: {ib_order.orderRef} (ID: {parent_order_id})")
                ib_order.transmit = True
                self.ib_wrapper.placeOrder(parent_order_id, contract, ib_order)
                actions_placed_count += 1
                # Track immediately
                self.open_order_ids.add(parent_order_id); self.order_id_to_ref[parent_order_id] = ib_order.orderRef; self.order_ref_to_id[ib_order.orderRef] = parent_order_id
                self.order_timestamps.append(now)
                logger.info(f"Placed simple {ib_order.orderType} order {ib_order.orderRef}: ID {parent_order_id}")

            # --- MODIFICATION: Increment OER Action Count ---
            if actions_placed_count > 0:
                with self._oer_lock:
                    self.oer_order_actions_count += actions_placed_count
                    logger.debug(f"OER Actions incremented by {actions_placed_count}. Total actions: {self.oer_order_actions_count}")
            # --- END MODIFICATION ---

        except Exception as e:
            logger.exception(f"Error placing order group {base_ref}: {e}")
            self.event_queue.put(OrderFailedEvent(datetime.datetime.now(datetime.timezone.utc), parent_order_id, order_event.symbol, 0, f"Exception placing order: {e}"))
            # --- Clean up tracking for potentially placed parts ---
            # Use 'ib_order' ref here. Use getattr for child orders safely.
            ids_to_remove = [parent_order_id, stop_order_id, profit_order_id]
            # Ensure refs are accessed safely using getattr on potentially None objects
            refs_to_remove = [
                getattr(ib_order, 'orderRef', None),
                getattr(stop_order, 'orderRef', None),
                getattr(profit_order, 'orderRef', None)
            ]
            for i, order_id in enumerate(ids_to_remove):
                if order_id is not None:
                    self.open_order_ids.discard(order_id)
                    ref = self.order_id_to_ref.pop(order_id, None)
                    # Also try removing the ref from the other mapping if it existed
                    ref_from_list = refs_to_remove[i]
                    if ref_from_list and ref_from_list in self.order_ref_to_id:
                          self.order_ref_to_id.pop(ref_from_list, None)
                    elif ref and ref in self.order_ref_to_id: # Fallback using ref from id map
                         self.order_ref_to_id.pop(ref, None)

    # --- MODIFICATION: Add method to handle internal fill events ---
    def process_internal_fill_event(self, event: InternalFillProcessedEvent):
        """Processes internal fill events to increment the OER fill counter."""
        if isinstance(event, InternalFillProcessedEvent):
            with self._oer_lock:
                self.oer_fill_count += 1
                logger.debug(f"OER Fill recorded for OrderID {event.order_id}. Total fills: {self.oer_fill_count}")
    # --- END MODIFICATION ---

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

    # --- MODIFICATION: Add OER Check Method ---
    def check_oer(self):
        """Calculates and logs the Order Efficiency Ratio."""
        now_mono = time.monotonic()
        if now_mono - self.last_oer_check_time > self.oer_check_interval:
            with self._oer_lock:
                actions = self.oer_order_actions_count
                fills = self.oer_fill_count
                current_oer = (fills / actions) if actions > 0 else 1.0 # Avoid division by zero, assume 100% if no actions

                log_msg = f"OER Check: Fills={fills}, Actions={actions}, Ratio={current_oer:.4f} (Threshold: {self.oer_threshold:.4f})"

                # Only check threshold if minimum actions are met
                if actions >= self.oer_min_actions:
                    if current_oer < self.oer_threshold:
                        logger.warning(f"{log_msg} - OER BELOW THRESHOLD!")
                        # --- Potential Future Actions ---
                        # Trigger alert (e.g., put AlertEvent on queue)
                        # Throttle new order placement
                        # Halt specific strategy causing low OER
                        # --- End Potential Actions ---
                    else:
                        logger.info(log_msg)
                else:
                    logger.info(f"{log_msg} - Min actions ({self.oer_min_actions}) not yet met.")

                # Reset counters for the next window interval
                # Note: This uses a fixed window. A rolling window (e.g., using a deque)
                # might be more appropriate for continuous monitoring.
                self.oer_order_actions_count = 0
                self.oer_fill_count = 0
                self.last_oer_check_time = now_mono
    # --- END MODIFICATION ---

    def shutdown(self):
        """Cleans up executor resources."""
        logger.info("Shutting down Execution Handler...")
        self._stopping = True
        # --- MODIFICATION: Log final OER stats ---
        try:
            with self._oer_lock:
                 actions = self.oer_order_actions_count
                 fills = self.oer_fill_count
                 final_oer = (fills / actions) if actions > 0 else 1.0
                 logger.info(f"Final OER Stats (Last Window): Fills={fills}, Actions={actions}, Ratio={final_oer:.4f}")
        except Exception as e:
             logger.error(f"Error logging final OER stats: {e}")
        # --- END MODIFICATION ---
        if self.ib_wrapper.isConnected() and self.open_order_ids:
            logger.warning(f"Executor shutdown: {len(self.open_order_ids)} order(s) potentially open: {list(self.open_order_ids)}")
        logger.info("Execution Handler shutdown complete.")