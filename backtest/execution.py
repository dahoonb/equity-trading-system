# backtest/execution.py (Revised and Completed)
import queue
import datetime
import math # Import math for isnan checks
from typing import Dict, Optional

# Import core components
from core.events import OrderEvent, FillEvent, OrderFailedEvent # Added OrderFailedEvent
# from core.event_queue import event_queue # Passed in constructor
from utils.logger import setup_logger, logger # Use configured logger

# Ensure logger is configured (assuming setup_logger is called elsewhere)
# logger = setup_logger() # Or get it if already configured

class SimulatedExecutionHandler:
    """
    Simulates the execution of orders in a backtest environment.

    Fills orders based on the open price of the *next* available bar
    after the order event is processed. Includes simulated commission
    and slippage. Puts FillEvent or OrderFailedEvent onto the event queue.
    """
    def __init__(self, event_q: queue.Queue,
                 commission_per_share: float = 0.005,
                 min_commission: float = 1.0,
                 slippage_pct: float = 0.0005): # 0.05% default slippage
        """
        Initializes the SimulatedExecutionHandler.

        Args:
            event_q: The main event queue for the system.
            commission_per_share: Commission cost per share traded.
            min_commission: Minimum commission per order.
            slippage_pct: Percentage slippage applied to the fill price
                          (positive for buys, negative for sells).
        """
        if not isinstance(event_q, queue.Queue):
            raise TypeError("event_q must be a queue.Queue instance.")
        if not isinstance(commission_per_share, (int, float)) or commission_per_share < 0:
             logger.warning(f"Invalid commission_per_share ({commission_per_share}). Defaulting to 0.0.")
             commission_per_share = 0.0
        if not isinstance(min_commission, (int, float)) or min_commission < 0:
             logger.warning(f"Invalid min_commission ({min_commission}). Defaulting to 0.0.")
             min_commission = 0.0
        if not isinstance(slippage_pct, (int, float)) or slippage_pct < 0:
             logger.warning(f"Invalid slippage_pct ({slippage_pct}). Defaulting to 0.0.")
             slippage_pct = 0.0

        self.event_queue = event_q
        self.commission_per_share = commission_per_share
        self.min_commission = min_commission
        self.slippage_pct = slippage_pct

        # Stores the *next* bar's open price for fill simulation {symbol: open_price}
        self.next_bar_open_prices: Dict[str, Optional[float]] = {}

        logger.info(f"SimulatedExecutionHandler initialized. Commission/Share: ${commission_per_share:.4f}, Min Comm: ${min_commission:.2f}, Slippage: {slippage_pct*100:.3f}%")

    def update_next_bar_open(self, symbol: str, open_price: Optional[float]):
        """
        Stores the open price of the *next* bar, received from the data handler
        before processing the current bar's events.
        """
        if open_price is not None and not math.isnan(open_price) and open_price > 0:
            self.next_bar_open_prices[symbol] = open_price
            # logger.debug(f"Executor updated next open for {symbol}: {open_price:.2f}")
        else:
            # If next open is invalid, remove it to prevent using stale data
            if symbol in self.next_bar_open_prices:
                 del self.next_bar_open_prices[symbol]
            logger.warning(f"Received invalid next bar open price ({open_price}) for {symbol}. Fill may fail.")

    def calculate_commission(self, quantity: float, fill_price: float) -> float:
        """Calculates simulated commission based on configured rates."""
        if quantity <= 0 or fill_price <= 0: return 0.0
        comm = abs(quantity) * self.commission_per_share
        return max(comm, self.min_commission)

    def _apply_slippage(self, price: float, direction: str) -> float:
        """Applies simulated percentage slippage to the base fill price."""
        if price <= 0: return price # Cannot apply slippage to zero/negative price
        slippage_multiplier = 0.0
        if direction == 'BUY':
            slippage_multiplier = 1.0 + self.slippage_pct
        elif direction == 'SELL':
            slippage_multiplier = 1.0 - self.slippage_pct
        else:
            return price # No slippage for unknown directions

        slipped_price = price * slippage_multiplier
        # Ensure price doesn't go below zero due to large slippage on low-priced stocks
        return max(0.01, slipped_price) # Minimum price of $0.01

    def process_order(self, order: OrderEvent, fill_timestamp: datetime.datetime):
        """
        Simulates filling an order based on the stored *next* bar's open price.
        Applies slippage and commission. Puts a FillEvent or OrderFailedEvent
        onto the event queue.

        Args:
            order: The OrderEvent to process.
            fill_timestamp: The timestamp of the bar where the fill occurs
                            (i.e., the timestamp of the *next* bar).
        """
        symbol = order.symbol
        order_ref = getattr(order, 'order_ref', f"Order_{symbol}_{order.direction}") # Use ref if available

        # --- Validation ---
        if not isinstance(order, OrderEvent):
             logger.error(f"Invalid event type passed to process_order: {type(order)}")
             return
        if order.quantity <= 0:
             logger.error(f"Order event has invalid quantity ({order.quantity}) for {order_ref}. Order dropped.")
             # Optionally generate OrderFailedEvent
             self.event_queue.put(OrderFailedEvent(fill_timestamp, None, symbol, 0, f"Invalid order quantity: {order.quantity}"))
             return

        # --- Get Fill Price ---
        base_fill_price = self.next_bar_open_prices.get(symbol)
        if base_fill_price is None:
            logger.warning(f"No next bar open price available for {symbol} to simulate fill for order {order_ref}. Order dropped.")
            self.event_queue.put(OrderFailedEvent(fill_timestamp, None, symbol, 0, "Missing next bar open price for fill"))
            return

        # --- Apply Slippage ---
        simulated_fill_price = self._apply_slippage(base_fill_price, order.direction)
        if simulated_fill_price <= 0:
             logger.error(f"Simulated fill price became zero or negative after slippage for {order_ref} (Base: {base_fill_price:.4f}). Order failed.")
             self.event_queue.put(OrderFailedEvent(fill_timestamp, None, symbol, 0, f"Fill price <= 0 after slippage"))
             return

        # --- Calculate Costs ---
        fill_cost = order.quantity * simulated_fill_price # Cost based on slipped price
        commission = self.calculate_commission(order.quantity, simulated_fill_price)

        # --- Create Fill Event ---
        fill_event = FillEvent(
            timestamp=fill_timestamp, # Use the timestamp of the bar where fill occurs
            symbol=symbol,
            exchange='SIMULATED',
            quantity=order.quantity, # FillEvent expects positive quantity
            direction=order.direction,
            fill_cost=fill_cost, # Total cost/proceeds before commission
            commission=commission
            # order_id and exec_id are None in simulation
        )
        # Add simulated price info for logging/analysis if needed
        # fill_event.simulated_base_price = base_fill_price
        # fill_event.simulated_fill_price = simulated_fill_price

        logger.debug(
            f"Simulating fill for {order_ref}: {fill_event.direction} {fill_event.quantity} {symbol} "
            f"@ ~{simulated_fill_price:.4f} (Base Open: {base_fill_price:.4f}, "
            f"Slippage: {self.slippage_pct*100:.3f}%) Cost: {fill_cost:.2f}, Comm: {commission:.2f}"
        )
        self.event_queue.put(fill_event)

        # Optional: Clear the price used for this fill?
        # If the main loop ensures update_next_bar_open is called *before*
        # processing the *next* set of orders for that bar, clearing might not be needed.
        # If multiple orders for the same symbol might process using the same next_bar_open,
        # clearing might be safer, but complicates logic if multiple fills *should* happen
        # at the same open price. Let's assume the main loop handles timing correctly for now.
        # self.next_bar_open_prices.pop(symbol, None)