# filename: core/events.py
# core/events.py (Modified for Solution C - Added atr_value to SignalEvent)
import datetime
import pandas as pd
from typing import List, Optional # Import Optional

class Event:
    """Base class for all events."""
    pass

class MarketEvent(Event):
    """Handles the event of receiving new market data."""
    def __init__(self, timestamp: datetime.datetime, symbol: str, data_type: str, data: dict):
        self.type = 'MARKET'
        self.timestamp = timestamp
        self.symbol = symbol
        self.data_type = data_type
        self.data = data

    def __str__(self):
        data_str = str(self.data)
        if len(data_str) > 100: data_str = data_str[:100] + "..."
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] MARKET: {self.symbol} ({self.data_type}) Data: {data_str}"

class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    Includes optional ATR value used for stop calculation.
    """
    def __init__(self, timestamp: datetime.datetime, symbol: str, direction: str,
                 strength: float = 1.0, stop_price: Optional[float] = None,
                 entry_price: Optional[float] = None,
                 atr_value: Optional[float] = None): # <-- MODIFICATION: Added atr_value
        self.type = 'SIGNAL'
        self.timestamp = timestamp
        self.symbol = symbol
        self.direction = direction.upper()
        self.strength = strength
        self.stop_price = stop_price
        self.entry_price = entry_price
        self.atr_value = atr_value # <-- MODIFICATION: Store ATR value

    def __str__(self):
         details = f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] SIGNAL: {self.symbol} Direction: {self.direction} Strength: {self.strength:.2f}"
         if self.entry_price: details += f" EntryRef: {self.entry_price:.2f}"
         if self.stop_price: details += f" Stop: {self.stop_price:.2f}"
         # --- MODIFICATION: Display ATR if present ---
         if self.atr_value is not None:
              details += f" ATR: {self.atr_value:.3f}"
         # --- END MODIFICATION ---
         return details

class ContractQualificationCompleteEvent(Event):
    """Signals that the initial asynchronous contract qualification is done."""
    def __init__(self, timestamp: datetime.datetime, successful_symbols: List[str], failed_symbols: List[str]):
        self.type = 'QUALIFICATION_COMPLETE'
        self.timestamp = timestamp
        self.successful_symbols = successful_symbols
        self.failed_symbols = failed_symbols

    def __str__(self):
        return (f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] QUALIFICATION_COMPLETE: "
                f"Success={len(self.successful_symbols)}, Failed={len(self.failed_symbols)}")

class OrderEvent(Event):
    """Handles the event of sending an Order to an execution system."""
    def __init__(self, timestamp: datetime.datetime, symbol: str, order_type: str,
                 direction: str, quantity: float, stop_price: Optional[float] = None,
                 limit_price: Optional[float] = None,
                 take_profit_price: Optional[float] = None,
                 # --- SOLUTION C: Add TRAIL parameters ---
                 trailing_amount: Optional[float] = None,    # For auxPrice on TRAIL orders
                 trailing_percent: Optional[float] = None,   # For trailingPercent on TRAIL orders
                 trail_stop_price: Optional[float] = None,    # For trailStopPrice (initial trigger)
                 atr_value: Optional[float] = None
                 # --- END SOLUTION C ---
                 ):
        self.type = 'ORDER'
        self.timestamp = timestamp
        self.symbol = symbol
        self.order_type = order_type.upper()
        self.direction = direction.upper()
        self.quantity = abs(quantity)
        self.stop_price = stop_price
        self.limit_price = limit_price
        self.take_profit_price = take_profit_price # <-- MODIFICATION: Store take profit
        # --- SOLUTION C: Store TRAIL parameters ---
        self.trailing_amount = trailing_amount
        self.trailing_percent = trailing_percent
        self.trail_stop_price = trail_stop_price
        self.atr_value = atr_value # Store it
        # --- END SOLUTION C ---
        self.order_ref = f"{direction[:1]}{int(quantity)}_{symbol}"

    def __str__(self):
        order_details = f"{self.direction} {self.quantity} {self.symbol} @ {self.order_type}"
        if self.limit_price: order_details += f" LMT {self.limit_price:.2f}"
        if self.stop_price: order_details += f" (Initial Stop Ref: {self.stop_price:.2f})" # Clarify role
        if self.take_profit_price: order_details += f" (TP: {self.take_profit_price:.2f})"
        # --- SOLUTION C: Display TRAIL parameters ---
        if self.order_type == 'TRAIL':
            if self.trailing_amount is not None:
                order_details += f" TrailAmt: {self.trailing_amount:.2f}"
            elif self.trailing_percent is not None:
                order_details += f" Trail%: {self.trailing_percent:.2f}"
            if self.trail_stop_price is not None:
                order_details += f" Trigger: {self.trail_stop_price:.2f}"
        # --- END SOLUTION C ---
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ORDER: {order_details}"

class FillEvent(Event):
    """Encapsulates the notion of a filled order, as returned from a brokerage."""
    def __init__(self, timestamp: datetime.datetime, symbol: str, exchange: str,
                 quantity: float, direction: str, fill_cost: float, commission: float = 0.0,
                 order_id: Optional[int] = None, exec_id: Optional[str] = None): # Use Optional consistently
        self.type = 'FILL'
        self.timestamp = timestamp
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = abs(quantity)
        self.direction = direction.upper()
        self.fill_cost = abs(fill_cost)
        self.commission = abs(commission)
        self.order_id = order_id
        self.exec_id = exec_id
        # Ensure division by zero doesn't occur if quantity is somehow zero
        self.fill_price = (self.fill_cost / self.quantity) if self.quantity > 1e-9 else 0.0

    def __str__(self):
        return (f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] FILL: {self.direction} {self.quantity} {self.symbol} "
                f"@ {self.fill_price:.2f} Cost: {self.fill_cost:.2f} Comm: {self.commission:.2f} "
                f"(OrderID: {self.order_id}, ExecID: {self.exec_id})")

class OrderFailedEvent(Event):
    """Handles the event of an order failing (rejected, cancelled by IB, etc.)."""
    def __init__(self, timestamp: datetime.datetime, order_id: Optional[int], symbol: Optional[str], # Use Optional
                 error_code: int, error_msg: str):
        self.type = 'ORDER_FAILED'
        self.timestamp = timestamp
        self.order_id = order_id
        self.symbol = symbol # Symbol might not always be available depending on error context
        self.error_code = error_code
        self.error_msg = error_msg

    def __str__(self):
        return (f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ORDER_FAILED: OrderID {self.order_id} "
                f"(Symbol: {self.symbol or 'N/A'}) Code: {self.error_code} - {self.error_msg}")

class InternalFillProcessedEvent(Event):
    """
    Internal event to signal that a FillEvent has been processed by the portfolio.
    Used for decoupling OER fill counting.
    """
    def __init__(self, timestamp: datetime.datetime, order_id: Optional[int]):
        self.type = 'INTERNAL_FILL_PROCESSED'
        self.timestamp = timestamp
        self.order_id = order_id

    def __str__(self):
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] INTERNAL_FILL_PROCESSED: OrderID {self.order_id}"

class ShutdownEvent(Event):
    """Special event to signal system shutdown."""
    def __init__(self, reason: str = "Unknown"):
        self.type = 'SHUTDOWN'
        self.reason = reason

    def __str__(self):
        return f"SHUTDOWN Event: Reason: {self.reason}"