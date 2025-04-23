# core/events.py
import datetime
import pandas as pd

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
    """Handles the event of sending a Signal from a Strategy object."""
    def __init__(self, timestamp: datetime.datetime, symbol: str, direction: str,
                 strength: float = 1.0, stop_price: float | None = None,
                 entry_price: float | None = None):
        self.type = 'SIGNAL'
        self.timestamp = timestamp
        self.symbol = symbol
        self.direction = direction.upper()
        self.strength = strength
        self.stop_price = stop_price
        self.entry_price = entry_price

    def __str__(self):
         details = f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] SIGNAL: {self.symbol} Direction: {self.direction} Strength: {self.strength:.2f}"
         if self.entry_price: details += f" EntryRef: {self.entry_price:.2f}"
         if self.stop_price: details += f" Stop: {self.stop_price:.2f}"
         return details

class OrderEvent(Event):
    """Handles the event of sending an Order to an execution system."""
    def __init__(self, timestamp: datetime.datetime, symbol: str, order_type: str,
                 direction: str, quantity: float, stop_price: float | None = None,
                 limit_price: float | None = None):
        self.type = 'ORDER'
        self.timestamp = timestamp
        self.symbol = symbol
        self.order_type = order_type.upper()
        self.direction = direction.upper()
        self.quantity = abs(quantity)
        self.stop_price = stop_price
        self.limit_price = limit_price
        self.order_ref = f"{direction[:1]}{int(quantity)}_{symbol}" # Simple reference

    def __str__(self):
        order_details = f"{self.direction} {self.quantity} {self.symbol} @ {self.order_type}"
        if self.limit_price: order_details += f" LMT {self.limit_price:.2f}"
        if self.stop_price: order_details += f" (Stop: {self.stop_price:.2f})"
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ORDER: {order_details}"

class FillEvent(Event):
    """Encapsulates the notion of a filled order, as returned from a brokerage."""
    def __init__(self, timestamp: datetime.datetime, symbol: str, exchange: str,
                 quantity: float, direction: str, fill_cost: float, commission: float = 0.0,
                 order_id: int | None = None, exec_id: str | None = None):
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
        self.fill_price = (self.fill_cost / self.quantity) if self.quantity > 1e-9 else 0.0

    def __str__(self):
        return (f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] FILL: {self.direction} {self.quantity} {self.symbol} "
                f"@ {self.fill_price:.2f} Cost: {self.fill_cost:.2f} Comm: {self.commission:.2f} "
                f"(OrderID: {self.order_id}, ExecID: {self.exec_id})")

class OrderFailedEvent(Event):
    """Handles the event of an order failing (rejected, cancelled by IB, etc.)."""
    def __init__(self, timestamp: datetime.datetime, order_id: int | None, symbol: str | None,
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


class ShutdownEvent(Event):
    """Special event to signal system shutdown."""
    def __init__(self, reason: str = "Unknown"):
        self.type = 'SHUTDOWN'
        self.reason = reason

    def __str__(self):
        return f"SHUTDOWN Event: Reason: {self.reason}"