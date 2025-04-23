# core/event_queue.py
import queue
import logging

logger = logging.getLogger("TradingSystem")

MAX_QUEUE_SIZE = 10_000      # make it configurable later if desired

event_queue: "queue.Queue" = queue.Queue(maxsize=MAX_QUEUE_SIZE)

def put_safely(evt, block: bool = False) -> None:
    """Put *evt* on the queue, dropping it (with CRITICAL log)
    if the queue is already full."""
    try:
        event_queue.put(evt, block=block)
    except queue.Full:
        logger.critical("EVENT‑QUEUE FULL – dropping %s", type(evt).__name__)