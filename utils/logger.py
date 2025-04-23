# utils/logger.py (Revised and Completed with Rotation)
import logging
import sys
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler # Import handler for rotation
from typing import Optional

# --- Global flag and reference ---
_logger_initialized = False
_trading_system_logger: Optional[logging.Logger] = None
_log_filename: Optional[str] = None # Store filename globally

def setup_logger(log_directory: str = "logs",
                 log_level: int = logging.INFO,
                 log_to_console: bool = True,
                 log_to_file: bool = True,
                 backup_count: int = 7) -> logging.Logger:
    """
    Sets up the global logging configuration for the Trading System.

    Configures a root logger and a specific logger named "TradingSystem".
    Supports logging to console and a rotating file.
    This function is idempotent - it configures the logger only once.

    Args:
        log_directory: The directory to store log files.
        log_level: The minimum logging level (e.g., logging.INFO, logging.DEBUG).
        log_to_console: Whether to output logs to the console (stdout).
        log_to_file: Whether to output logs to a rotating file.
        backup_count: The number of backup log files to keep during rotation.

    Returns:
        The configured logger instance named "TradingSystem".
    """
    global _logger_initialized, _trading_system_logger, _log_filename

    # Return existing logger if already initialized
    if _logger_initialized and _trading_system_logger:
        # Update level if called again with a different level
        current_level_name = logging.getLevelName(log_level)
        if _trading_system_logger.level != log_level:
            _trading_system_logger.setLevel(log_level)
            # Also update root logger level if necessary
            logging.getLogger().setLevel(log_level)
            _trading_system_logger.info(f"Log level updated to: {current_level_name}")
        return _trading_system_logger

    # --- Perform Setup Only Once ---
    # Get the root logger
    root_logger = logging.getLogger()
    # Clear existing handlers ONLY during the very first setup
    # This prevents duplicate handlers if setup_logger is called multiple times
    # (though the idempotency check above should prevent this)
    if not root_logger.hasHandlers(): # Check if handlers already exist
        # Set level on root logger - controls the minimum level for all handlers
        root_logger.setLevel(log_level)

        # Create formatter
        log_format = '%(asctime)s - %(name)s - %(levelname)-8s - %(message)s' # Added padding to levelname
        date_format = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(log_format, datefmt=date_format)

        # Create Console Handler (if enabled)
        if log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            # Set level for this handler individually if needed (e.g., console only shows INFO+)
            # console_handler.setLevel(logging.INFO)
            root_logger.addHandler(console_handler)

        # Create Rotating File Handler (if enabled)
        if log_to_file:
            try:
                # Ensure log directory exists
                os.makedirs(log_directory, exist_ok=True)

                # Create log file name (based on date for daily rotation)
                # Note: TimedRotatingFileHandler handles the date in the filename automatically if using suffix
                base_log_filename = os.path.join(log_directory, "trading_system.log")
                _log_filename = base_log_filename # Store the base filename

                # Rotates at midnight, keeps 'backup_count' old logs, appends date to backups
                file_handler = TimedRotatingFileHandler(
                    _log_filename,
                    when="midnight",      # Rotate daily
                    interval=1,           # Check interval (1 day)
                    backupCount=backup_count, # Number of backups to keep
                    encoding='utf-8',     # Specify encoding
                    delay=False,          # Create log file immediately
                    utc=True              # Use UTC for rotation time
                )
                file_handler.setFormatter(formatter)
                # Use suffix for rotated files (e.g., trading_system.log.2023-10-27)
                file_handler.suffix = "%Y-%m-%d"
                root_logger.addHandler(file_handler)
            except Exception as e:
                 print(f"Error setting up file logging to {log_directory}: {e}", file=sys.stderr)
                 # Continue without file logging if it fails

        # Silence noisy libraries (set their level higher than the root logger's level if needed)
        logging.getLogger('ib_insync').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        # Add others as needed, e.g., logging.getLogger('matplotlib').setLevel(logging.WARNING)

    # Get the specific logger for the trading system
    # This logger will inherit the level and handlers from the root logger
    logger_instance = logging.getLogger("TradingSystem")

    # Attach log filename to the logger instance for reference elsewhere
    if _log_filename:
         setattr(logger_instance, 'log_filename', _log_filename)

    # Log initialization message only once
    if not _logger_initialized:
        logger_instance.info(f"Logger initialized. Level: {logging.getLevelName(log_level)}. "
                             f"Console: {'Yes' if log_to_console else 'No'}, "
                             f"File: {'Yes (' + _log_filename + ')' if log_to_file and _log_filename else 'No'}")
        _logger_initialized = True

    _trading_system_logger = logger_instance
    return logger_instance

# --- Make logger accessible globally after first setup ---
# Call setup_logger here with default settings or load from config if needed early
# Example: logger = setup_logger(log_level=logging.INFO)
# The instance returned by subsequent calls will be the same configured logger.
# We initialize it here so other modules can just import 'logger'
logger = setup_logger()