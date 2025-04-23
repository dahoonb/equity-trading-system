# filename: main.py
# main.py (Revised: Pass data_handler to executor)
import sys
import os
import platform
import logging
# Setup logger first
from utils.logger import setup_logger, logger

import time
import queue
import signal
import datetime
import pandas as pd
import threading
from typing import Optional, List, Dict, Any

# Import system components
from config_loader import load_config
from core.ib_wrapper import IBWrapper
from core.event_queue import event_queue
from core.events import ShutdownEvent, MarketEvent, SignalEvent, OrderEvent, FillEvent, OrderFailedEvent
from data.ib_handler import IBDataHandler
from strategy.momentum import MovingAverageCrossoverStrategy
from strategy.mean_reversion import RsiMeanReversionStrategy
from portfolio.live_manager import LivePortfolioManager
from execution.ib_executor import IBExecutionHandler
from performance.tracker import PerformanceTracker

# --- Global Shutdown Flag ---
shutdown_signal_received = False
ib_thread_stop_requested = threading.Event() # Event to signal IB thread to stop

def signal_handler(signum, frame):
    """Handles OS signals for graceful shutdown."""
    global shutdown_signal_received, logger, ib_thread_stop_requested
    if not shutdown_signal_received:
        log_func = logger.warning if logger else print
        log_func(f"Received OS signal {signum}. Initiating graceful shutdown...")
        shutdown_signal_received = True
        ib_thread_stop_requested.set() # Signal IB thread to stop
        try: event_queue.put_nowait(ShutdownEvent(f"OS Signal {signum}"))
        except queue.Full: log_func("Event queue full during shutdown signal.")
        except Exception as e: log_func(f"Error putting shutdown event on queue: {e}")
    else: log_func = logger.warning if logger else print; log_func("Shutdown already in progress.")

def run_trading_system(ib_wrapper: IBWrapper):
    """Main function to run the trading system using ibapi."""
    global shutdown_signal_received, logger

    data_handler: Optional[IBDataHandler] = None
    executor: Optional[IBExecutionHandler] = None
    portfolio: Optional[LivePortfolioManager] = None
    performance_tracker: Optional[PerformanceTracker] = None
    strategies: List = []
    config: Dict = {}
    benchmark_daily_returns: Optional[pd.Series] = None
    calculate_benchmark_metrics = False

    try:
        # --- Load Configuration ---
        config = load_config("config.yaml")
        log_level_str = config.get('logging', {}).get('log_level', 'INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        logger.setLevel(log_level)
        logger.info(f"--- Starting Trading System (ibapi Mode) ---")
        logger.info(f"Log Level: {log_level_str}")
        logger.info(f"Using Log File: {getattr(logger, 'log_filename', 'N/A')}") # type: ignore

        # --- Extract Config Parameters ---
        symbols_to_trade = config['trading']['symbols']
        initial_capital = float(config['account']['initial_capital'])
        risk_free_rate = float(config.get('performance', {}).get('risk_free_rate', 0.0))
        periodic_check_interval = int(config.get('monitoring', {}).get('check_interval_seconds', 30))
        benchmark_symbol = config.get('benchmarking', {}).get('symbol', 'SPY')
        calculate_benchmark_metrics = config.get('benchmarking', {}).get('calculate_alpha_beta', True)
        if not isinstance(symbols_to_trade, list) or not all(isinstance(s, str) for s in symbols_to_trade):
             raise TypeError("Trading symbols must be a list of strings.")
        logger.info(f"Trading Symbols: {symbols_to_trade}")
        logger.info(f"Initial Capital (Config): ${initial_capital:,.2f}")
        logger.info(f"Risk-Free Rate: {risk_free_rate*100:.2f}%")
        logger.info(f"Periodic Check Interval: {periodic_check_interval} seconds")
        logger.info(f"Benchmark Symbol: {benchmark_symbol if calculate_benchmark_metrics else 'N/A'}")

        # --- Instantiate Components (without immediate API calls in init) ---
        performance_tracker = PerformanceTracker(initial_capital, risk_free_rate=risk_free_rate)
        data_handler = IBDataHandler(event_queue, symbols=symbols_to_trade, ib_wrapper=ib_wrapper)
        # --- FIX: Pass data_handler to executor ---
        executor = IBExecutionHandler(event_queue, ib_wrapper=ib_wrapper, data_handler=data_handler)

        strategy_configs = config.get('strategies', {})
        strategy_atr_config = config.get('trading', {}).get('atr_stop', {})
        if strategy_configs.get('momentum_ma', {}).get('enabled', False):
            momentum_config = strategy_configs['momentum_ma']
            strategy_params = {**strategy_atr_config, **momentum_config}; strategy_params.pop('enabled', None)
            strategies.append(MovingAverageCrossoverStrategy(symbols=symbols_to_trade, event_q=event_queue, **strategy_params))
            logger.info(f"Loaded MovingAverageCrossoverStrategy with params: {strategy_params}")
        if strategy_configs.get('mean_reversion_rsi', {}).get('enabled', False):
            rsi_config = strategy_configs['mean_reversion_rsi']
            strategy_params = {**strategy_atr_config, **rsi_config}; strategy_params.pop('enabled', None)
            strategies.append(RsiMeanReversionStrategy(symbols=symbols_to_trade, event_q=event_queue, **strategy_params))
            logger.info(f"Loaded RsiMeanReversionStrategy with params: {strategy_params}")
        if not strategies: logger.critical("No strategies loaded! Exiting."); raise SystemExit("No strategies")
        logger.info(f"Total strategies loaded: {len(strategies)}")

        portfolio = LivePortfolioManager(ib_wrapper, data_handler, event_queue, strategies, config, performance_tracker)

        # --- Initial Sync (AFTER connection is confirmed) ---
        logger.info("IB Connection Confirmed. Starting component synchronization...")

        # 1. Start and Wait for Contract Qualification (Data Handler)
        logger.info("Initiating contract qualification...")
        data_handler.start_initial_sync() # Triggers qualification
        if not data_handler.wait_for_contract_qualification(timeout=30.0):
             logger.warning("Contract qualification failed/timed out. Some symbols may not trade.")
             if not data_handler.contracts: logger.critical("No contracts qualified. Exiting."); raise SystemExit("No contracts")

        # 2. Start and Wait for Executor and Portfolio Initial Sync
        logger.info("Initiating executor and portfolio state synchronization...")
        executor.start_initial_sync()    # Triggers request open orders
        portfolio.start_initial_sync()   # Triggers request account state/positions

        portfolio_sync_timeout, executor_sync_timeout = 20.0, 15.0
        logger.info(f"Waiting up to {portfolio_sync_timeout}s for Portfolio sync (Account/Pos)...")
        portfolio_synced = portfolio.wait_for_initial_sync(timeout=portfolio_sync_timeout)
        logger.info(f"Waiting up to {executor_sync_timeout}s for Executor sync (Open Orders)...")
        executor_synced = executor.wait_for_initial_sync(timeout=executor_sync_timeout)

        if not portfolio_synced or not executor_synced:
             logger.critical("Portfolio or Executor initial synchronization timed out/failed. Exiting.")
             raise SystemExit("Component Sync Failed")
        logger.info("Portfolio and Executor initial sync confirmed.")

        # 3. Subscribe to Live Data (Now safe)
        logger.info("Subscribing to live market data...")
        if not data_handler.subscribe_live_data():
            logger.critical("Failed to subscribe to live data. Exiting.")
            raise SystemExit("Live Data Subscription Failed")
        logger.info("Initial setup and component synchronization complete.")
        portfolio.update_daily_state(datetime.datetime.now(datetime.timezone.utc)) # Set initial daily state

        # --- Load Benchmark Data ---
        if calculate_benchmark_metrics and benchmark_symbol:
             logger.info(f"Loading benchmark data for {benchmark_symbol}...")
             try:
                 benchmark_df = data_handler.request_historical_data_sync(
                     benchmark_symbol, duration='15 Y', bar_size='1 day', use_rth=True
                 )
                 if benchmark_df is not None and not benchmark_df.empty:
                     if benchmark_df.index.tz is None: benchmark_df.index = benchmark_df.index.tz_localize('UTC')
                     elif benchmark_df.index.tz != datetime.timezone.utc: benchmark_df.index = benchmark_df.index.tz_convert('UTC')
                     benchmark_daily_returns = benchmark_df['close'].pct_change().dropna()
                     logger.info(f"Loaded {len(benchmark_daily_returns)} benchmark daily returns.")
                 else:
                     logger.warning(f"Could not load benchmark data for {benchmark_symbol}."); calculate_benchmark_metrics = False
             except Exception as e: logger.exception(f"Error loading benchmark {benchmark_symbol}: {e}"); calculate_benchmark_metrics = False

        # --- Main Event Loop ---
        last_periodic_task_time = time.monotonic()
        logger.info("=== Starting Main Event Loop ===")
        while not shutdown_signal_received:
            try:
                event = None
                try: event = event_queue.get(block=True, timeout=0.1)
                except queue.Empty:
                    if shutdown_signal_received: break; continue

                if event:
                    current_event_time = getattr(event, 'timestamp', portfolio.current_time or datetime.datetime.now(datetime.timezone.utc))
                    if isinstance(current_event_time, datetime.datetime):
                        if current_event_time.tzinfo is None: current_event_time = current_event_time.replace(tzinfo=datetime.timezone.utc)
                        elif current_event_time.tzinfo != datetime.timezone.utc: current_event_time = current_event_time.astimezone(datetime.timezone.utc)
                    else: current_event_time = datetime.datetime.now(datetime.timezone.utc) # Fallback

                    if portfolio: portfolio.update_time(current_event_time)

                    try:
                        if isinstance(event, ShutdownEvent):
                            shutdown_signal_received = True; logger.critical(f"Shutdown event received: {event.reason}"); ib_thread_stop_requested.set(); break
                        elif isinstance(event, MarketEvent):
                            if portfolio: portfolio.update_market_data(event)
                            if not portfolio or not portfolio.trading_halted:
                                for strat in strategies:
                                    if event.symbol in strat.symbols: strat.process_market_event(event)
                        elif isinstance(event, SignalEvent):
                            if not portfolio or not portfolio.trading_halted: portfolio.process_signal_event(event)
                        elif isinstance(event, OrderEvent):
                            if not portfolio or not portfolio.trading_halted:
                                if executor: executor.process_order_event(event)
                            else: logger.warning(f"Trading halted. Dropping order: {event}")
                        elif isinstance(event, FillEvent):
                            if portfolio: portfolio.process_fill_event(event)
                            # if executor: executor.process_fill_event(event) # Executor might observe too
                        elif isinstance(event, OrderFailedEvent):
                            if portfolio: portfolio.process_order_failed_event(event)
                            if executor: executor.process_order_failed_event(event)
                        else: logger.warning(f"Unknown event type: {type(event)} - {event}")
                    except Exception as e:
                        logger.exception(f"Error processing event type {getattr(event, 'type', 'UNKNOWN')}: {e}")
                        if portfolio: portfolio.trigger_portfolio_shutdown(f"Event Error: {e}")
                        else: shutdown_signal_received = True; ib_thread_stop_requested.set()

                # Periodic Tasks
                now_mono = time.monotonic()
                if now_mono - last_periodic_task_time > periodic_check_interval:
                    logger.debug(f"Performing periodic check...")
                    if not ib_wrapper.isConnected():
                        logger.critical("Main loop: IB connection LOST. Triggering shutdown.")
                        if portfolio: portfolio.trigger_portfolio_shutdown("IB Connection Lost")
                        else: shutdown_signal_received = True; ib_thread_stop_requested.set()
                    elif portfolio and not portfolio.trading_halted:
                        portfolio.request_time_update()
                        if portfolio.check_risk_limits(): logger.warning("Periodic risk check triggered halt.")
                    last_periodic_task_time = now_mono

            except KeyboardInterrupt:
                if not shutdown_signal_received: logger.warning("Manual shutdown (KeyboardInterrupt)."); shutdown_signal_received = True; ib_thread_stop_requested.set();
                try: event_queue.put_nowait(ShutdownEvent("KeyboardInterrupt"))
                except queue.Full: logger.error("Event queue full during KeyboardInterrupt.")
            except Exception as e:
                logger.exception(f"Critical unhandled exception in main loop: {e}")
                shutdown_signal_received = True; ib_thread_stop_requested.set()
                if portfolio: portfolio.trigger_portfolio_shutdown(f"Main Loop Exception: {e}")
                else: logger.critical(f"CRITICAL: Main Loop Exception: {e} (Portfolio unavailable)")
                break

        # --- Shutdown Sequence ---
        logger.info("=== Main Loop Exited. Initiating Shutdown Sequence ===")
        if portfolio: portfolio.trading_halted = True
        try:
            final_time = datetime.datetime.now(datetime.timezone.utc)
            if portfolio and portfolio.portfolio_value is not None: performance_tracker.record_equity(final_time, portfolio.portfolio_value)
            elif performance_tracker and not performance_tracker.equity_curve: performance_tracker.record_equity(final_time, initial_capital)
            logger.info("Calculating final performance metrics...")
            if performance_tracker:
                 final_metrics = performance_tracker.calculate_metrics(benchmark_returns=benchmark_daily_returns if calculate_benchmark_metrics else None)
                 results_dir = config.get('logging', {}).get('log_directory', 'logs') # Save results near logs
                 os.makedirs(results_dir, exist_ok=True)
                 performance_tracker.save_results(output_dir=results_dir)
            else: logger.error("Performance tracker unavailable for final metrics.")
        except Exception as e: logger.exception(f"Error during final metrics/saving: {e}")

        logger.info("Shutting down components...")
        if executor and hasattr(executor, 'shutdown'):
             try: executor.shutdown()
             except Exception as e: logger.exception(f"Error shutting down Executor: {e}")
        if portfolio and hasattr(portfolio, 'shutdown'):
             try: portfolio.shutdown()
             except Exception as e: logger.exception(f"Error shutting down Portfolio: {e}")
        if data_handler and hasattr(data_handler, 'shutdown'):
             try: data_handler.shutdown()
             except Exception as e: logger.exception(f"Error shutting down DataHandler: {e}")

    except (SystemExit, ConnectionError, ValueError, TypeError) as e:
         logger.critical(f"System exit during setup/sync: {e}")
         if 'executor' in locals() and executor: executor.shutdown()
         if 'portfolio' in locals() and portfolio: portfolio.shutdown()
         if 'data_handler' in locals() and data_handler: data_handler.shutdown()
    except Exception as e:
         logger.exception(f"Unhandled exception during system setup/sync: {e}")
         if 'portfolio' in locals() and portfolio: portfolio.trigger_portfolio_shutdown(f"Setup Exception: {e}")
         if 'executor' in locals() and executor: executor.shutdown()
         if 'data_handler' in locals() and data_handler: data_handler.shutdown()

    finally:
        logger.info("--- Trading System Process Ending ---")

# --- Main Execution Block ---
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    ib_wrapper: Optional[IBWrapper] = None
    try:
        ib_wrapper = IBWrapper(event_queue)
        config = load_config("config.yaml")
        ib_host = config['ibkr']['host']
        ib_port = config['ibkr'].get('paper_port', 7497) # Default to paper
        ib_client_id = config['ibkr']['client_id']
        ib_wrapper.start_connection(ib_host, ib_port, ib_client_id)
        if ib_wrapper.isConnected(): run_trading_system(ib_wrapper)
        else: logger.critical("IB connection failed during initial startup.")
    except ConnectionRefusedError: logger.critical("Connection refused. TWS/Gateway running and API configured?")
    except ConnectionError as ce: logger.critical(f"IB Connection Error during startup: {ce}")
    except (KeyboardInterrupt, SystemExit): logger.warning("System exit requested during startup/run.")
    except Exception as e: logger.exception("Unhandled exception at top level startup.")
    finally:
        logger.info("System shutdown finalizing in main block.")
        if ib_wrapper: ib_wrapper.stop_connection()
        logging.shutdown()
        print("Trading system finished.")