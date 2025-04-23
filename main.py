# filename: main.py
# main.py (Revised: Asynchronous startup using Event Queue)
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
# --- ADDED FOR DEBUGGING ---
logger.info(f"[main.py] Event Queue Object ID: {id(event_queue)}")
# --- END DEBUGGING ---
# MODIFICATION: Import the new event
from core.events import (
    ShutdownEvent, MarketEvent, SignalEvent, OrderEvent, FillEvent,
    OrderFailedEvent, ContractQualificationCompleteEvent,
    InternalFillProcessedEvent
)
from data.ib_handler import IBDataHandler # Assumes this now has start_async_contract_qualification
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

    # --- Component Instances ---
    data_handler: Optional[IBDataHandler] = None
    executor: Optional[IBExecutionHandler] = None
    portfolio: Optional[LivePortfolioManager] = None
    performance_tracker: Optional[PerformanceTracker] = None
    strategies: List = []
    config: Dict = {}
    benchmark_daily_returns: Optional[pd.Series] = None
    calculate_benchmark_metrics = False

    # --- MODIFICATION: State flags for async startup ---
    qualification_started = False
    qualification_complete = False
    post_qualification_sync_started = False
    system_ready = False # Flag to indicate all sync steps are done
    # --- END MODIFICATION ---

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
        # --- MODIFICATION START: Load performance target check interval ---
        perf_target_check_interval = int(config.get('monitoring', {}).get('perf_target_check_interval_seconds', 86400)) # Default to daily
        perf_targets = config.get('performance', {}).get('targets', {}) # Get the targets dict
        # --- MODIFICATION END ---
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
        # Assumes IBDataHandler __init__ now registers permanent listeners
        data_handler = IBDataHandler(event_queue, symbols=symbols_to_trade, ib_wrapper=ib_wrapper)
        # --- ADDED FOR DEBUGGING ---
        logger.info(f"[main.py run_trading_system] Data Handler's Event Queue ID: {id(data_handler.event_queue)}")
        # --- END DEBUGGING ---
        executor = IBExecutionHandler(event_queue, ib_wrapper=ib_wrapper, data_handler=data_handler, config=config)

        strategy_configs = config.get('strategies', {})
        strategy_atr_config = config.get('trading', {}).get('atr_stop', {})
        # ... (strategy loading logic remains the same) ...
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
        logger.info("IB Connection Confirmed. Starting asynchronous component synchronization...")

        # --- MODIFICATION: Start Async Qualification ---
        # 1. Start Asynchronous Contract Qualification (Non-blocking)
        logger.info("Initiating asynchronous contract qualification...")
        # Assumes data_handler now has this non-blocking method
        data_handler.start_async_contract_qualification()
        qualification_started = True
        # REMOVED: Blocking wait: data_handler.wait_for_contract_qualification(...)
        logger.info("Contract qualification requests sent. Waiting for completion event...")
        # --- END MODIFICATION ---

        # --- NOTE: Steps 2, 3, and Benchmark Loading are now deferred ---
        # --- They will be triggered inside the event loop ---

        # --- Main Event Loop ---
        last_periodic_task_time = time.monotonic()
        # --- Add last OER check time tracking if checking here ---
        # last_oer_check_time = time.monotonic() # Or rely on executor's internal timer

        # --- MODIFICATION START: Initialize last target check time ---
        last_perf_target_check_time = time.monotonic()
        # --- MODIFICATION END ---
        logger.info("=== Starting Main Event Loop (Awaiting Qualification Completion) ===")
        while not shutdown_signal_received:
            try:
                event = None
                try: event = event_queue.get(block=True, timeout=0.1)
                except queue.Empty:
                    if shutdown_signal_received: break
                    # --- Periodic Tasks moved here to run even if queue is empty ---
                    now_mono = time.monotonic()
                    # Standard periodic check
                    if now_mono - last_periodic_task_time > periodic_check_interval:
                        logger.debug(f"Performing periodic check (System Ready: {system_ready})...")
                        if ib_wrapper._client_thread and not ib_wrapper._client_thread.is_alive():
                            logger.critical("CRITICAL: IBAPI Network Thread is NOT ALIVE! System cannot function.")
                        else:
                            logger.debug("Periodic check: IBAPI Network Thread appears alive.")

                        if not ib_wrapper.isConnected():
                            logger.critical("Main loop: IB connection LOST. Triggering shutdown.")
                            if portfolio: portfolio.trigger_portfolio_shutdown("IB Connection Lost")
                            else: shutdown_signal_received = True; ib_thread_stop_requested.set()
                        elif system_ready and portfolio and not portfolio.trading_halted:
                            portfolio.request_time_update() # Still useful for IB time sync
                            if portfolio.check_risk_limits():
                                logger.warning("Periodic risk check triggered halt.")
                        last_periodic_task_time = now_mono

                    # --- MODIFICATION START: Performance target check ---
                    # Check performance targets periodically (less frequently)
                    if system_ready and portfolio and performance_tracker and perf_targets.get('enable_periodic_check', False):
                         if now_mono - last_perf_target_check_time > perf_target_check_interval:
                              current_system_date = portfolio.current_time.date() if portfolio.current_time else datetime.date.today()
                              logger.info(f"Performing periodic performance target check (Interval: {perf_target_check_interval}s)...")
                              try:
                                  performance_tracker.check_performance_targets(current_system_date, perf_targets)
                              except Exception as perf_check_exc:
                                  logger.exception(f"Error during periodic performance target check: {perf_check_exc}")
                              last_perf_target_check_time = now_mono
                    # --- MODIFICATION END ---
                    
                    # --- OER Check (if system ready and executor exists) ---
                    if system_ready and executor:
                         executor.check_oer() # Let executor manage its own timing

                    continue # No event, continue loop

                # Process received event
                if event:
                    # --- Update current time ---
                    current_event_time = getattr(event, 'timestamp', portfolio.current_time or datetime.datetime.now(datetime.timezone.utc))
                    # ... (time update logic remains the same) ...
                    if isinstance(current_event_time, datetime.datetime):
                        if current_event_time.tzinfo is None: current_event_time = current_event_time.replace(tzinfo=datetime.timezone.utc)
                        elif current_event_time.tzinfo != datetime.timezone.utc: current_event_time = current_event_time.astimezone(datetime.timezone.utc)
                    else: current_event_time = datetime.datetime.now(datetime.timezone.utc) # Fallback
                    if portfolio and system_ready: # Only update portfolio time if system is ready
                         portfolio.update_time(current_event_time)


                    # --- Event Processing Logic ---
                    try:
                        if isinstance(event, ShutdownEvent):
                            shutdown_signal_received = True
                            logger.critical(f"Shutdown event received: {event.reason}")
                            ib_thread_stop_requested.set()
                            break

                        # --- MODIFICATION: Handle Qualification Complete Event ---
                        elif isinstance(event, ContractQualificationCompleteEvent):
                            logger.info(f"Received {event}")
                            qualification_complete = True
                            if not event.successful_symbols and event.failed_symbols:
                                logger.critical("Contract qualification failed for all symbols. Exiting.")
                                raise SystemExit("No contracts qualified")
                            elif event.failed_symbols:
                                logger.warning(f"Contract qualification failed for: {event.failed_symbols}")

                            # --- Trigger Post-Qualification Sync ---
                            if not post_qualification_sync_started:
                                post_qualification_sync_started = True
                                logger.info("Contract qualification confirmed. Initiating post-qualification sync...")

                                # 2. Start and Wait for Executor and Portfolio Initial Sync
                                logger.info("Initiating executor and portfolio state synchronization...")
                                try:
                                    if executor: executor.start_initial_sync()    # Triggers request open orders
                                    if portfolio: portfolio.start_initial_sync()   # Triggers request account state/positions

                                    portfolio_sync_timeout, executor_sync_timeout = 45.0, 15.0
                                    logger.info(f"Waiting up to {portfolio_sync_timeout}s for Portfolio sync (Account/Pos)...")
                                    portfolio_synced = portfolio.wait_for_initial_sync(timeout=portfolio_sync_timeout) if portfolio else False
                                    logger.info(f"Waiting up to {executor_sync_timeout}s for Executor sync (Open Orders)...")
                                    executor_synced = executor.wait_for_initial_sync(timeout=executor_sync_timeout) if executor else False

                                    if not portfolio_synced or not executor_synced:
                                         logger.critical("Portfolio or Executor initial synchronization timed out/failed. Exiting.")
                                         raise SystemExit("Component Sync Failed")
                                    logger.info("Portfolio and Executor initial sync confirmed.")

                                    # 3. Subscribe to Live Data (Now safe)
                                    logger.info("Subscribing to live market data...")
                                    if not data_handler.subscribe_live_data():
                                        logger.critical("Failed to subscribe to live data after sync. Exiting.")
                                        raise SystemExit("Live Data Subscription Failed")

                                    if portfolio: portfolio.update_daily_state(current_event_time) # Set initial daily state

                                    # 4. Load Benchmark Data (Now safe)
                                    # --- MODIFICATION START: Benchmark loading with enhanced logging ---
                                    if calculate_benchmark_metrics and benchmark_symbol:
                                        logger.info(f"Attempting to load benchmark data for '{benchmark_symbol}' via IBDataHandler...")
                                        try:
                                            benchmark_df = data_handler.request_historical_data_sync( benchmark_symbol, duration='15 Y', bar_size='1 day', use_rth=True, timeout_per_req=180.0)
                                            if benchmark_df is not None and not benchmark_df.empty:
                                                if benchmark_df.index.tz is None: benchmark_df.index = benchmark_df.index.tz_localize('UTC')
                                                elif benchmark_df.index.tz != datetime.timezone.utc: benchmark_df.index = benchmark_df.index.tz_convert('UTC')
                                                benchmark_daily_returns = benchmark_df['close'].pct_change().dropna()
                                                if benchmark_daily_returns.empty:
                                                    logger.warning(f"Benchmark daily returns for '{benchmark_symbol}' empty. Disabling Alpha/Beta.")
                                                    calculate_benchmark_metrics = False
                                                else: logger.info(f"Loaded {len(benchmark_daily_returns)} benchmark returns.")
                                            else: logger.warning(f"Could not load benchmark data '{benchmark_symbol}'. Disabling Alpha/Beta."); calculate_benchmark_metrics = False
                                        except Exception as bench_exc: logger.exception(f"Error loading benchmark data for {benchmark_symbol}: {bench_exc}"); calculate_benchmark_metrics = False
                                    elif calculate_benchmark_metrics and not benchmark_symbol: logger.warning("Benchmark calc enabled, but no symbol. Disabling."); calculate_benchmark_metrics = False
                                    else: logger.info("Benchmark calc (Alpha/Beta) disabled.")
                                    # --- MODIFICATION END ---

                                    # --- System is Ready ---
                                    system_ready = True
                                    logger.info("=== System Ready for Trading ===")

                                except Exception as sync_exc:
                                     logger.exception(f"Critical error during post-qualification sync: {sync_exc}")
                                     raise SystemExit(f"Post-Qualification Sync Error: {sync_exc}")
                            else:
                                logger.warning("Received duplicate ContractQualificationCompleteEvent. Ignoring.")
                        # --- END MODIFICATION ---

                        # --- Guard other event processing until system is ready ---
                        elif not system_ready:
                            logger.debug(f"System not ready, queuing/discarding event: {type(event)}")
                            # Optionally queue events if needed, or just discard until ready

                        elif isinstance(event, MarketEvent):
                            if portfolio: portfolio.update_market_data(event)
                            if not portfolio or not portfolio.trading_halted:
                                for strat in strategies:
                                    if event.symbol in strat.symbols: strat.process_market_event(event)
                        elif isinstance(event, SignalEvent):
                            if portfolio and not portfolio.trading_halted: portfolio.process_signal_event(event)
                        elif isinstance(event, OrderEvent):
                            if executor and not (portfolio and portfolio.trading_halted):
                                executor.process_order_event(event)
                            else: logger.warning(f"Trading halted or executor missing. Dropping order: {event}")
                        elif isinstance(event, FillEvent):
                            if portfolio: portfolio.process_fill_event(event)
                        # --- MODIFICATION: Handle Internal Fill Event ---
                        elif isinstance(event, InternalFillProcessedEvent):
                            if executor:
                                executor.process_internal_fill_event(event)
                        # --- END MODIFICATION ---
                        elif isinstance(event, OrderFailedEvent):
                            if portfolio: portfolio.process_order_failed_event(event)
                            if executor: executor.process_order_failed_event(event)
                        else:
                            logger.warning(f"Unknown event type received: {type(event)} - {event}")

                    except (SystemExit, ConnectionError) as e: # Specific exceptions leading to exit
                        logger.critical(f"System exit triggered during event processing: {e}")
                        shutdown_signal_received = True; ib_thread_stop_requested.set(); break
                    except Exception as e:
                        logger.exception(f"Error processing event type {getattr(event, 'type', 'UNKNOWN')}: {e}")
                        if portfolio and not shutdown_signal_received:
                             portfolio.trigger_portfolio_shutdown(f"Event Error: {e}") # Will put ShutdownEvent on queue
                        elif not shutdown_signal_received:
                             shutdown_signal_received = True; ib_thread_stop_requested.set()
                             logger.critical("Triggering shutdown due to event error (Portfolio unavailable).")

            except KeyboardInterrupt:
                if not shutdown_signal_received:
                    logger.warning("Manual shutdown (KeyboardInterrupt).")
                    shutdown_signal_received = True; ib_thread_stop_requested.set()
                    try: event_queue.put_nowait(ShutdownEvent("KeyboardInterrupt"))
                    except queue.Full: logger.error("Event queue full during KeyboardInterrupt.")
                break # Exit loop on KeyboardInterrupt
            except SystemExit: # Catch SystemExit explicitly to allow clean shutdown
                 logger.warning("SystemExit caught in main loop. Initiating shutdown.")
                 shutdown_signal_received = True; ib_thread_stop_requested.set(); break
            except Exception as e:
                 logger.exception(f"Critical unhandled exception in main loop: {e}")
                 shutdown_signal_received = True; ib_thread_stop_requested.set()
                 if portfolio: portfolio.trigger_portfolio_shutdown(f"Main Loop Exception: {e}")
                 else: logger.critical(f"CRITICAL: Main Loop Exception: {e} (Portfolio unavailable)")
                 break # Exit loop on critical error

        # --- Shutdown Sequence ---
        # ... (Shutdown sequence remains the same) ...
        logger.info("=== Main Loop Exited. Initiating Shutdown Sequence ===")
        # Ensure trading is marked as halted if not already
        if portfolio and not portfolio.trading_halted: portfolio.trading_halted = True

        try:
            # Record final equity
            final_time = datetime.datetime.now(datetime.timezone.utc)
            if system_ready and portfolio and portfolio.portfolio_value is not None:
                performance_tracker.record_equity(final_time, portfolio.portfolio_value)
            elif performance_tracker and not performance_tracker.equity_curve:
                # Record initial capital if system never became ready or no equity curve exists
                performance_tracker.record_equity(final_time, initial_capital)

            # Calculate metrics (only if system was ready and tracker exists)
            if system_ready and performance_tracker:
                logger.info("Calculating final performance metrics...")
                final_metrics = performance_tracker.calculate_metrics(
                    benchmark_returns=benchmark_daily_returns if calculate_benchmark_metrics else None
                )
                results_dir = config.get('logging', {}).get('log_directory', 'logs')
                os.makedirs(results_dir, exist_ok=True)
                performance_tracker.save_results(output_dir=results_dir)
            elif not system_ready:
                 logger.warning("System did not become fully ready. Skipping final performance metrics calculation.")
            else: logger.error("Performance tracker unavailable for final metrics.")
        except Exception as e: logger.exception(f"Error during final metrics/saving: {e}")

        logger.info("Shutting down components...")
        # Use `locals()` to safely check if components were instantiated before shutdown
        if 'executor' in locals() and executor and hasattr(executor, 'shutdown'):
             try: executor.shutdown()
             except Exception as e: logger.exception(f"Error shutting down Executor: {e}")
        if 'portfolio' in locals() and portfolio and hasattr(portfolio, 'shutdown'):
             try: portfolio.shutdown()
             except Exception as e: logger.exception(f"Error shutting down Portfolio: {e}")
        if 'data_handler' in locals() and data_handler and hasattr(data_handler, 'shutdown'):
             try: data_handler.shutdown()
             except Exception as e: logger.exception(f"Error shutting down DataHandler: {e}")


    except (SystemExit, ConnectionError, ValueError, TypeError) as e:
         # Catch errors during initial setup before main loop
         logger.critical(f"System exit during setup phase: {e}")
         # Attempt shutdown even if setup failed partially
         if 'executor' in locals() and executor: executor.shutdown()
         if 'portfolio' in locals() and portfolio: portfolio.shutdown()
         if 'data_handler' in locals() and data_handler: data_handler.shutdown()
    except Exception as e:
         logger.exception(f"Unhandled exception during system setup phase: {e}")
         # Attempt shutdown even if setup failed partially
         if 'portfolio' in locals() and portfolio: portfolio.trigger_portfolio_shutdown(f"Setup Exception: {e}")
         if 'executor' in locals() and executor: executor.shutdown()
         if 'data_handler' in locals() and data_handler: data_handler.shutdown()

    finally:
        logger.info("--- Trading System Process Ending ---")


# --- Main Execution Block ---
# ... (Main execution block remains the same) ...
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    ib_wrapper: Optional[IBWrapper] = None
    try:
        # Setup logger moved inside try block if it depends on config
        # setup_logger() # Assuming this is called elsewhere or implicitly

        ib_wrapper = IBWrapper(event_queue) # Pass event queue here
        config = load_config("config.yaml")
        ib_host = config['ibkr']['host']
        ib_port = config['ibkr'].get('paper_port', 7497) # Default to paper
        ib_client_id = config['ibkr']['client_id']

        logger.info("Attempting to connect to IB...")
        ib_wrapper.start_connection(ib_host, ib_port, ib_client_id)

        if ib_wrapper.isConnected():
             logger.info("IB Connection successful. Starting trading system run...")
             run_trading_system(ib_wrapper)
        else: logger.critical("IB connection failed during initial startup. Exiting.")

    except ConnectionRefusedError: logger.critical("Connection refused. Is TWS/Gateway running and API configured?")
    except ConnectionError as ce: logger.critical(f"IB Connection Error during startup: {ce}")
    except (KeyboardInterrupt, SystemExit): logger.warning("System exit requested during startup/run.")
    except Exception as e: logger.exception("Unhandled exception at top level startup.")
    finally:
        logger.info("System shutdown finalizing in main block.")
        if ib_wrapper and ib_wrapper.isConnected():
             logger.info("Stopping IB connection...")
             ib_wrapper.stop_connection()
        elif ib_wrapper: logger.info("IB Wrapper exists but not connected.")
        else: logger.info("IB Wrapper not initialized.")

        logging.shutdown()
        print("Trading system finished.")
