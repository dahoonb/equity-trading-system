# backtest/main_loop.py (Revised Loop Logic for Fill Timing)
import queue
import datetime
import pandas as pd
import logging
import os # Import os for path joining
import sys

# Import system components
from backtest.data import CSVSimDataHandler
from backtest.portfolio import BacktestPortfolio # Revised portfolio
from backtest.execution import SimulatedExecutionHandler # Revised executor
# Import strategies
from strategy.momentum import MovingAverageCrossoverStrategy
from strategy.mean_reversion import RsiMeanReversionStrategy
# Import core components
from core.event_queue import event_queue # Use shared queue
from core.events import MarketEvent, SignalEvent, OrderEvent, FillEvent, OrderFailedEvent # Added OrderFailedEvent
from utils.logger import setup_logger, logger # Use configured logger
from config_loader import load_config # Import config loader
from performance.tracker import PerformanceTracker # Import tracker directly for benchmark

# Ensure logger is configured
# logger = setup_logger() # Or get it if already configured

def run_backtest(config_path="config.yaml"):
    """
    Runs the event-driven backtest using historical data.

    Args:
        config_path: Path to the configuration YAML file.
    """
    logger.info("--- Starting Backtest Simulation ---")

    # --- Load Configuration ---
    try:
        config = load_config(config_path)
        # Backtest specific config
        csv_dir = config['backtest']['csv_directory']
        symbols = config['trading']['symbols'] # Use same symbols as live
        initial_capital = float(config['account']['initial_capital'])
        start_date_str = config['backtest']['start_date']
        end_date_str = config['backtest']['end_date']
        # Strategy choice (can be made more dynamic)
        strategy_choice = config['backtest'].get('strategy', 'rsi') # Default to rsi
        # Execution simulation config
        commission_per_share = config['backtest'].get('commission_per_share', 0.005)
        min_commission = config['backtest'].get('min_commission', 1.0)
        slippage_pct = config['backtest'].get('slippage_pct', 0.0005)
        # Benchmarking config
        benchmark_symbol = config.get('benchmarking', {}).get('symbol', 'SPY')
        calculate_benchmark_metrics = config.get('benchmarking', {}).get('calculate_alpha_beta', True)
        # Performance tracker config
        risk_free_rate = float(config.get('performance', {}).get('risk_free_rate', 0.0))

        start_dt = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
        end_dt = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)

        logger.info(f"Backtest Period: {start_date_str} to {end_date_str}")
        logger.info(f"Universe: {symbols}")
        logger.info(f"Initial Capital: ${initial_capital:,.2f}")
        logger.info(f"Strategy Choice: {strategy_choice}")
        logger.info(f"Simulated Commission/Share: ${commission_per_share:.4f}, Min: ${min_commission:.2f}")
        logger.info(f"Simulated Slippage: {slippage_pct*100:.3f}%")

    except (KeyError, ValueError, TypeError) as e:
        logger.critical(f"Invalid or missing configuration parameter in {config_path}: {e}")
        return
    except FileNotFoundError:
        logger.critical(f"Configuration file not found at {config_path}")
        return

    # --- Initialize Components ---
    data_handler = CSVSimDataHandler(csv_dir, symbols, event_queue, start_dt, end_dt)
    # Pass necessary params from config to Portfolio
    portfolio = BacktestPortfolio(
        initial_capital=initial_capital,
        settlement_days=config['account']['settlement_days'],
        risk_per_trade=config['trading']['risk_per_trade'],
        max_active_positions=config['trading']['max_active_positions']
    )
    # Pass risk-free rate to internal tracker
    portfolio.performance_tracker.risk_free_rate = risk_free_rate

    executor = SimulatedExecutionHandler(
        event_queue,
        commission_per_share=commission_per_share,
        min_commission=min_commission,
        slippage_pct=slippage_pct
    )

    # Instantiate selected strategy/strategies
    strategies = []
    strategy_configs = config.get('strategies', {})
    if strategy_choice == 'ma_cross' and strategy_configs.get('momentum_ma', {}).get('enabled', True): # Default enabled for chosen strategy
        momentum_config = strategy_configs['momentum_ma']
        strategy_params = {k: v for k, v in momentum_config.items() if k != 'enabled'}
        strategies.append(MovingAverageCrossoverStrategy(symbols, event_queue, **strategy_params))
    elif strategy_choice == 'rsi' and strategy_configs.get('mean_reversion_rsi', {}).get('enabled', True):
        rsi_config = strategy_configs['mean_reversion_rsi']
        strategy_params = {k: v for k, v in rsi_config.items() if k != 'enabled'}
        strategies.append(RsiMeanReversionStrategy(symbols, event_queue, **strategy_params))
    # Add logic for combining strategies if needed later
    else:
         logger.error(f"Strategy '{strategy_choice}' not found or not enabled in config.")
         return

    if not strategies:
        logger.error("No valid strategy selected for backtest.")
        return

    # --- Load Benchmark Data ---
    benchmark_daily_returns = None
    if calculate_benchmark_metrics and benchmark_symbol:
        logger.info(f"Loading benchmark data for {benchmark_symbol}...")
        benchmark_path = os.path.join(csv_dir, f"{benchmark_symbol}.csv")
        try:
            bench_df = pd.read_csv(benchmark_path, index_col='date', parse_dates=True)
            # Filter benchmark data within the backtest date range
            bench_df = bench_df[(bench_df.index >= start_dt) & (bench_df.index <= end_dt)]
            if bench_df.empty:
                 raise ValueError(f"Benchmark data for {benchmark_symbol} is empty after date filtering.")
            bench_df.columns = [x.lower().replace(' ', '_') for x in bench_df.columns]
            close_col = 'adj_close' if 'adj_close' in bench_df.columns else 'close'
            if close_col in bench_df.columns:
                 # Ensure index is UTC DatetimeIndex
                 if bench_df.index.tz is None: bench_df.index = bench_df.index.tz_localize('UTC')
                 else: bench_df.index = bench_df.index.tz_convert('UTC')
                 benchmark_daily_returns = bench_df[close_col].pct_change().dropna()
                 logger.info(f"Loaded {len(benchmark_daily_returns)} benchmark returns for {benchmark_symbol}.")
            else: logger.warning(f"Benchmark file {benchmark_path} missing close/adj_close column.")
        except FileNotFoundError:
            logger.warning(f"Benchmark data file not found: {benchmark_path}. Alpha/Beta metrics disabled.")
        except ValueError as ve:
            logger.warning(f"{ve}. Alpha/Beta metrics disabled.")
        except Exception as e:
            logger.exception(f"Error loading benchmark data {benchmark_symbol}: {e}")

    # --- Main Backtest Loop ---
    bars_processed = 0
    orders_to_execute_next_bar = []
    current_bar_timestamp = None # Initialize

    logger.info("=== Starting Backtest Event Loop ===")
    while True:
        # 1. Stream data for the current bar (Bar N)
        # This puts MarketEvent(s) for Bar N onto the event queue
        has_more_data = data_handler.stream_next_bar()
        if not has_more_data:
            break # End of data

        # Get the timestamp for the *current* bar (Bar N)
        current_bar_timestamp = data_handler.current_bar_time
        if current_bar_timestamp is None: break # Should not happen

        # Log progress periodically
        bars_processed += 1
        if bars_processed % 100 == 0:
             logger.info(f"Processing bar for date: {current_bar_timestamp.strftime('%Y-%m-%d')}")

        # 2. Process settlements due *before* Bar N
        portfolio.update_time(current_bar_timestamp)

        # 3. Execute orders generated based on Bar N-1
        # Use the open prices from Bar N (current bar) stored in the executor
        if orders_to_execute_next_bar:
            logger.debug(f"[{current_bar_timestamp.strftime('%Y-%m-%d')}] Executing {len(orders_to_execute_next_bar)} order(s) from previous bar.")
            for order in orders_to_execute_next_bar:
                 executor.process_order(order, current_bar_timestamp) # Fills using next_bar_open (set below)
            orders_to_execute_next_bar.clear() # Clear the list after processing

        # 4. Process events generated by fills (from N-1 orders) or this bar's MarketEvent
        # This step processes MarketEvent N, generates Signal N, and queues Order N
        # It also processes Fill Events from orders placed based on N-1
        while True:
            try:
                event = event_queue.get(block=False)
            except queue.Empty:
                break # No more events for this cycle

            if event.type == 'MARKET':
                # Update portfolio's last known price (for sizing) using current bar's close
                if 'close' in event.data: portfolio.update_market_price(event.symbol, event.data['close'])
                # Pass to strategy for signal generation based on Bar N's data (e.g., close)
                for strategy in strategies:
                     if event.symbol in strategy.symbols:
                         strategy.process_market_event(event) # Generates SignalEvents

                # --- IMPORTANT: Update Executor with Bar N's open for NEXT iteration's fills ---
                if 'open' in event.data:
                     executor.update_next_bar_open(event.symbol, event.data['open'])

            elif event.type == 'SIGNAL':
                # Pass signal to portfolio (generates OrderEvent if valid)
                portfolio.process_signal(event) # Generates OrderEvents and puts on queue

            elif event.type == 'ORDER':
                # OrderEvent generated based on Bar N data. Queue it for execution at Bar N+1 open.
                logger.debug(f"[{current_bar_timestamp.strftime('%Y-%m-%d')}] Queuing order based on current bar data for next bar execution: {event}")
                orders_to_execute_next_bar.append(event)

            elif event.type == 'FILL':
                # Process fills immediately (these are fills from orders based on Bar N-1 executed at Bar N open)
                portfolio.process_fill(event)
                # Update strategy's view of position based on fill
                for strategy in strategies:
                     if event.symbol in strategy.symbols:
                         strategy.update_position(event.symbol, portfolio.holdings.get(event.symbol, {}).get('quantity', 0.0))

            elif event.type == 'ORDER_FAILED':
                 # Handle order failure (e.g., log it, maybe update strategy state)
                 logger.warning(f"Order Failed Event received by backtest loop: {event}")
                 # Optionally notify portfolio/strategy if needed

        # End of processing events for Bar N. Loop continues to stream Bar N+1.


    # --- Backtest Finished ---
    logger.info("Backtest finished processing all bars.")

    # --- Final Portfolio Value Update ---
    # Use the last processed bar's timestamp and prices to mark-to-market
    final_timestamp = current_bar_timestamp or end_dt # Use last bar time or configured end date
    logger.info(f"Calculating final portfolio value at {final_timestamp.strftime('%Y-%m-%d')}...")
    portfolio.update_time(final_timestamp) # Process final settlements
    # Update with last known closing prices before final calculation
    for symbol, df in data_handler.symbol_data.items():
         if not df.empty:
             # Use the close of the last available bar for that symbol
             last_valid_index = df.index[-1]
             portfolio.update_market_price(symbol, df['close'].loc[last_valid_index])
    portfolio._update_portfolio_value() # Final MTM and equity recording

    # --- Calculate and Print Final Metrics ---
    logger.info("Calculating final performance metrics...")
    try:
        tracker = portfolio.get_performance_tracker()
        final_metrics = tracker.calculate_metrics(
            benchmark_returns=benchmark_daily_returns # Pass benchmark returns if loaded
        )
        # Save results
        results_dir = config.get('backtest', {}).get('results_directory', 'results')
        tracker.save_results(output_dir=results_dir)
        logger.info(f"Performance results saved to '{results_dir}'.")

    except Exception as e:
        logger.exception("Error during final metrics calculation or saving.")

    logger.info("--- Backtest Simulation Complete ---")


if __name__ == '__main__':
    config_file = "config.yaml"
    if not os.path.exists(config_file):
         print(f"ERROR: {config_file} not found.")
         print("Please create config.yaml with a 'backtest' section.")
         sys.exit(1)

    try:
        run_backtest(config_path=config_file)
    except Exception as e:
        # Use logger if available, otherwise print
        log_func = logger.exception if logger else print
        log_func("Unhandled exception during backtest run.")
        sys.exit(1)