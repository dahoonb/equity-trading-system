# filename: backtest/main_loop.py
# backtest/main_loop.py (Revised Loop Logic for Fill Timing)
import queue
import datetime
import pandas as pd
import pandas as np
import logging
import os # Import os for path joining
import sys
import itertools # For parameter combinations
from pandas.tseries.offsets import DateOffset # For walk-forward periods

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

def _run_single_backtest(
    config: dict,
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    strategy_params: dict, # Pass specific parameters for this run
    run_label: str = "single_run" # Label for output files/logs
    ) -> dict:
    """
    Encapsulates the logic to run one backtest for a specific period and parameter set.

    Args:
        config: The main configuration dictionary.
        start_date: Start date for this specific backtest run.
        end_date: End date for this specific backtest run.
        strategy_params: Dictionary of parameters for the strategy instance.
        run_label: A label for logging and results saving.

    Returns:
        A dictionary containing performance metrics for this run.
    """
    logger.info(f"--- Starting Single Backtest Run: {run_label} ---")
    logger.info(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Strategy Params: {strategy_params}")

    # --- Clear Event Queue ---
    while not event_queue.empty():
        try: event_queue.get_nowait()
        except queue.Empty: break

    # --- Load Config (Extract relevant parts) ---
    # Already loaded, but extract specific settings needed here
    csv_dir = config['backtest']['csv_directory']
    symbols = config['trading']['symbols']
    initial_capital = float(config['account']['initial_capital'])
    strategy_choice = config['backtest'].get('strategy', 'rsi') # Determine strategy type
    commission_per_share = config['backtest'].get('commission_per_share', 0.005)
    min_commission = config['backtest'].get('min_commission', 1.0)
    slippage_pct = config['backtest'].get('slippage_pct', 0.0005)
    benchmark_symbol = config.get('benchmarking', {}).get('symbol', 'SPY')
    calculate_benchmark_metrics = config.get('benchmarking', {}).get('calculate_alpha_beta', True)
    risk_free_rate = float(config.get('performance', {}).get('risk_free_rate', 0.0))

    # --- Initialize Components FOR THIS RUN ---
    # Ensure components are fresh for each run
    data_handler = CSVSimDataHandler(csv_dir, symbols, event_queue, start_date, end_date)
    portfolio = BacktestPortfolio(
        initial_capital=initial_capital,
        settlement_days=config['account']['settlement_days'],
        risk_per_trade=config['trading']['risk_per_trade'], # Use global config for risk rules
        max_active_positions=config['trading']['max_active_positions']
    )
    portfolio.performance_tracker.risk_free_rate = risk_free_rate
    executor = SimulatedExecutionHandler(
        event_queue,
        commission_per_share=commission_per_share,
        min_commission=min_commission,
        slippage_pct=slippage_pct
    )

    # Instantiate the correct strategy with the provided strategy_params
    strategies = []
    if strategy_choice == 'ma_cross':
        strategies.append(MovingAverageCrossoverStrategy(symbols, event_queue, **strategy_params))
    elif strategy_choice == 'rsi':
        strategies.append(RsiMeanReversionStrategy(symbols, event_queue, **strategy_params))
    else:
        logger.error(f"Unsupported strategy choice '{strategy_choice}' in _run_single_backtest")
        return {"Error": "Unsupported Strategy"}

    # --- Load Benchmark Data (for this period only) ---
    benchmark_daily_returns = None
    if calculate_benchmark_metrics and benchmark_symbol:
        # (Simplified benchmark loading - use existing robust logic but filter dates)
        try:
            benchmark_path = os.path.join(csv_dir, f"{benchmark_symbol}.csv")
            bench_df_full = pd.read_csv(benchmark_path, index_col='date', parse_dates=True)
            bench_df = bench_df_full[(bench_df_full.index >= start_date) & (bench_df_full.index <= end_date)]
            if not bench_df.empty:
                 # ... (rest of benchmark processing logic as before) ...
                 close_col = 'adj_close' if 'adj_close' in bench_df.columns else 'close'
                 if close_col in bench_df.columns:
                     if bench_df.index.tz is None: bench_df.index = bench_df.index.tz_localize('UTC')
                     else: bench_df.index = bench_df.index.tz_convert('UTC')
                     benchmark_daily_returns = bench_df[close_col].pct_change().dropna()
                     if benchmark_daily_returns.empty: calculate_benchmark_metrics = False
            else: calculate_benchmark_metrics = False
        except Exception as e: logger.error(f"Error loading benchmark data for {run_label}: {e}"); calculate_benchmark_metrics = False
        # --- (End Simplified benchmark loading) ---


    # --- Run the Event Loop (Core logic extracted from original run_backtest) ---
    bars_processed = 0
    orders_to_execute_next_bar = []
    current_bar_timestamp = None
    while True:
        has_more_data = data_handler.stream_next_bar()
        if not has_more_data: break
        current_bar_timestamp = data_handler.current_bar_time
        if current_bar_timestamp is None: break
        bars_processed += 1
        portfolio.update_time(current_bar_timestamp)
        if orders_to_execute_next_bar:
            for order in orders_to_execute_next_bar:
                executor.process_order(order, current_bar_timestamp)
            orders_to_execute_next_bar.clear()
        while True:
            try: event = event_queue.get(block=False)
            except queue.Empty: break
            # --- Process event types (MARKET, SIGNAL, ORDER, FILL, ORDER_FAILED) ---
            # ... (This core event processing logic remains largely the same) ...
            if event.type == 'MARKET':
                if 'close' in event.data: portfolio.update_market_price(event.symbol, event.data['close'])
                for strategy in strategies:
                     if event.symbol in strategy.symbols: strategy.process_market_event(event)
                if 'open' in event.data: executor.update_next_bar_data(event.symbol, event.data.get('open'), event.data.get('high'), event.data.get('low'))
            elif event.type == 'SIGNAL':
                portfolio.process_signal(event)
            elif event.type == 'ORDER':
                orders_to_execute_next_bar.append(event)
            elif event.type == 'FILL':
                portfolio.process_fill(event)
                for strategy in strategies:
                     if event.symbol in strategy.symbols:
                         strategy.update_position(event.symbol, portfolio.holdings.get(event.symbol, {}).get('quantity', 0.0))
            elif event.type == 'ORDER_FAILED':
                 logger.warning(f"Order Failed Event ({run_label}): {event}")

    logger.info(f"Run {run_label}: Backtest loop finished. Processed {bars_processed} bars.")

    # --- Final Updates and Metric Calculation ---
    final_timestamp = current_bar_timestamp or end_date
    portfolio.update_time(final_timestamp)
    for symbol, df in data_handler.symbol_data.items():
         if not df.empty:
             last_valid_index = df.index[-1]
             portfolio.update_market_price(symbol, df['close'].loc[last_valid_index])
    portfolio._update_portfolio_value()

    logger.info(f"Run {run_label}: Calculating final metrics...")
    tracker = portfolio.get_performance_tracker()
    try:
        final_metrics = tracker.calculate_metrics(benchmark_daily_returns if calculate_benchmark_metrics else None)
        # --- Save Results for this Specific Run ---
        results_dir_base = config.get('backtest', {}).get('results_directory', 'results')
        run_results_dir = os.path.join(results_dir_base, run_label) # Save to sub-directory
        tracker.save_results(output_dir=run_results_dir)
        logger.info(f"Run {run_label}: Performance results saved to '{run_results_dir}'.")
        daily_equity_df = tracker.get_daily_equity_dataframe()
        return {"metrics": final_metrics, "daily_equity": daily_equity_df}
    except Exception as e:
        logger.exception(f"Run {run_label}: Error during final metrics calculation or saving.")
        return {"metrics": {"Error": f"Metrics calculation failed: {e}"}, "daily_equity": None}

def run_walk_forward_analysis(config_path="config.yaml"):
    """
    Performs walk-forward analysis by optimizing parameters on in-sample periods
    and testing them on subsequent out-of-sample periods.
    """
    logger.info("--- Starting Walk-Forward Analysis ---")
    config = load_config(config_path)
    wf_config = config.get('backtest', {}).get('walk_forward', {})

    if not wf_config.get('enabled', False):
        logger.warning("Walk-forward analysis is not enabled in config. Exiting.")
        return

    try:
        # Parse periods and parameters
        overall_start = pd.Timestamp(config['backtest']['start_date'], tz='UTC')
        overall_end = pd.Timestamp(config['backtest']['end_date'], tz='UTC')
        is_offset = pd.tseries.frequencies.to_offset(wf_config['in_sample_period'])
        oos_offset = pd.tseries.frequencies.to_offset(wf_config['out_of_sample_period'])
        opt_metric_key_raw = wf_config['optimization_metric']
        strategy_choice = config['backtest']['strategy']
        param_ranges = wf_config.get('parameter_ranges', {}).get(strategy_choice, {})

        # Map optimization metric key from config to the key used in calculate_metrics results
        # This needs adjustment based on the EXACT keys returned by calculate_metrics
        metric_map = {
            "sharpe_ratio": "sharpe_numeric", # Use the direct numeric key
            "cagr": "cagr_numeric",
            "calmar": "calmar_numeric",
            "profit_factor": "profit_factor_numeric"
            # Add other metrics if needed
        }
        optimization_metric_key = metric_map.get(opt_metric_key_raw)
        if not optimization_metric_key:
             logger.critical(f"Invalid optimization_metric '{opt_metric_key_raw}' specified in config.")
             return

        if not param_ranges:
            logger.critical(f"No parameter_ranges defined for strategy '{strategy_choice}' in walk_forward config.")
            return

        logger.info(f"Walk-Forward Config: IS={wf_config['in_sample_period']}, OOS={wf_config['out_of_sample_period']}, OptMetric={opt_metric_key_raw}")

    except (KeyError, ValueError, TypeError) as e:
        logger.critical(f"Invalid walk_forward configuration: {e}")
        return

    # Prepare parameter combinations
    param_names = list(param_ranges.keys())
    param_values = list(param_ranges.values())
    param_combinations = list(itertools.product(*param_values))
    logger.info(f"Generated {len(param_combinations)} parameter combinations for optimization.")

    all_oos_results = [] # Store results from each OOS period
    all_oos_results_metrics = [] # Rename original list
    all_oos_equity_dfs = [] # New list for equity data
    current_is_start = overall_start

    # --- Walk-Forward Loop ---
    wf_step = 0
    while True:
        wf_step += 1
        # Define periods for this step
        current_is_end = current_is_start + is_offset - pd.Timedelta(days=1)
        current_oos_start = current_is_end + pd.Timedelta(days=1)
        current_oos_end = current_oos_start + oos_offset - pd.Timedelta(days=1)

        # Check if periods exceed overall end date
        if current_is_end > overall_end: break
        # Adjust OOS end date if it goes beyond the overall end date
        current_oos_end = min(current_oos_end, overall_end)
        if current_oos_start > current_oos_end: break # No OOS period left

        logger.info(f"\n=== Walk-Forward Step {wf_step} ===")
        logger.info(f"In-Sample Period:   {current_is_start.strftime('%Y-%m-%d')} to {current_is_end.strftime('%Y-%m-%d')}")
        logger.info(f"Out-of-Sample Period: {current_oos_start.strftime('%Y-%m-%d')} to {current_oos_end.strftime('%Y-%m-%d')}")

        # --- In-Sample Optimization ---
        best_params = None
        best_metric_value = -float('inf') # Initialize for maximization

        logger.info(f"--- Optimizing over {len(param_combinations)} parameter sets for Step {wf_step} IS period ---")
        for i, params_tuple in enumerate(param_combinations):
            current_params = dict(zip(param_names, params_tuple))
            # Include base ATR settings from main config
            current_params.update(config.get('trading', {}).get('atr_stop', {}))

            run_label = f"WF{wf_step}_IS_Opt_{i+1}"
            # --- Run backtest for these params on IS period ---
            is_metrics = _run_single_backtest(config, current_is_start, current_is_end, current_params, run_label)

            # Extract optimization metric value (handle potential errors/missing keys/string values)
            current_metric_num = is_metrics.get(optimization_metric_key) # Get numeric value directly

            if current_metric_num is None or not isinstance(current_metric_num, (int, float)):
                logger.error(f"Could not retrieve valid numeric optimization metric '{optimization_metric_key}' for run {run_label}. Value: {current_metric_num}")
                continue

            # Handle infinity for profit factor if maximizing it
            current_metric_for_comparison = current_metric_num
            # Optional: if optimizing profit factor, you might treat inf as a very large number or handle separately
            # if opt_metric_key_raw == "profit_factor" and current_metric_for_comparison == float('inf'):
            #    current_metric_for_comparison = 999999 # Or some large number

            logger.debug(f"Opt Run {i+1}: Params={current_params}, {optimization_metric_key}={current_metric_for_comparison:.4f}")

            # Compare numeric values directly
            if current_metric_for_comparison > best_metric_value:
                best_metric_value = current_metric_for_comparison
                best_params = current_params
                logger.info(f"Opt Run {i+1}: Found NEW BEST params! Metric: {best_metric_value:.4f}, Params: {best_params}")

        if best_params is None:
            logger.error(f"Optimization failed for Step {wf_step} IS period. No valid parameters found. Skipping OOS.")
            # Move to next period - advance start date
            current_is_start = current_oos_start # Start next IS where this OOS started
            continue

        logger.info(f"--- In-Sample Optimization Complete for Step {wf_step} ---")
        logger.info(f"Best Parameters: {best_params}")
        logger.info(f"Best Metric Value ({optimization_metric_key}): {best_metric_value:.3f}")

        # --- Out-of-Sample Test ---
        logger.info(f"--- Running Out-of-Sample Test for Step {wf_step} with best parameters ---")
        oos_run_label = f"WF{wf_step}_OOS"
        oos_run_output = _run_single_backtest(config, current_oos_start, current_oos_end, best_params, oos_run_label)
        oos_metrics = oos_run_output.get("metrics", {})
        oos_equity_df = oos_run_output.get("daily_equity")

        if "Error" in oos_metrics:
            logger.error(f"Out-of-sample run {oos_run_label} failed: {oos_metrics['Error']}")
        elif oos_equity_df is not None and not oos_equity_df.empty: # Check equity df is valid
            # Store metrics for summary table
            oos_metrics['step'] = wf_step; oos_metrics['is_start'] = ...; # Add metadata as before
            all_oos_results_metrics.append(oos_metrics)
            # Store equity df for aggregation
            all_oos_equity_dfs.append(oos_equity_df)
            logger.info(f"Out-of-Sample Run {oos_run_label} complete.")
        else:
            logger.error(f"Out-of-sample run {oos_run_label} completed but returned invalid equity data. Skipping aggregation for this step.")

        # --- Roll Forward ---
        # Advance start date by the out-of-sample period length for the next iteration
        current_is_start = current_oos_start # Start next IS where this OOS started


    # --- Walk-Forward Analysis Finished ---
    if not all_oos_equity_dfs:
        logger.error("No valid out-of-sample equity dataframes collected. Cannot calculate aggregated WFA performance.")
        return # Or just finish after saving the summary table

    logger.info("\n--- Calculating Aggregated Walk-Forward Performance ---")
    try:
        # Concatenate all OOS equity curves
        aggregated_oos_equity = pd.concat(all_oos_equity_dfs).sort_index()
        # Remove potential duplicate indices from overlapping period definitions (if any, less likely with rolling)
        aggregated_oos_equity = aggregated_oos_equity[~aggregated_oos_equity.index.duplicated(keep='last')]

        # Save the aggregated curve
        aggregated_curve_path = os.path.join(results_dir_base, "walk_forward_aggregated_equity.csv")
        aggregated_oos_equity.to_csv(aggregated_curve_path)
        logger.info(f"Aggregated OOS equity curve saved to: {aggregated_curve_path}")

        # Calculate metrics on the aggregated curve
        # Use initial capital from config, but could adjust if needed
        initial_capital = float(config['account']['initial_capital'])
        risk_free_rate = float(config.get('performance', {}).get('risk_free_rate', 0.0))

        # Create a new tracker instance for the aggregated results
        aggregated_tracker = PerformanceTracker(initial_capital, risk_free_rate)
        # Manually populate its daily equity list (or use record_equity in a loop)
        # Need to be careful about the initial capital assumption here.
        # A more robust way might be to calculate returns from the aggregated curve directly.

        # --- Alternative & Simpler: Calculate metrics directly from the aggregated curve ---
        if len(aggregated_oos_equity) >= 2:
            agg_start_equity = aggregated_oos_equity['equity'].iloc[0]
            agg_end_equity = aggregated_oos_equity['equity'].iloc[-1]
            agg_start_date = aggregated_oos_equity.index[0]
            agg_end_date = aggregated_oos_equity.index[-1]
            agg_duration_days = (agg_end_date - agg_start_date).days
            agg_daily_returns = aggregated_oos_equity['equity'].pct_change().dropna()

            if len(agg_daily_returns) >= 2:
                # Calculate CAGR, Sharpe, MaxDD etc. using the same formulas as in PerformanceTracker
                # but applied to the aggregated_oos_equity and agg_daily_returns series.
                agg_metrics = {} # Dictionary to hold aggregated results

                agg_years = max(1.0, agg_duration_days) / 365.25
                agg_cagr = ((agg_end_equity / agg_start_equity) ** (1.0 / agg_years)) - 1 if agg_start_equity > 0 and agg_years > 0 else 0.0
                agg_metrics['Aggregated CAGR %'] = f"{agg_cagr * 100:.2f}%"

                agg_volatility = agg_daily_returns.std() * np.sqrt(252)
                agg_metrics['Aggregated Annual Volatility %'] = f"{agg_volatility * 100:.2f}%"

                agg_sharpe = ((agg_cagr - risk_free_rate) / agg_volatility) if agg_volatility > 1e-9 else 0.0
                agg_metrics[f'Aggregated Sharpe Ratio (Rf={risk_free_rate*100:.1f}%)'] = f"{agg_sharpe:.3f}"

                agg_rolling_max = aggregated_oos_equity['equity'].cummax()
                agg_daily_drawdown = aggregated_oos_equity['equity'] / agg_rolling_max - 1.0
                agg_max_drawdown = agg_daily_drawdown.min()
                agg_metrics['Aggregated Max Drawdown %'] = f"{agg_max_drawdown * 100:.2f}%"
                try: agg_metrics['Aggregated Max Drawdown Date'] = agg_daily_drawdown.idxmin().strftime('%Y-%m-%d')
                except: pass

                agg_calmar = (agg_cagr / abs(agg_max_drawdown)) if agg_max_drawdown < -1e-9 else 0.0
                agg_metrics['Aggregated Calmar Ratio'] = f"{agg_calmar:.2f}"

                # Log these aggregated metrics
                logger.info("--- Aggregated Walk-Forward Metrics ---")
                max_key_len = max(len(k) for k in agg_metrics.keys()) if agg_metrics else 0
                for key, value in agg_metrics.items(): logger.info(f"{key:<{max_key_len}} : {value}")
                logger.info("--------------------------------------")

            else: logger.warning("Insufficient returns in aggregated OOS data to calculate metrics.")
        else: logger.warning("Insufficient daily data points in aggregated OOS equity curve.")

    except Exception as agg_e:
        logger.exception(f"Error during WFA OOS aggregation or metric calculation: {agg_e}")

    # Save the original per-step summary CSV
    oos_df = pd.DataFrame(all_oos_results_metrics) # Use the renamed list
    results_dir_base = config.get('backtest', {}).get('results_directory', 'results')
    wf_results_path = os.path.join(results_dir_base, "walk_forward_summary.csv")
    try:
         oos_df.to_csv(wf_results_path, index=False)
         logger.info(f"Walk-forward summary saved to: {wf_results_path}")
    except Exception as e:
         logger.error(f"Failed to save walk-forward summary CSV: {e}")

    # Calculate and print overall aggregated metrics (e.g., average Sharpe, compounded return)
    # This requires combining the equity curves or daily returns from each OOS run,
    # which needs the _run_single_backtest to potentially return more than just metrics dict.
    # For now, just print the summary DataFrame.
    logger.info("\n--- Walk-Forward Summary Table ---")
    print(oos_df.to_string()) # Print full DataFrame to console log
    logger.info("---------------------------------")

# --- Main Execution Block ---
if __name__ == '__main__':
    config_file = "config.yaml"
    if not os.path.exists(config_file):
         print(f"ERROR: {config_file} not found.")
         sys.exit(1)

    try:
        config = load_config(config_file)
        # Decide whether to run walk-forward or single backtest
        if config.get('backtest', {}).get('walk_forward', {}).get('enabled', False):
            run_walk_forward_analysis(config_path=config_file)
        else:
            logger.info("Walk-forward disabled. Running single backtest.")
            # Need to adapt the original run_backtest call or integrate its core logic here
            # For now, let's assume the user wants WF if enabled, otherwise nothing.
            # Or call a function that uses _run_single_backtest with default config params.
            default_params = {} # Extract default params from config['strategies'][strategy_choice]
            strategy_choice = config['backtest']['strategy']
            default_params = config.get('strategies', {}).get(strategy_choice, {})
            default_params.update(config.get('trading', {}).get('atr_stop', {}))
            if 'enabled' in default_params: del default_params['enabled']

            if default_params:
                 start_dt = pd.Timestamp(config['backtest']['start_date'], tz='UTC')
                 end_dt = pd.Timestamp(config['backtest']['end_date'], tz='UTC')
                 _run_single_backtest(config, start_dt, end_dt, default_params, run_label="single_backtest")
            else:
                 logger.error("Could not determine default parameters for single backtest.")

    except Exception as e:
        log_func = logger.exception if logger else print
        log_func("Unhandled exception during backtest run.")
        sys.exit(1)