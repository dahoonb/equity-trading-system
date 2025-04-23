# performance/tracker.py (Revised and Completed)
import logging
logger = logging.getLogger("TradingSystem") # Use the same name used in setup_logger

import pandas as pd
import numpy as np
import math
import datetime
from collections import deque
from typing import TYPE_CHECKING, Optional, List, Dict, Tuple, Any # Import necessary types
import os # Import os for saving results

# Import core components
from core.events import FillEvent

# Conditionally import scipy for Alpha/Beta calculation
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logger.info("Scipy not found. Alpha/Beta metrics will not be calculated.")


if TYPE_CHECKING:
    pass # Avoid circular import if needed

class PerformanceTracker:
    """
    Tracks portfolio equity, trades, and calculates performance metrics.
    Handles both high-frequency equity curve and daily equity snapshots.
    Calculates standard metrics like CAGR, Sharpe, Sortino, Drawdown,
    and optionally Alpha/Beta if benchmark data is provided.
    """
    def __init__(self, initial_capital: float, risk_free_rate: float = 0.0):
        """
        Initializes the PerformanceTracker.

        Args:
            initial_capital: The starting capital of the portfolio.
            risk_free_rate: The annualized risk-free rate (e.g., 0.02 for 2%).
        """
        if not isinstance(initial_capital, (int, float)) or initial_capital <= 0:
             logger.warning(f"Invalid initial_capital ({initial_capital}). Defaulting to 1.0.")
             initial_capital = 1.0
        if not isinstance(risk_free_rate, (int, float)):
             logger.warning(f"Invalid risk_free_rate ({risk_free_rate}). Defaulting to 0.0.")
             risk_free_rate = 0.0

        self.initial_capital = float(initial_capital)
        self.risk_free_rate = float(risk_free_rate) # Annualized risk-free rate

        # Stores (timestamp, equity) tuples for detailed curve
        self.equity_curve: deque[Tuple[datetime.datetime, float]] = deque()
        # Stores (date_obj, equity_float) tuples for daily EOD values
        self.daily_equity_list: List[Tuple[datetime.date, float]] = []
        # Stores dictionaries of trade details
        self.trades: List[Dict[str, Any]] = []

        self.last_equity_record_date: Optional[datetime.date] = None # Store date object
        self._initial_equity_recorded = False # Flag to record first point

        logger.info(f"PerformanceTracker initialized with Initial Capital: ${self.initial_capital:,.2f}, Risk-Free Rate: {self.risk_free_rate*100:.2f}%")

    def record_equity(self, timestamp: datetime.datetime, equity: float):
        """
        Records the portfolio equity at a given timestamp.
        Handles timezone awareness, deduplication, and daily snapshot updates.
        """
        if not isinstance(timestamp, datetime.datetime) or not isinstance(equity, (int, float)):
            logger.error(f"Invalid input to record_equity: timestamp={timestamp}, equity={equity}")
            return
        if math.isnan(equity) or equity < 0:
             logger.warning(f"Attempted to record invalid equity value: {equity} at {timestamp}")
             return # Don't record invalid equity

        # Ensure timestamp is timezone-aware UTC
        if timestamp.tzinfo is None:
             timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        else:
             timestamp = timestamp.astimezone(datetime.timezone.utc)

        current_date = timestamp.date()

        # Record initial point if not done yet
        if not self._initial_equity_recorded:
             self.equity_curve.append((timestamp, equity))
             self.daily_equity_list.append((current_date, equity))
             self.last_equity_record_date = current_date
             self._initial_equity_recorded = True
             logger.debug(f"Recorded initial equity point: {timestamp} - ${equity:,.2f}")
             return

        # Record subsequent points for the detailed curve
        # Add only if timestamp is new or equity value has changed
        if not self.equity_curve or timestamp > self.equity_curve[-1][0] or equity != self.equity_curve[-1][1]:
             # If timestamp is identical to last, update the equity value instead of appending
             if self.equity_curve and self.equity_curve[-1][0] == timestamp:
                 self.equity_curve[-1] = (timestamp, equity)
             else:
                 self.equity_curve.append((timestamp, equity))

        # Update daily equity list
        if self.last_equity_record_date is None: # Should be set by initial record
             self.last_equity_record_date = current_date
             self.daily_equity_list.append((current_date, equity))
        elif current_date > self.last_equity_record_date:
             # End of previous day: Find the last equity value recorded ON the previous day
             prev_day_equity = self.initial_capital # Default if no prior data
             # Iterate backwards through detailed curve to find last point on previous day
             for i in range(len(self.equity_curve) - 1, -1, -1):
                 ts, eq = self.equity_curve[i]
                 if ts.date() == self.last_equity_record_date:
                     prev_day_equity = eq
                     break

             # Update or append previous day's record in daily list
             if self.daily_equity_list and self.daily_equity_list[-1][0] == self.last_equity_record_date:
                  self.daily_equity_list[-1] = (self.last_equity_record_date, prev_day_equity)
             else: # Should not happen if logic is correct, but safety append
                  self.daily_equity_list.append((self.last_equity_record_date, prev_day_equity))
             logger.debug(f"Recorded EOD equity for {self.last_equity_record_date}: ${prev_day_equity:,.2f}")

             # Record the first point for the new day
             self.daily_equity_list.append((current_date, equity))
             self.last_equity_record_date = current_date
        else: # Still the same day, update the last entry for this day
            if self.daily_equity_list and self.daily_equity_list[-1][0] == current_date:
                 self.daily_equity_list[-1] = (current_date, equity)
            else: # Should not happen if logic is correct, but safety append
                 self.daily_equity_list.append((current_date, equity))

    def record_trade(self, fill_event: FillEvent, realized_pnl: float, holding_period_days: Optional[float] = None):
        """Records details of a closed trade (or partial close)."""
        if not isinstance(fill_event, FillEvent):
             logger.error(f"Invalid fill_event type passed to record_trade: {type(fill_event)}")
             return
        if not isinstance(realized_pnl, (int, float)): # Allow zero PnL trades to be recorded if needed
             logger.error(f"Invalid realized_pnl type passed to record_trade: {type(realized_pnl)}")
             return

        # Record trade details
        trade_details = {
            'timestamp': fill_event.timestamp,
            'symbol': fill_event.symbol,
            'direction': fill_event.direction, # Direction of the closing fill
            'quantity': fill_event.quantity, # Quantity closed
            'fill_price': fill_event.fill_price,
            'pnl': realized_pnl, # Net PnL for this closing part
            'commission': fill_event.commission, # Commission for this fill
            'holding_period_days': round(holding_period_days, 2) if holding_period_days is not None else None,
            'order_id': fill_event.order_id,
            'exec_id': fill_event.exec_id
        }
        self.trades.append(trade_details)
        logger.debug(f"Recorded trade PNL: Symbol={trade_details['symbol']}, Qty={trade_details['quantity']}, PNL={trade_details['pnl']:.2f}")

    def get_equity_dataframe(self) -> pd.DataFrame:
        """Converts the full equity curve deque to a sorted, unique-indexed DataFrame."""
        if not self.equity_curve:
            return pd.DataFrame(columns=['equity']).set_index(pd.to_datetime([]).tz_localize('UTC'))
        df = pd.DataFrame(list(self.equity_curve), columns=['timestamp', 'equity'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df.set_index('timestamp', inplace=True)
        # Ensure equity is numeric and handle potential duplicates by keeping the last value for a given timestamp
        df['equity'] = pd.to_numeric(df['equity'], errors='coerce')
        df = df[~df.index.duplicated(keep='last')]
        df.dropna(subset=['equity'], inplace=True)
        return df.sort_index()

    def get_daily_equity_dataframe(self) -> pd.DataFrame:
         """Converts daily equity list to a sorted, unique-indexed DataFrame."""
         if not self.daily_equity_list:
             return pd.DataFrame(columns=['equity']).set_index(pd.to_datetime([]).tz_localize('UTC'))
         df = pd.DataFrame(self.daily_equity_list, columns=['date', 'equity'])
         df['date'] = pd.to_datetime(df['date']) # Convert date objects to Timestamps
         df = df.set_index('date')
         # Ensure equity is numeric and handle potential duplicates by keeping the last value for a given date
         df['equity'] = pd.to_numeric(df['equity'], errors='coerce')
         df = df[~df.index.duplicated(keep='last')]
         df.dropna(subset=['equity'], inplace=True)
         # Ensure index is DatetimeIndex and UTC for consistency with returns calculation
         df.index = pd.to_datetime(df.index, utc=True)
         return df.sort_index()

    def calculate_metrics(self, benchmark_returns: Optional[pd.Series] = None) -> Dict[str, Any]:
        """
        Calculates comprehensive performance metrics based on recorded equity and trades.

        Args:
            benchmark_returns: Optional pandas Series of benchmark daily returns,
                               indexed by UTC timestamp, for Alpha/Beta calculation.

        Returns:
            A dictionary containing calculated performance metrics.
        """
        logger.info("Calculating final performance metrics...")
        metrics: Dict[str, Any] = {}

        daily_equity_df = self.get_daily_equity_dataframe()
        if len(daily_equity_df) < 2:
            logger.warning("Not enough daily equity data points (< 2) for metrics calculation.")
            return {"Error": "Insufficient daily data"}

        # --- Basic Portfolio Stats ---
        start_equity = self.initial_capital
        end_equity = daily_equity_df['equity'].iloc[-1]
        metrics['Start Date'] = daily_equity_df.index[0].strftime('%Y-%m-%d')
        metrics['End Date'] = daily_equity_df.index[-1].strftime('%Y-%m-%d')
        duration_days = (daily_equity_df.index[-1] - daily_equity_df.index[0]).days
        metrics['Duration (Days)'] = duration_days
        metrics['Starting Equity'] = f"${start_equity:,.2f}"
        metrics['Ending Equity'] = f"${end_equity:,.2f}"
        total_net_profit = end_equity - start_equity
        metrics['Total Net Profit'] = f"${total_net_profit:,.2f}"
        total_return_pct = ((end_equity / start_equity) - 1) * 100 if start_equity > 0 else 0.0
        metrics['Total Return %'] = f"{total_return_pct:.2f}%"

        # --- Risk & Return Metrics (Require Daily Returns) ---
        daily_returns = daily_equity_df['equity'].pct_change().dropna()
        if daily_returns.empty or len(daily_returns) < 2:
             logger.warning("Not enough daily returns (< 2) to calculate risk metrics.")
             metrics.update({'CAGR %': 'N/A', 'Annual Volatility %': 'N/A', 'Sharpe Ratio': 'N/A', 'Sortino Ratio': 'N/A', 'Max Drawdown %': 'N/A', 'Calmar Ratio': 'N/A'})
        else:
            # CAGR
            years = max(1.0, duration_days) / 365.25 # Avoid division by zero if duration is < 1 day
            cagr = ((end_equity / start_equity) ** (1.0 / years)) - 1 if start_equity > 0 and years > 0 else 0.0
            metrics['CAGR %'] = f"{cagr * 100:.2f}%"

            # Volatility
            volatility = daily_returns.std() * np.sqrt(252) # Annualized standard deviation
            metrics['Annualized Volatility %'] = f"{volatility * 100:.2f}%"

            # Sharpe Ratio
            annual_risk_free_rate = self.risk_free_rate
            sharpe = ((cagr - annual_risk_free_rate) / volatility) if volatility > 1e-9 else 0.0
            metrics[f'Sharpe Ratio (Rf={annual_risk_free_rate*100:.1f}%)'] = f"{sharpe:.3f}"

            # Sortino Ratio
            # Use daily risk-free rate as target return for downside deviation
            daily_risk_free = (1 + annual_risk_free_rate)**(1/252) - 1
            downside_returns_diff = daily_returns - daily_risk_free
            downside_std = np.sqrt(np.mean(np.square(downside_returns_diff[downside_returns_diff < 0]))) if (downside_returns_diff < 0).any() else 0.0
            downside_deviation = downside_std * np.sqrt(252)
            sortino = ((cagr - annual_risk_free_rate) / downside_deviation) if downside_deviation > 1e-9 else 0.0
            metrics[f'Sortino Ratio (Rf={annual_risk_free_rate*100:.1f}%)'] = f"{sortino:.3f}"

            # Max Drawdown
            rolling_max = daily_equity_df['equity'].cummax()
            daily_drawdown = daily_equity_df['equity'] / rolling_max - 1.0
            max_drawdown = daily_drawdown.min() # This will be negative or zero
            metrics['Max Drawdown %'] = f"{max_drawdown * 100:.2f}%"
            try:
                max_dd_date = daily_drawdown.idxmin()
                metrics['Max Drawdown Date'] = max_dd_date.strftime('%Y-%m-%d')
            except ValueError:
                metrics['Max Drawdown Date'] = 'N/A'

            # Calmar Ratio
            calmar = (cagr / abs(max_drawdown)) if max_drawdown < -1e-9 else 0.0
            metrics['Calmar Ratio'] = f"{calmar:.2f}"

            # --- Alpha and Beta Calculation (Optional) ---
            if benchmark_returns is not None and SCIPY_AVAILABLE and not benchmark_returns.empty:
                # Ensure benchmark returns have UTC DatetimeIndex
                if benchmark_returns.index.tz is None:
                    benchmark_returns.index = benchmark_returns.index.tz_localize('UTC')
                elif benchmark_returns.index.tz != datetime.timezone.utc:
                    benchmark_returns.index = benchmark_returns.index.tz_convert('UTC')

                # Align strategy returns with benchmark returns by date index
                aligned_returns = pd.DataFrame({'strategy': daily_returns, 'benchmark': benchmark_returns})
                aligned_returns = aligned_returns.dropna() # Drop dates where either is missing

                if len(aligned_returns) > 10: # Need sufficient points for meaningful regression
                    # Calculate excess returns over daily risk-free rate
                    excess_strategy_returns = aligned_returns['strategy'] - daily_risk_free
                    excess_benchmark_returns = aligned_returns['benchmark'] - daily_risk_free

                    try:
                        beta, alpha_daily, r_value, p_value, std_err = stats.linregress(
                            excess_benchmark_returns, excess_strategy_returns
                        )
                        # Annualize alpha: alpha_annual = alpha_daily * 252 (simpler approximation)
                        # Or compound: alpha_annual = (1 + alpha_daily)**252 - 1
                        alpha_annual = alpha_daily * 252 # Simple annualization
                        metrics['Beta (vs Benchmark)'] = f"{beta:.3f}"
                        metrics['Alpha (Annualized %)'] = f"{alpha_annual * 100:.2f}%"
                        metrics['Correlation (vs Benchmark)'] = f"{r_value:.3f}"
                        metrics['R-squared (vs Benchmark)'] = f"{r_value**2:.3f}"
                    except Exception as e:
                         logger.error(f"Error during Alpha/Beta calculation: {e}")
                         metrics['Beta (vs Benchmark)'] = 'Error'
                         metrics['Alpha (Annualized %)'] = 'Error'
                         metrics['Correlation (vs Benchmark)'] = 'Error'
                         metrics['R-squared (vs Benchmark)'] = 'Error'
                else:
                    logger.warning(f"Not enough overlapping data points ({len(aligned_returns)}) to calculate Alpha/Beta.")
                    metrics['Beta (vs Benchmark)'] = 'N/A (Data<11)'
                    metrics['Alpha (Annualized %)'] = 'N/A (Data<11)'
                    metrics['Correlation (vs Benchmark)'] = 'N/A (Data<11)'
                    metrics['R-squared (vs Benchmark)'] = 'N/A (Data<11)'
            else:
                metrics['Beta (vs Benchmark)'] = 'N/A'
                metrics['Alpha (Annualized %)'] = 'N/A'
                metrics['Correlation (vs Benchmark)'] = 'N/A'
                metrics['R-squared (vs Benchmark)'] = 'N/A'

        # --- Trade Statistics ---
        if not self.trades:
            logger.warning("No trade data recorded for trade metrics.")
            metrics.update({'Total Trades': 0, 'Win Rate %': 'N/A', 'Profit Factor': 'N/A',
                            'Avg Win $': 'N/A', 'Avg Loss $': 'N/A', 'Avg PnL per Trade $': 'N/A',
                            'Gross Profit $': '$0.00', 'Gross Loss $': '$0.00'})
        else:
            trade_df = pd.DataFrame(self.trades)
            trade_pnl = trade_df['pnl'] # Net PnL including commission effects implicitly
            num_trades = len(trade_pnl)
            winning_trades = trade_pnl[trade_pnl > 1e-9] # Count only non-zero wins
            losing_trades = trade_pnl[trade_pnl < -1e-9] # Count only non-zero losses
            num_winning = len(winning_trades)
            num_losing = len(losing_trades)
            num_breakeven = num_trades - num_winning - num_losing

            gross_profit = winning_trades.sum()
            gross_loss = abs(losing_trades.sum())

            win_rate = (num_winning / num_trades) * 100 if num_trades > 0 else 0.0
            profit_factor = (gross_profit / gross_loss) if gross_loss > 1e-9 else float('inf')
            avg_win = winning_trades.mean() if num_winning > 0 else 0.0
            avg_loss = abs(losing_trades.mean()) if num_losing > 0 else 0.0
            avg_pnl = trade_pnl.mean() if num_trades > 0 else 0.0

            metrics['Total Trades'] = num_trades
            metrics['Winning Trades'] = num_winning
            metrics['Losing Trades'] = num_losing
            metrics['Breakeven Trades'] = num_breakeven
            metrics['Win Rate %'] = f"{win_rate:.2f}%"
            metrics['Profit Factor'] = f"{profit_factor:.2f}" if profit_factor != float('inf') else "Inf"
            metrics['Avg Win $'] = f"${avg_win:.2f}"
            metrics['Avg Loss $'] = f"${avg_loss:.2f}"
            metrics['Avg PnL per Trade $'] = f"${avg_pnl:.2f}"
            metrics['Gross Profit $'] = f"${gross_profit:,.2f}"
            metrics['Gross Loss $'] = f"${gross_loss:,.2f}"

        # --- Log Summary ---
        logger.info("--- Performance Summary ---")
        max_key_len = max(len(k) for k in metrics.keys()) if metrics else 0
        for key, value in metrics.items():
            logger.info(f"{key:<{max_key_len}} : {value}")
        logger.info("---------------------------")
        return metrics

    def save_results(self, output_dir="results"):
        """Saves equity curve and trades to CSV files in the specified directory."""
        if not isinstance(output_dir, str) or not output_dir:
             logger.error("Invalid output directory specified for saving results.")
             return

        try:
            os.makedirs(output_dir, exist_ok=True)
            run_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            logger.info(f"Saving performance results to '{output_dir}' with timestamp {run_timestamp}...")

            # Save Full Equity Curve
            equity_df = self.get_equity_dataframe()
            if not equity_df.empty:
                equity_path = os.path.join(output_dir, f"equity_curve_{run_timestamp}.csv")
                equity_df.to_csv(equity_path)
                logger.info(f"Full equity curve saved ({len(equity_df)} points) to {equity_path}")
            else: logger.warning("Full equity curve is empty, not saving.")

            # Save Daily Equity Curve
            daily_equity_df = self.get_daily_equity_dataframe()
            if not daily_equity_df.empty:
                daily_equity_path = os.path.join(output_dir, f"daily_equity_{run_timestamp}.csv")
                daily_equity_df.to_csv(daily_equity_path)
                logger.info(f"Daily equity saved ({len(daily_equity_df)} points) to {daily_equity_path}")
            else: logger.warning("Daily equity data is empty, not saving.")

            # Save Trades Log
            if self.trades:
                 trades_df = pd.DataFrame(self.trades)
                 trades_path = os.path.join(output_dir, f"trades_{run_timestamp}.csv")
                 trades_df.to_csv(trades_path, index=False)
                 logger.info(f"Trades saved ({len(trades_df)} records) to {trades_path}")
            else:
                 logger.info("No trades recorded to save.")

        except Exception as e:
             logger.exception(f"Error saving performance results to directory '{output_dir}': {e}")