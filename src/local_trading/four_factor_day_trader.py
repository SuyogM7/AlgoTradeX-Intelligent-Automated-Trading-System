#!/usr/bin/env python3

import socket
import json
import csv
from collections import defaultdict, deque
import os

import pandas as pd
import numpy as np

from numba import jit
from pandarallel import pandarallel

# Initialize pandarallel AFTER defining mad()
@jit(nopython=True)
def mad(x):
    return np.mean(np.abs(x - np.mean(x)))

pandarallel.initialize(progress_bar=True)

class TradingStrategy:
    """
    A trading strategy class that uses four indicators:
      1. 200 EMA
      2. VWAP + Volume Filter
      3. Commodity Channel Index (CCI)
      4. EMA Crossover

    A trade signal is generated only if at least 3 out of 4 indicators agree.
    """
    def __init__(self, risk_amount=1.0):
        # Store a rolling window of market data for each symbol
        self.data_buffers = defaultdict(lambda: deque(maxlen=300))
        self.risk_amount = risk_amount
        

    def update_buffer(self, symbol, data_point):
        """Append the latest market data point to the symbol's buffer."""
        self.data_buffers[symbol].append(data_point)
        print(f"Buffer updated for {symbol}: now {len(self.data_buffers[symbol])} data points")

    def calculate_indicators(self, df):
        # Convert necessary columns to float
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # If 'vwap' exists, convert it to float as well
        if 'vwap' in df.columns:
            df['vwap'] = df['vwap'].astype(float)

        # 1. 200 EMA Indicator
        df["200_EMA"] = df['close'].ewm(span=200, min_periods=200).mean()
        if pd.isna(df["200_EMA"].iloc[-1]):
            indicator1 = 0  # Treat as neutral if EMA not available
        else:
            indicator1 = 1 if df['close'].iloc[-1] > df["200_EMA"].iloc[-1] else -1

        # 2. VWAP + Volume Filter Indicator
        df["volume_20_avg"] = df['volume'].rolling(window=20).mean()
        vwap_signal = 0
        if 'vwap' in df.columns:
            latest_close = df['close'].iloc[-1]
            latest_vwap = df['vwap'].iloc[-1]
            latest_volume = df['volume'].iloc[-1]
            vol_avg = df["volume_20_avg"].iloc[-1]
            if latest_close > latest_vwap and latest_volume > 0.8 * vol_avg:
                vwap_signal = 1
            elif latest_close < latest_vwap and latest_volume > 0.8 * vol_avg:
                vwap_signal = -1
        indicator2 = vwap_signal

        # 3. CCI Indicator
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        sma = df["typical_price"].rolling(window=20).mean()
        mad_vals = df["typical_price"].rolling(window=20).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
        cci = (df["typical_price"] - sma) / (0.015 * mad_vals)
        cci_signal = 0
        if not np.isnan(cci.iloc[-1]):
            if cci.iloc[-1] > 100:
                cci_signal = 1
            elif cci.iloc[-1] < -100:
                cci_signal = -1
            else:
                cci_signal = 0
        indicator3 = cci_signal

        # 4. EMA Crossover Indicator (9 EMA vs. 26 EMA)
        df["9_EMA"] = df['close'].ewm(span=9, min_periods=9).mean()
        df["26_EMA"] = df['close'].ewm(span=26, min_periods=26).mean()
        if pd.isna(df["26_EMA"].iloc[-1]):
            indicator4 = 0  # Neutral if 26 EMA not available
        else:
            indicator4 = 1 if df["9_EMA"].iloc[-1] > df["26_EMA"].iloc[-1] else -1

        print(f"Indicators computed: 200 EMA signal: {indicator1}, VWAP signal: {indicator2}, CCI signal: {indicator3}, EMA crossover: {indicator4}")
        return indicator1, indicator2, indicator3, indicator4

    def generate_trade_signal(self, symbol):
        """
        Returns 'BUY', 'SELL', or None if there are not enough data points or no consensus.
        A signal is generated if at least 3 of the 4 indicators agree.
        """
        # Lowered threshold to 200 data points (adjust as needed)
        if len(self.data_buffers[symbol]) < 200:
            print(f"Insufficient data for {symbol}: {len(self.data_buffers[symbol])} points")
            return None
        df = pd.DataFrame(list(self.data_buffers[symbol]))
        ind1, ind2, ind3, ind4 = self.calculate_indicators(df)
        signals = [ind1, ind2, ind3, ind4]
        buy_signals = sum(1 for s in signals if s == 1)
        sell_signals = sum(1 for s in signals if s == -1)
    
        if buy_signals >= 3:
            return 'BUY'
        elif sell_signals >= 3:
            return 'SELL'
        else:
            return None

    def is_stock(self, symbol):
        """
        Determines if the symbol represents a stock.
        For example, symbols starting with '^' might be indices.
        """
        return not symbol.startswith('^')


class PortfolioManager:
    """
    Manages cash, open positions, and trade execution.
    Also logs trade executions (both open and close) with timestamps and realized PNL.
    """
    def __init__(self, risk_amount=1.0):
        self.cash = 100000.0
        self.positions = {}  # symbol -> { 'entry_price': float, 'position_type': str, 'stop_loss': float, 'target': float, 'quantity': int }
        self.realized_pnl = 0.0
        self.risk_amount = risk_amount
        # File to log executed trades
        self.trade_log_file = 'data/trade_execution_log.csv'
        os.makedirs(os.path.dirname(self.trade_log_file), exist_ok=True)
        # Initialize the CSV with headers
        with open(self.trade_log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Symbol', 'Action', 'Price', 'Quantity', 'Realized_PNL'])

    def log_trade_event(self, timestamp, symbol, action, price, quantity, pnl):
        """
        Append a trade event to the trade log CSV file.
        For open trades, pnl can be marked as 'N/A'.
        For exit trades, the realized pnl is recorded.
        """
        try:
            with open(self.trade_log_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, symbol, action, price, quantity, pnl])
        except Exception as e: 
            print(f"Error logging trade event: {e}")

    def open_position(self, symbol, signal, current_price, timestamp, is_stock=True):
        if symbol in self.positions:
            return  # Position already open

        position_type = signal  # 'BUY' for long, 'SELL' for short
        profit_ratio = 1.5 if is_stock else 1.0
        quantity = 1  # Fixed quantity as per the strategy

        if position_type == 'BUY':
            # Check if enough cash to buy
            cost = current_price * quantity
            if self.cash < cost:
                print(f"Not enough cash to open BUY position for {symbol}")
                return
            self.cash -= cost
            stop_loss = current_price - self.risk_amount
            target = current_price + profit_ratio * self.risk_amount
        else:  # SELL (short)
            # Add proceeds from short sale
            self.cash += current_price * quantity
            stop_loss = current_price + self.risk_amount
            target = current_price - profit_ratio * self.risk_amount

        self.positions[symbol] = {
            'entry_price': current_price,
            'position_type': position_type,
            'stop_loss': stop_loss,
            'target': target,
            'quantity': quantity
        }
        print(f"Opened {position_type} position for {symbol} at {current_price:.2f} | SL: {stop_loss:.2f} | Target: {target:.2f}")
        self.log_trade_event(timestamp, symbol, f'OPEN {position_type}', current_price, quantity, 'N/A')

    def update_positions(self, symbol, current_price, timestamp):
        if symbol not in self.positions:
            return

        pos = self.positions[symbol]
        entry = pos['entry_price']
        action = pos['position_type']
        quantity = pos['quantity']

        # Determine exit condition
        exit_condition = False
        if action == 'BUY':
            if current_price >= pos['target'] or current_price <= pos['stop_loss']:
                exit_condition = True
        else:
            if current_price <= pos['target'] or current_price >= pos['stop_loss']:
                exit_condition = True

        if exit_condition:
            # Calculate PNL and adjust cash
            if action == 'BUY':
                self.cash += current_price * quantity  # Sell the position
                pnl = (current_price - entry) * quantity
            else:
                self.cash -= current_price * quantity  # Buy back the short
                pnl = (entry - current_price) * quantity

            self.realized_pnl += pnl

            print(f"Closing {symbol} {action} position at {current_price:.2f}")
            self.log_trade_event(
                timestamp, symbol, f'CLOSE {action}',
                current_price, quantity, round(pnl, 2)
            )

            del self.positions[symbol]

    def update_valuation(self, timestamp, market_data):
        """
        Calculates the total portfolio value (cash plus market value of open positions)
        and the total unrealized PNL.
        """
        total_value = self.cash + self.realized_pnl
        unrealized = 0.0
        prices = {d['symbol']: float(d['close']) for d in market_data}
        for sym, info in self.positions.items():
            if sym in prices:
                current_price = prices[sym]
                if info['position_type'] == 'BUY':
                    total_value += (current_price * info['quantity'])
                else:  # short
                    total_value -= (current_price * info['quantity'])
                entry = info['entry_price']
                if info['position_type'] == 'BUY':
                    unrealized += (current_price - entry) * info['quantity']
                else:
                    unrealized += (entry - current_price) * info['quantity']
        return total_value, unrealized


# Server configuration
HOST = "127.0.0.1"  # or wherever your TCP server is running
PORT = 9999

def start_client():
    strategy = TradingStrategy(risk_amount=1.0)
    portfolio = PortfolioManager(risk_amount=1.0)
    
    # Open CSV file to track portfolio session report (existing file)
    session_report_file = 'data/trading_session_report.csv'
    os.makedirs(os.path.dirname(session_report_file), exist_ok=True)
    with open(session_report_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Total Value', 'Unrealized_PnL', 'Positions_Held'])
    
    # Connect to the market data server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print(f"Connected to server at {HOST}:{PORT}")
        
        while True:
            try:
                data = s.recv(65535)
                if not data:
                    break  # No more data from server
                message = json.loads(data.decode())
                print("Raw market data received:", message)
                timestamp = message['timestamp']
                securities = message['data']
                print(f"\nReceived market data for {timestamp}")
                
                # Update buffers and generate signals for each security
                for sec in securities:
                    symbol = sec['symbol']
                    strategy.update_buffer(symbol, sec)
                    trade_signal = strategy.generate_trade_signal(symbol)
                    
                    # If a signal is generated and no position is open, open a new position
                    if trade_signal and symbol not in portfolio.positions:
                        price = float(sec['close'])
                        portfolio.open_position(symbol, trade_signal, price, timestamp, is_stock=strategy.is_stock(symbol))
                     
                    # Always check if open positions need to be exited
                    portfolio.update_positions(symbol, float(sec['close']), timestamp)
                
                # Update portfolio valuation and log the session report
                total_value, unrealized = portfolio.update_valuation(timestamp, securities)
                print(f"Portfolio Summary: Cash: ${portfolio.cash:,.2f}, Total Value: ${total_value:,.2f}, Unrealized PnL: ${unrealized:+,.2f}, Positions: {len(portfolio.positions)}")
                
                with open(session_report_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([timestamp, round(total_value, 2), round(unrealized, 2), len(portfolio.positions)])
            except json.JSONDecodeError:
                print("Invalid JSON received, skipping...")
                continue
        
    print("Trading session ended")

if __name__ == "__main__":
    start_client()
