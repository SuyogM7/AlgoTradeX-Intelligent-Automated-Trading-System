#!/usr/bin/env python3
# scripts/three_strategy_client.py

import socket
import json
import csv
from collections import defaultdict, deque

import pandas as pd
import numpy as np

from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import BollingerBands

# Server configuration
HOST = "127.0.0.1"  # or wherever your tcp_server is listening
PORT = 9999

class TradingStrategy:
    """
    A triple-factor day trading strategy using Bollinger Bands, MACD, and Volume Spike.
    Uses a voting system for trade signals.
    """
    
    def __init__(self):
        # Store up to 100 recent data points per symbol
        self.data_buffers = defaultdict(lambda: deque(maxlen=100))

    def update_buffers(self, symbol, data_point):
        """Append the latest data point to the symbol's buffer."""
        self.data_buffers[symbol].append(data_point)

    def calculate_indicators(self, symbol):
        """Calculate Bollinger Bands, MACD, RSI, and volume statistics."""
        buffer = list(self.data_buffers[symbol])
        if len(buffer) < 20:  
            return None  # Need at least 20 data points
        
        df = pd.DataFrame(buffer)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # Bollinger Bands (20-period, 2 std dev)
        bb = BollingerBands(df['close'], window=20, window_dev=2)
        
        # MACD (12,26,9)
        macd = MACD(df['close'], window_slow=26, window_fast=12, window_sign=9)
        
        # RSI (14-period)
        rsi = RSIIndicator(df['close'], window=14)
        
        # 20-bar volume moving average & standard deviation
        volume_ma = df['volume'].rolling(20).mean().iloc[-1]
        volume_std = df['volume'].rolling(20).std().iloc[-1]

        return {
            'bb_upper': bb.bollinger_hband().iloc[-1],
            'bb_lower': bb.bollinger_lband().iloc[-1],
            'bb_middle': bb.bollinger_mavg().iloc[-1],
            'macd_line': macd.macd().iloc[-1],
            'macd_signal': macd.macd_signal().iloc[-1],
            'rsi': rsi.rsi().iloc[-1],
            'volume_ma': volume_ma,
            'volume_std': volume_std
        }

    def generate_signal(self, symbol, current_data):
        """
        Returns 'BUY', 'SELL', or None based on a 3-factor voting system:
        1. Bollinger Bands breakout confirmation
        2. MACD crossover with threshold
        3. Volume spike above statistical significance
        """
        indicators = self.calculate_indicators(symbol)
        if not indicators:
            return None  # Not enough data

        signals = []
        close_price = float(current_data['close'])
        volume = float(current_data['volume'])
        open_price = float(current_data['open'])

        # Ensure previous_close is properly extracted and converted to float
        try:
            previous_close = float(list(self.data_buffers[symbol])[-2]['close']) if len(self.data_buffers[symbol]) > 1 else close_price
        except (KeyError, ValueError, TypeError):
            previous_close = close_price  # Fallback to current close if there's an issue

        # --- Bollinger Bands Strategy ---
        if close_price > indicators['bb_upper'] and previous_close <= float(indicators['bb_upper']):
            signals.append('SELL')
        elif close_price < indicators['bb_lower'] and previous_close >= float(indicators['bb_lower']):
            signals.append('BUY')

        # --- MACD Crossover with Threshold ---
        macd_diff = float(indicators['macd_line']) - float(indicators['macd_signal'])
        if macd_diff > 0.1:  # Require significant divergence
            signals.append('BUY')
        elif macd_diff < -0.1:
            signals.append('SELL')

        # --- Volume Spike Confirmation ---
        if volume > float(indicators['volume_ma']) + (2 * float(indicators['volume_std'])):
            signals.append('BUY' if close_price > open_price else 'SELL')

        # --- Voting System: 2 or more 'BUY' → BUY, 2 or more 'SELL' → SELL ---
        buy_count = signals.count('BUY')
        sell_count = signals.count('SELL')

        if buy_count >= 2:
            return 'BUY'
        if sell_count >= 2:
            return 'SELL'
        return None



class PortfolioManager:
    """
    Manages cash, positions, trade execution, and logs.
    """
    def __init__(self):
        self.cash = 100000.0
        # Dictionary of symbol -> {'quantity': int, 'avg_price': float}
        self.positions = defaultdict(dict)
        self.trade_log = []
        self.history = []
        
    def execute_trade(self, symbol, signal, price, timestamp):
        """
        Buys or sells a fixed portion of the portfolio:
          - Buys up to 10% of current cash or $1000, whichever is smaller.
          - Sells up to the full position if we hold it.
        """
        # position size in dollars
        max_investment = min(self.cash * 0.1, 1000)
        # how many shares
        quantity = int(max_investment / price)
        
        if quantity < 1:
            return  # Too expensive, skip
        
        if signal == 'BUY' and self.cash >= price * quantity:
            self._execute_buy(symbol, price, quantity, timestamp)
        elif signal == 'SELL' and symbol in self.positions:
            # Sell the lesser of "quantity" or what we hold
            holdings = self.positions[symbol].get('quantity', 0)
            if holdings > 0:
                sell_qty = min(quantity, holdings)
                self._execute_sell(symbol, price, sell_qty, timestamp)
            
    def _execute_buy(self, symbol, price, quantity, timestamp):
        cost = price * quantity
        self.cash -= cost
        if symbol in self.positions:
            current_qty = self.positions[symbol]['quantity']
            current_avg = self.positions[symbol]['avg_price']
            total_cost = (current_avg * current_qty) + cost
            new_qty = current_qty + quantity
            new_avg = total_cost / new_qty
            self.positions[symbol].update({
                'quantity': new_qty,
                'avg_price': new_avg
            })
        else:
            self.positions[symbol] = {
                'quantity': quantity,
                'avg_price': price
            }
        
        self.trade_log.append({
            'timestamp': timestamp,
            'symbol': symbol,
            'action': 'BUY',
            'price': price,
            'quantity': quantity
        })
        
    def _execute_sell(self, symbol, price, quantity, timestamp):
        revenue = price * quantity
        self.cash += revenue
        
        self.positions[symbol]['quantity'] -= quantity
        if self.positions[symbol]['quantity'] <= 0:
            del self.positions[symbol]  # remove from dict if fully closed
        
        self.trade_log.append({
            'timestamp': timestamp,
            'symbol': symbol,
            'action': 'SELL',
            'price': price,
            'quantity': quantity
        })
        
    def update_valuation(self, timestamp, market_data):
        """
        Recalculate total portfolio value:
          - total_value = cash + sum(market_value_of_all_positions)
          - unrealized = sum( (current_price - avg_price) * quantity ) across all positions
        """
        total_value = self.cash
        unrealized = 0.0
        
        # Convert list of securities to a lookup of {symbol: close_price}
        prices = {d['symbol']: float(d['close']) for d in market_data}
        
        for sym, info in self.positions.items():
            qty = info['quantity']
            avg_price = info['avg_price']
            if sym in prices:
                current_price = prices[sym]
                position_value = current_price * qty
                total_value += position_value
                unrealized += (current_price - avg_price) * qty
        
        self.history.append({
            'timestamp': timestamp,
            'total_value': total_value,
            'unrealized': unrealized,
            'positions': len(self.positions)
        })
        
        return total_value, unrealized


def start_client():
    strategy = TradingStrategy()
    portfolio = PortfolioManager()
    
    # Connect to local TCP server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print(f"✅ Connected to server at {HOST}:{PORT}")
        
        # Open a CSV file to track portfolio changes over time
        report_file = 'data/trading_session_report.csv'
        with open(report_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Total Value', 'Unrealized PnL', 'Positions Held'])
        
        # Main data loop
        while True:
            try:
                data = s.recv(65535)
                if not data:
                    # Server disconnected or no more data
                    break
                
                # Parse JSON from server
                message = json.loads(data.decode())
                timestamp = message['timestamp']
                securities = message['data']
                
                print(f"\n📅 Received market data for {timestamp}")
                
                # Step 1: Update strategy buffers & generate signals
                for sec in securities:
                    symbol = sec['symbol']
                    strategy.update_buffers(symbol, sec)
                    signal = strategy.generate_signal(symbol, sec)
                    
                    if signal:
                        price = float(sec['close'])
                        print(f"🚨 Signal: {symbol} {signal} at ${price:.2f}")
                        portfolio.execute_trade(symbol, signal, price, timestamp)
                
                # Step 2: Update portfolio valuation
                total_value, unrealized = portfolio.update_valuation(timestamp, securities)
                
                # Step 3: Print summary
                print(f"\n💰 Portfolio Summary:")
                print(f"Cash: ${portfolio.cash:,.2f}")
                print(f"Total Value: ${total_value:,.2f}")
                print(f"Unrealized PnL: ${unrealized:+,.2f}")
                print(f"Positions Held: {len(portfolio.positions)}")
                
                # Step 4: Append a CSV row
                with open(report_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([timestamp, round(total_value, 2), round(unrealized, 2), len(portfolio.positions)])
                    
            except json.JSONDecodeError:
                print("⚠️ Invalid JSON data received, skipping...")
                continue
    
    print("\n✅ Trading session ended")


if __name__ == "__main__":
    start_client()