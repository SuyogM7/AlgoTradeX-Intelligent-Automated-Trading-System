#!/usr/bin/env python3
# scripts/three_strategy_client.py

import socket
import json
import csv
from collections import defaultdict, deque
from datetime import datetime, time

import pandas as pd
import numpy as np

from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import BollingerBands

# Server configuration
HOST = "127.0.0.1"  # or wherever your TCP server is listening
PORT = 9999


###############################################################################
#                           TRADING STRATEGY                                  #
###############################################################################

class TradingStrategy:
    """
    A triple-factor day trading strategy using:
      1. Bollinger Bands (breakout approach)
      2. MACD (lower threshold)
      3. Volume Spike
    Now only requires 1 strong signal to trigger a trade (instead of 2/3).
    """
    
    def __init__(self):
        # Store up to 100 recent data points per symbol (sufficient for 20-bar indicators)
        self.data_buffers = defaultdict(lambda: deque(maxlen=100))

    def update_buffers(self, symbol, data_point):
        """Append the latest data point (OHLCV) to the symbol's buffer."""
        self.data_buffers[symbol].append(data_point)

    def calculate_indicators(self, symbol):
        """Calculate Bollinger Bands, MACD, RSI, and volume stats for the symbol."""
        buffer = list(self.data_buffers[symbol])
        if len(buffer) < 20:
            return None  # Need at least 20 data points
        
        df = pd.DataFrame(buffer)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # --- Bollinger Bands (20-period, 2 std dev) ---
        bb = BollingerBands(df['close'], window=20, window_dev=2)
        
        # --- MACD (12, 26, 9) ---
        macd = MACD(df['close'], window_slow=26, window_fast=12, window_sign=9)
        
        # --- RSI (14) ---
        rsi = RSIIndicator(df['close'], window=14)
        
        # --- Volume stats (rolling mean & std over 20 bars) ---
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
        Returns 'BUY', 'SELL', or None based on single-factor triggers:
          1. Bollinger Band breakout
          2. MACD crossover with threshold of +/-0.05
          3. Volume spike
        If any one of these triggers a BUY, we return BUY.
        If any one triggers a SELL, we return SELL.
        (Adjust if you want 2/3 signals.)
        """
        indicators = self.calculate_indicators(symbol)
        if not indicators:
            return None  # Not enough data yet

        close_price = float(current_data['close'])
        volume = float(current_data['volume'])
        open_price = float(current_data['open'])

        # Safely get the previous close
        buffer_list = self.data_buffers[symbol]
        if len(buffer_list) > 1:
            try:
                prev_close = float(buffer_list[-2]['close'])
            except (KeyError, ValueError, TypeError):
                prev_close = close_price
        else:
            prev_close = close_price

        # Flags to track potential signals
        buy_signal = False
        sell_signal = False

        # --- 1) Bollinger Band Logic ---
        if close_price > indicators['bb_upper'] and prev_close <= indicators['bb_upper']:
            buy_signal = True
        elif close_price < indicators['bb_lower'] and prev_close >= indicators['bb_lower']:
            sell_signal = True

        # --- 2) MACD Crossover (looser threshold = +/-0.05) ---
        macd_diff = indicators['macd_line'] - indicators['macd_signal']
        if macd_diff > 0.05:
            buy_signal = True
        elif macd_diff < -0.05:
            sell_signal = True

        # --- 3) Volume Spike ---
        # If volume is > mean + 2 std, bullish candle => BUY, else SELL
        if volume > indicators['volume_ma'] + (2 * indicators['volume_std']):
            if close_price > open_price:
                buy_signal = True
            else:
                sell_signal = True

        # Decide final output: single-factor approach
        if buy_signal and not sell_signal:
            return 'BUY'
        elif sell_signal and not buy_signal:
            return 'SELL'
        return None


###############################################################################
#                           PORTFOLIO MANAGEMENT                              #
###############################################################################

class PortfolioManager:
    """
    Manages:
      - Cash
      - Positions
      - Trade execution
      - Stop-loss, take-profit, trailing stop
      - Partial exits at 15:30, 15:45
      - EOD liquidation (16:00)

    Tracks realized & unrealized PnL in a CSV report.
    """
    def __init__(self):
        self.cash = 100_000.0

        # Positions: symbol -> {
        #   'quantity': int,
        #   'avg_price': float,
        #   'stop_price': float,
        #   'take_profit_price': float
        # }
        self.positions = defaultdict(dict)
        
        # Trade & Valuation Logs
        self.trade_log = []
        self.history = []
        
        # Risk parameters
        self.stop_loss_pct = 0.02      # 2% below entry
        self.take_profit_pct = 0.03    # 3% above entry
        
        # Track realized PnL
        self.realized_pnl = 0.0

        # Trailing stop buffer in % (optional)
        self.trailing_stop_buffer = 0.02

    def execute_trade(self, symbol, signal, price, timestamp):
        """
        'BUY'  => buy up to 10% of cash or $1000, whichever is smaller,
                  only if we don't already hold this symbol.
        'SELL' => sell from existing position (no shorting).
        """
        # Skip BUY if already own the symbol
        if signal == 'BUY' and symbol in self.positions and self.positions[symbol]['quantity'] > 0:
            print(f"⚠️ Already holding {symbol}, skipping additional BUY.")
            return

        max_investment = min(self.cash * 0.1, 1000)
        quantity = int(max_investment / price)
        if quantity < 1:
            return  # too expensive, skip

        if signal == 'BUY':
            if self.cash >= (price * quantity):
                self._execute_buy(symbol, price, quantity, timestamp)
        elif signal == 'SELL':
            if symbol in self.positions and self.positions[symbol]['quantity'] > 0:
                current_qty = self.positions[symbol]['quantity']
                sell_qty = min(quantity, current_qty)
                if sell_qty > 0:
                    self._execute_sell(symbol, price, sell_qty, timestamp)

    def _execute_buy(self, symbol, price, quantity, timestamp):
        cost = price * quantity
        self.cash -= cost
        
        if symbol in self.positions:
            # Update existing position
            current_qty = self.positions[symbol]['quantity']
            current_avg = self.positions[symbol]['avg_price']
            total_cost = (current_avg * current_qty) + cost
            new_qty = current_qty + quantity
            new_avg = total_cost / new_qty
            
            self.positions[symbol]['quantity'] = new_qty
            self.positions[symbol]['avg_price'] = new_avg
            # Adjust stop & take-profit
            self.positions[symbol]['stop_price'] = new_avg * (1 - self.stop_loss_pct)
            self.positions[symbol]['take_profit_price'] = new_avg * (1 + self.take_profit_pct)
        else:
            # Create new position
            self.positions[symbol] = {
                'quantity': quantity,
                'avg_price': price,
                'stop_price': price * (1 - self.stop_loss_pct),
                'take_profit_price': price * (1 + self.take_profit_pct),
            }
        
        self.trade_log.append({
            'timestamp': timestamp,
            'symbol': symbol,
            'action': 'BUY',
            'price': price,
            'quantity': quantity,
        })

    def _execute_sell(self, symbol, price, quantity, timestamp):
        """
        Execute a SELL, updating cash & realized PnL.
        """
        if symbol not in self.positions or self.positions[symbol]['quantity'] < quantity:
            return  # Shouldn't happen, but just in case

        avg_price = self.positions[symbol]['avg_price']
        revenue = price * quantity
        realized_trade_pnl = (price - avg_price) * quantity

        # Update realized PnL & cash
        self.realized_pnl += realized_trade_pnl
        self.cash += revenue

        self.positions[symbol]['quantity'] -= quantity
        if self.positions[symbol]['quantity'] <= 0:
            del self.positions[symbol]

        self.trade_log.append({
            'timestamp': timestamp,
            'symbol': symbol,
            'action': 'SELL',
            'price': price,
            'quantity': quantity,
            'realized_pnl': round(realized_trade_pnl, 2),
        })

    def check_stop_loss_take_profit(self, market_data, timestamp):
        """
        For each held position, check if:
          - current_price <= stop_price => SELL all
          - current_price >= take_profit_price => SELL all
          - trailing stop adjustments (if price is above avg, ratchet up stop)
        """
        prices = {d['symbol']: float(d['close']) for d in market_data}
        
        symbols_to_sell = []
        for sym, pos in self.positions.items():
            if sym not in prices:
                continue
            
            current_price = prices[sym]
            
            # Trailing Stop Update
            if current_price > pos['avg_price']:
                # If new price is highest so far, raise the stop
                new_stop = current_price * (1 - self.trailing_stop_buffer)
                if new_stop > pos['stop_price']:
                    pos['stop_price'] = new_stop

            # Stop-loss triggered
            if current_price <= pos['stop_price']:
                symbols_to_sell.append((sym, current_price, 'StopLoss'))
            # Take-profit triggered
            elif current_price >= pos['take_profit_price']:
                symbols_to_sell.append((sym, current_price, 'TakeProfit'))

        for (sym, sell_price, reason) in symbols_to_sell:
            qty = self.positions[sym]['quantity']
            self._execute_sell(sym, sell_price, qty, f"{timestamp} ({reason})")

    def partial_close(self, fraction, timestamp):
        """
        Sells a fraction (0.5 = 50%, etc.) of each open position.
        Useful near the end of day to lock partial profits.
        """
        for sym in list(self.positions.keys()):
            current_qty = self.positions[sym]['quantity']
            if current_qty > 0:
                qty_to_sell = int(current_qty * fraction)
                if qty_to_sell < 1:
                    continue
                sell_price = self.positions[sym]['avg_price']  # or last known price
                self._execute_sell(sym, sell_price, qty_to_sell, 
                                   f"{timestamp} (PartialClose {fraction*100:.0f}%)")

    def close_all_positions(self, timestamp):
        """
        Liquidate all positions (e.g. at 16:00).
        """
        for sym in list(self.positions.keys()):
            qty = self.positions[sym]['quantity']
            avg_price = self.positions[sym]['avg_price']  # or last known close
            self._execute_sell(sym, avg_price, qty, f"{timestamp} (EOD Liquidation)")

    def update_valuation(self, timestamp, market_data):
        """
        total_value = self.cash + sum(value of open positions).
        unrealized_pnl = sum((current_price - avg_price)*quantity).
        Returns (total_value, unrealized_pnl, realized_pnl).
        """
        total_value = self.cash
        unrealized_pnl = 0.0

        prices = {d['symbol']: float(d['close']) for d in market_data}
        
        for sym, pos in self.positions.items():
            qty = pos['quantity']
            avg_price = pos['avg_price']
            current_price = prices.get(sym, avg_price)
            
            total_value += current_price * qty
            unrealized_pnl += (current_price - avg_price) * qty

        # Log valuation
        self.history.append({
            'timestamp': timestamp,
            'total_value': total_value,
            'realized_pnl': self.realized_pnl,
            'unrealized_pnl': unrealized_pnl,
            'positions': len(self.positions)
        })

        return total_value, unrealized_pnl, self.realized_pnl


###############################################################################
#                         HELPER FUNCTIONS                                    #
###############################################################################

def is_end_of_day(timestamp_str):
    """
    True if hour >= 16 (4PM).
    """
    try:
        dt = datetime.fromisoformat(timestamp_str)
        if dt.hour >= 16:
            return True
    except ValueError:
        pass
    return False

def get_time(timestamp_str):
    """
    Extract time component from ISO string
    """
    try:
        dt = datetime.fromisoformat(timestamp_str)
        return dt.time()
    except ValueError:
        return None


###############################################################################
#                        MAIN TCP CLIENT LOOP                                 #
###############################################################################

def start_client():
    strategy = TradingStrategy()
    portfolio = PortfolioManager()
    
    # Connect to local TCP server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print(f"✅ Connected to server at {HOST}:{PORT}")
        
        # CSV file for reporting
        report_file = 'data/trading_session_report.csv'
        with open(report_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Total Value', 'Realized PnL', 'Unrealized PnL', 'Positions Held'])
        
        # Main loop
        while True:
            try:
                data = s.recv(65535)
                if not data:
                    break  # No more data or server disconnected
                
                # Parse JSON from server (15-min bars)
                message = json.loads(data.decode())
                timestamp = message['timestamp']
                securities = message['data']
                
                print(f"\n📅 Received market data for {timestamp}")
                
                # 1. Strategy updates & signals
                for sec in securities:
                    symbol = sec['symbol']
                    strategy.update_buffers(symbol, sec)
                    signal = strategy.generate_signal(symbol, sec)
                    
                    # 2. Execute signals
                    if signal:
                        price = float(sec['close'])
                        print(f"🚨 Signal: {symbol} {signal} at ${price:.2f}")
                        portfolio.execute_trade(symbol, signal, price, timestamp)
                
                # 3. Check stops / trailing / take-profit
                portfolio.check_stop_loss_take_profit(securities, timestamp)
                
                # 4. Partial closures at 15:30 & 15:45 (optional)
                t = get_time(timestamp)
                if t == time(15, 30):
                    print(f"⏰ 15:30 - Partial Exit (50%)")
                    portfolio.partial_close(0.5, timestamp)
                elif t == time(15, 45):
                    print(f"⏰ 15:45 - Partial Exit (50%)")
                    portfolio.partial_close(0.5, timestamp)
                
                # 5. End-of-day liquidation
                if is_end_of_day(timestamp):
                    print(f"⏰ End of Day Reached – Closing all positions...")
                    portfolio.close_all_positions(timestamp)
                
                # 6. Calculate portfolio valuation
                total_value, unrealized, realized = portfolio.update_valuation(timestamp, securities)
                
                # 7. Print summary
                print(f"💰 Portfolio Summary:")
                print(f"   Cash:          ${portfolio.cash:,.2f}")
                print(f"   Total Value:   ${total_value:,.2f}")
                print(f"   Realized PnL:  ${realized:+,.2f}")
                print(f"   Unrealized PnL: ${unrealized:+,.2f}")
                print(f"   Positions Held: {len(portfolio.positions)}")
                
                # 8. Log to CSV
                with open(report_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        timestamp,
                        round(total_value, 2),
                        round(realized, 2),
                        round(unrealized, 2),
                        len(portfolio.positions)
                    ])
                    
            except json.JSONDecodeError:
                print("⚠️ Invalid JSON data received, skipping...")
                continue
            except KeyboardInterrupt:
                print("\n🔴 Ctrl+C detected – shutting down gracefully...")
                break
    
    print("\n✅ Trading session ended")


if __name__ == "__main__":
    start_client()
