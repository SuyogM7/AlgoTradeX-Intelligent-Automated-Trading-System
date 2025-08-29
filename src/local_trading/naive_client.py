import socket
import json
import csv
import datetime

# Server configuration
HOST = "127.0.0.1"  
PORT = 9999         

# Portfolio & Trading Settings
starting_cash = 100000  # Start with $100K
cash_balance = starting_cash
portfolio = {}  # Example: { "AAPL": {"quantity": 10, "avg_price": 150.00, "last_close": 150.00} }
log_file = "data/trading_session_report.csv"

def start_client():
    """ Connects to the server and receives the finance price stream. """
    global cash_balance, portfolio

    # Create a TCP socket
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        # Connect to the finance server
        client.connect((HOST, PORT))
        print(f"✅ Connected to server at {HOST}:{PORT}")

        buffer = ""

        # Initialize CSV file with headers
        with open(log_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Timestamp", "Cash Balance", "Total Equity", "Unrealized PnL"])

        # Continuously receive data from the server
        while True:
            chunk = client.recv(4096).decode("utf-8")  # Large buffer to handle JSON list
            if not chunk:
                print("⚠️ Server closed connection.")
                break  # Exit if the server closes connection

            buffer += chunk

            # Process complete JSON objects
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)  # Extract one JSON object
                
                try:
                    message = json.loads(line.strip())  # Parse JSON safely
                    
                    if not isinstance(message, dict) or "timestamp" not in message or "data" not in message:
                        print(f"⚠️ Invalid message format: {message}")
                        continue  # Skip invalid messages

                    timestamp = message["timestamp"]
                    securities_data = message["data"]

                    print(f"\n📊 Market Data for {timestamp}:")
                    for security in securities_data:
                        print(f"  {security['symbol']}: Open={security['open']}, Close={security['close']}")

                    # Process the received securities data
                    trading_signals = process_market_data(securities_data)

                    # Print generated trading signals
                    for signal in trading_signals:
                        print(f"🚀 TRADE SIGNAL: {signal}")

                    # Calculate updated PnL and show portfolio status
                    display_portfolio(securities_data)

                    # Save snapshot to CSV after each valid market update
                    save_portfolio_snapshot(timestamp)

                except json.JSONDecodeError as e:
                    print(f"⚠️ Error parsing JSON: {line} ({e})")

    except ConnectionRefusedError:
        print("⚠️ Error: Could not connect to server. Make sure it is running.")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
    finally:
        client.close()
        save_portfolio_snapshot("Final Snapshot")  # Save final state
        print("✅ Trading session ended. Report saved.")

def process_market_data(securities_data):
    """ Simple strategy: Buy if close is lower than open, otherwise sell. """
    global cash_balance, portfolio
    signals = []
    
    for security in securities_data:
        symbol = security["symbol"]
        open_price = float(security["open"])
        close_price = float(security["close"])

        if close_price < open_price:
            # Buy Signal
            signals.append(f"BUY {symbol} at {close_price}")
            execute_trade(symbol, "BUY", close_price)
        else:
            # Sell Signal
            signals.append(f"SELL {symbol} at {close_price}")
            execute_trade(symbol, "SELL", close_price)

    return signals

def execute_trade(symbol, action, price, quantity=10):
    """ Executes buy/sell trades and updates portfolio """
    global cash_balance, portfolio

    if action == "BUY":
        cost = price * quantity
        if cash_balance >= cost:
            cash_balance -= cost
            if symbol in portfolio:
                total_cost = (portfolio[symbol]["quantity"] * portfolio[symbol]["avg_price"]) + cost
                total_quantity = portfolio[symbol]["quantity"] + quantity
                portfolio[symbol]["avg_price"] = total_cost / total_quantity
                portfolio[symbol]["quantity"] += quantity
            else:
                portfolio[symbol] = {"quantity": quantity, "avg_price": price, "last_close": price}
        else:
            print(f"⚠️ Not enough cash to buy {symbol} at {price}")
    elif action == "SELL":
        if symbol in portfolio and portfolio[symbol]["quantity"] >= quantity:
            cash_balance += price * quantity
            portfolio[symbol]["quantity"] -= quantity
            if portfolio[symbol]["quantity"] == 0:
                del portfolio[symbol]
        else:
            print(f"⚠️ Not enough {symbol} shares to sell at {price}")

def display_portfolio(securities_data):
    """ Calculates and displays PnL, cash balance, and equity value.
        Also updates each holding's last_close price from current market data.
    """
    global cash_balance, portfolio

    equity_value = cash_balance  # Start with cash balance
    unrealized_pnl = 0

    print("\n💰 Portfolio Status:")
    print(f"Cash Balance: ${cash_balance:,.2f}")

    if portfolio:
        print("📈 Holdings:")
        for security in securities_data:
            symbol = security["symbol"]
            if symbol in portfolio:
                close_price = float(security["close"])
                portfolio[symbol]["last_close"] = close_price
                quantity = portfolio[symbol]["quantity"]
                avg_price = portfolio[symbol]["avg_price"]
                
                market_value = close_price * quantity
                equity_value += market_value
                unrealized_pnl += (close_price - avg_price) * quantity
                
                print(f"  {symbol}: {quantity} shares | Avg Price: ${avg_price:.2f} | Last Close: ${close_price:.2f} | Market Value: ${market_value:,.2f}")

    print(f"\n💹 Unrealized PnL: ${unrealized_pnl:,.2f}")
    print(f"🏦 Total Equity Value: ${equity_value:,.2f}")
    print("-" * 50)

def save_portfolio_snapshot(timestamp):
    """Saves current PnL and portfolio status to a CSV file with all numbers rounded to 2 decimals."""
    global cash_balance, portfolio

    total_equity = cash_balance
    unrealized_pnl = 0

    for symbol, holdings in portfolio.items():
        last_close_price = holdings.get("last_close", holdings["avg_price"])
        market_value = holdings["quantity"] * last_close_price
        total_equity += market_value
        unrealized_pnl += (last_close_price - holdings["avg_price"]) * holdings["quantity"]

    cash_rounded = round(cash_balance, 2)
    total_equity_rounded = round(total_equity, 2)
    unrealized_pnl_rounded = round(unrealized_pnl, 2)

    # Append to CSV
    with open(log_file, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, cash_rounded, total_equity_rounded, unrealized_pnl_rounded])

    print(f"💾 Logged Portfolio Snapshot: Cash=${cash_rounded}, Total Equity=${total_equity_rounded}, PnL=${unrealized_pnl_rounded}")

if __name__ == "__main__":
    start_client()
