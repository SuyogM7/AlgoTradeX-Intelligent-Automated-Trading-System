import os
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

# Load environment variables
load_dotenv()

# Retrieve API credentials
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    raise ValueError("Missing API credentials. Ensure they are set as environment variables or in a .env file.")

# Initialize Alpaca Trading Client (Paper Trading Mode)
trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# Fetch account details
account = trading_client.get_account()

print(f"Account balance: ${account.cash}")
print(f"Portfolio value: ${account.portfolio_value}")
print(f"Buying power: ${account.buying_power}")
print(f"Equity: ${account.equity}")