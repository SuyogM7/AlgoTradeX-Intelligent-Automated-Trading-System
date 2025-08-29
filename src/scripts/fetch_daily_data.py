import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest

# Load environment variables
load_dotenv()

# Retrieve API credentials
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    raise ValueError("Missing API credentials. Ensure they are set as environment variables or in a .env file.")

# Initialize Alpaca Historical Data Client
stock_historical_data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

stock_tickers = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "BRK.B", "NVDA", "JPM", "TSLA", "JNJ",
    "V", "PG", "UNH", "HD", "MA", "BAC", "DIS", "PYPL", "CRM", "PFE",
    "VZ", "INTC", "CMCSA", "NFLX", "XOM", "KO", "ABT", "T", "NKE", "MRK",
    "WMT", "ADP", "CVX", "TMO", "ORCL", "IBM", "ACN", "MCD", "MDLZ", "UPS",
    "COST", "HON", "DHR", "QCOM", "MDT", "TXN", "AVGO", "SPGI", "ADBE", "LLY"
]

etfs = [
    "SPY",   # S&P 500
    "DIA",   # Dow Jones Industrial Average
    "QQQ",   # Nasdaq-100
    "IWM",   # Russell 2000
    "VTI",   # Total US Stock Market
    "IYT",   # Dow Jones Transportation Average
    "IDU",   # Dow Jones Utility Average
    "VXX",   # Volatility ETF
]

now = datetime.now(ZoneInfo("America/Chicago"))

###### NOTE: Modify the timeframe and start datetime as needed ######
req = StockBarsRequest(
    symbol_or_symbols=stock_tickers + etfs,  # specify symbol or symbols
    timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Day),  # specify timeframe
    start=now - timedelta(days=5*365),  # specify start datetime, last 5 years
    adjustment=Adjustment.ALL,  # specify adjustment
    # end_date=None,  # specify end datetime, default=now
    # limit=2,  # specify limit
)

df = stock_historical_data_client.get_stock_bars(req).df
df = df.sort_values(by=["timestamp", "symbol"])

# Rename CSV file to match requested data
df.to_csv(os.path.join("data", "historical_stock_data_daily_5_yrs.csv"), index=True)
print("Data successfully saved to CSV")