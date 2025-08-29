import os
import time
import pandas as pd
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

# Stock and ETF symbols
stock_tickers = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "JPM", "TSLA", "JNJ", "V",
    "PG", "UNH", "HD", "MA", "BAC", "DIS", "PYPL", "CRM", "PFE", "VZ",
    "INTC", "CMCSA", "NFLX", "XOM", "KO", "ABT", "T", "NKE", "MRK", "WMT",
    "ADP", "CVX", "TMO", "ORCL", "IBM", "ACN", "MCD", "MDLZ", "UPS", "COST",
    "HON", "DHR", "QCOM", "MDT", "TXN", "AVGO", "SPGI", "ADBE", "LLY"
]
etfs = ["SPY", "DIA", "QQQ", "IWM", "VTI", "IYT", "IDU", "VXX"]

symbols = stock_tickers + etfs

# Define timeframe and date range (Last 1 Year)
now = datetime.now(ZoneInfo("America/Chicago"))
start_date = now - timedelta(days=365)  # 1 year of data

# Alpaca rate limits: 200 requests per minute
BATCH_SIZE = 10  # Fetch 10 symbols per request
RATE_LIMIT_DELAY = 10  # Adjust based on API speed
MAX_RETRIES = 5  # Number of retries for failures

# Store data in a DataFrame
all_data = []

# Fetch data in batches
total_batches = (len(symbols) // BATCH_SIZE) + 1
for i in range(0, len(symbols), BATCH_SIZE):
    batch = symbols[i:i + BATCH_SIZE]
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"🔄 Fetching {len(batch)} symbols: {batch} (Batch {i//BATCH_SIZE + 1}/{total_batches})")

            # Create request
            req = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame(amount=15, unit=TimeFrameUnit.Minute),  # 15-Minute bars
                start=start_date,
                adjustment=Adjustment.ALL
            )

            # Fetch data
            df = stock_historical_data_client.get_stock_bars(req).df
            if not df.empty:
                all_data.append(df)
                print(f"✅ Retrieved {len(df)} rows for {len(batch)} symbols.")

            # Break retry loop on success
            break  

        except Exception as e:
            print(f"⚠️ Error fetching {batch}: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"🔄 Retrying in 5 seconds... ({attempt + 1}/{MAX_RETRIES})")
                time.sleep(5)
            else:
                print(f"❌ Failed to fetch {batch} after {MAX_RETRIES} attempts. Skipping.")

    # Respect rate limit (wait only if more symbols remain)
    if i + BATCH_SIZE < len(symbols):  
        print(f"⏳ Waiting {RATE_LIMIT_DELAY} seconds to avoid rate limits...")
        time.sleep(RATE_LIMIT_DELAY)

# Combine all data and save to CSV
if all_data:
    final_df = pd.concat(all_data)
    final_df = final_df.sort_values(by=["timestamp", "symbol"])
    final_df.to_csv(os.path.join("data", "historical_stock_data_15min_1year.csv"), index=True)
    print(f"✅ Data successfully saved to CSV. Total rows: {len(final_df)}")
else:
    print("❌ No data fetched.")