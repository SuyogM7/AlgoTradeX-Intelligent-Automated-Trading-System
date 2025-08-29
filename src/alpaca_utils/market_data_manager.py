import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest

class MarketDataManager:
    """
    Manages market data retrieval from Alpaca API.
    Fetches historical stock and ETF data, retrieving 5-minute bars for the past 5 days by default.
    """

    def __init__(self, timeframe=TimeFrame(5, TimeFrameUnit.Minute), days=5):
        """Initialize Alpaca API client and set query parameters."""
        load_dotenv()

        self.api_key = os.getenv("ALPACA_API_KEY")
        self.secret_key = os.getenv("ALPACA_SECRET_KEY")

        if not self.api_key or not self.secret_key:
            raise ValueError("Missing API credentials. Ensure they are set in the .env file.")

        self.client = StockHistoricalDataClient(self.api_key, self.secret_key)

        self.etfs = [
            "SPY", "QQQ", "IWM",  # Core market ETFs (S&P 500, Nasdaq, Russell 2000)
            "XLE",  # Energy Sector ETF
            "VXX"  # Volatility hedge (risk-off protection)
        ]

        self.stock_tickers = [
            "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA",  # Tech giants (liquid + volatile)
            "TSLA", "NFLX", "DIS",  # High-beta momentum stocks (EV, streaming, media)
            "GS", "PYPL", "XYZ",  # More volatile financials
            "XOM"   # Major Energy stock
        ]

        self.timeframe = timeframe
        self.days = days

    def fetch_historical_data(self, symbol=None):
        """
        Fetches historical 5-minute bars for all securities or a specific symbol for the last `self.days` days.

        :param symbol: (Optional) Fetch historical data for a specific stock/ETF.
        :return: DataFrame with historical market data.
        """
        now = datetime.now(ZoneInfo("America/New_York"))

        # Determine which symbols to request
        symbols_to_fetch = [symbol] if symbol else self.etfs + self.stock_tickers

        request = StockBarsRequest(
            symbol_or_symbols=symbols_to_fetch,
            timeframe=self.timeframe,
            start=now - timedelta(days=self.days),
            adjustment=Adjustment.ALL,
        )

        print(f"\n📡 Fetching {self.timeframe} bars for {symbol if symbol else 'all securities'} over the last {self.days} days...")

        try:
            df = self.client.get_stock_bars(request).df

            if df.empty:
                print("❌ No data received. Check your API or market hours.")
                return None

            df = df.reset_index()
            df = df.sort_values(by=["timestamp", "symbol"])

            print(f"✅ Successfully fetched {len(df)} rows of historical data for {symbol if symbol else 'all securities'}.")
            return df

        except Exception as e:
            print(f"❌ Error fetching data: {e}")
            return None
