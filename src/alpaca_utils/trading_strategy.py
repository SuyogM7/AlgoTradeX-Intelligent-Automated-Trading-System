import pandas as pd
import numpy as np
from collections import defaultdict, deque

class TradingStrategy:
    """
    Implements a trading strategy using four key indicators:
      1. 200 EMA
      2. VWAP + Volume Filter
      3. Commodity Channel Index (CCI)
      4. EMA Crossover

    A trade signal ('BUY' or 'SELL') is generated if at least 3 out of 4 indicators agree.
    """

    def __init__(self):
        """Initializes the trading strategy."""
        self.data_buffers = defaultdict(lambda: deque(maxlen=300))  # Store rolling market data for each symbol

    def update_buffer(self, symbol, data_point):
        """Stores incoming market data for a given symbol."""
        self.data_buffers[symbol].append(data_point)

    def calculate_indicators(self, df):
        """Computes four key indicators for trading decisions."""
        df = df.copy()
        df[['close', 'high', 'low', 'volume']] = df[['close', 'high', 'low', 'volume']].astype(float)

        if 'vwap' in df.columns:
            df['vwap'] = df['vwap'].astype(float)

        # 1️⃣ 200 EMA Indicator
        df["200_EMA"] = df['close'].ewm(span=200, min_periods=200).mean()
        indicator1 = 0 if pd.isna(df["200_EMA"].iloc[-1]) else (1 if df['close'].iloc[-1] > df["200_EMA"].iloc[-1] else -1)

        # 2️⃣ VWAP + Volume Filter Indicator
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

        # 3️⃣ CCI Indicator
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        sma = df["typical_price"].rolling(window=20).mean()
        mad_vals = df["typical_price"].rolling(window=20).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
        cci = (df["typical_price"] - sma) / (0.015 * mad_vals)
        indicator3 = 0 if np.isnan(cci.iloc[-1]) else (1 if cci.iloc[-1] > 100 else (-1 if cci.iloc[-1] < -100 else 0))

        # 4️⃣ EMA Crossover Indicator (9 EMA vs. 26 EMA)
        df["9_EMA"] = df['close'].ewm(span=9, min_periods=9).mean()
        df["26_EMA"] = df['close'].ewm(span=26, min_periods=26).mean()
        indicator4 = 0 if pd.isna(df["26_EMA"].iloc[-1]) else (1 if df["9_EMA"].iloc[-1] > df["26_EMA"].iloc[-1] else -1)

        return indicator1, indicator2, indicator3, indicator4

    def generate_trade_signal(self, symbol):
        """Determines whether to 'BUY', 'SELL', or hold based on indicator agreement."""
        if len(self.data_buffers[symbol]) < 200:
            return None  # Not enough data

        df = pd.DataFrame(list(self.data_buffers[symbol]))
        ind1, ind2, ind3, ind4 = self.calculate_indicators(df)

        buy_signals = sum(1 for i in [ind1, ind2, ind3, ind4] if i == 1)
        sell_signals = sum(1 for i in [ind1, ind2, ind3, ind4] if i == -1)

        if buy_signals >= 3:
            return 'BUY'
        elif sell_signals >= 3:
            return 'SELL'
        else:
            return None