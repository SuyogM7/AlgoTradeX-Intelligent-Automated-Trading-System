#!/usr/bin/env python3

import os
import csv
import pandas as pd
import numpy as np
from three_strategy_client import TradingStrategy, PortfolioManager

# ------------------------
# Configurable Parameters
# ------------------------
DATA_PATH = "./data/historical_stock_data_15min_1year.csv"  # Adjust if needed
RESULTS_PATH = "./data/backtest_results.csv"

# Print progress every N rows
PRINT_EVERY = 5000

# ------------------------
# Main Backtest Function
# ------------------------
def main():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"❌ CSV not found at {DATA_PATH}")

    print(f"✅ Loading backtest data from: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    if df.empty:
        raise ValueError("❌ The CSV file is empty. Cannot run backtest.")

    # Ensure correct format & sorting
    if "timestamp" not in df.columns:
        raise ValueError("❌ 'timestamp' column not found in CSV.")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(by=["timestamp", "symbol"]).reset_index(drop=True)

    print(f"🚀 Running backtest on {len(df)} rows...")

    # Initialize strategy & portfolio
    strategy = TradingStrategy()
    portfolio = PortfolioManager()

    # Prepare CSV logging
    # We'll write each row as we go so we can track partial progress.
    with open(RESULTS_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "TotalValue", "UnrealizedPnL", "PositionsHeld"])

    # Main loop
    for i, row in enumerate(df.itertuples(index=False), start=1):
        # Convert to dict for the strategy
        market_data = row._asdict()

        # Update strategy buffers & generate signals
        strategy.update_buffers(market_data["symbol"], market_data)
        signal = strategy.generate_signal(market_data["symbol"], market_data)

        if signal:
            price = float(market_data["close"])
            # Execute trade
            portfolio.execute_trade(market_data["symbol"], signal, price, market_data["timestamp"])
            
            # Debug: Only print when signal is generated
            if i < 5000:
                print(f"{i} -> {signal}, {market_data['symbol']}, ${price:.2f}, cash={portfolio.cash}")

        # Update portfolio valuation
        total_value, unrealized = portfolio.update_valuation(market_data["timestamp"], [market_data])

        # Write partial result to CSV
        with open(RESULTS_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                market_data["timestamp"],
                round(total_value, 2),
                round(unrealized, 2),
                len(portfolio.positions)
            ])

        # Print progress occasionally
        if i % PRINT_EVERY == 0:
            print(f"...processed {i} rows so far. Current TotalValue={round(total_value,2)}")


    print(f"\n✅ Backtest Complete! Processed {len(df)} rows in total.")
    print(f"📊 Results saved to: {RESULTS_PATH}")

# ------------------------
# Entry Point
# ------------------------
if __name__ == "__main__":
    main()
