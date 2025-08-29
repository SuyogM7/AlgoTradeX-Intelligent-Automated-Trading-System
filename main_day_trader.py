import os
import time
import datetime

from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from src.alpaca_utils.market_data_manager import MarketDataManager
from src.alpaca_utils.account_manager import AccountManager
from src.alpaca_utils.trade_manager import TradeManager
from src.alpaca_utils.trading_strategy import TradingStrategy
from src.alpaca_utils.risk_manager import RiskManager

# -------------------
# Initialize modules
# -------------------
account_manager = AccountManager(paper=True)
market_data_manager = MarketDataManager(
    timeframe=TimeFrame(15, TimeFrameUnit.Minute), 
    days=7
)
trading_strategy = TradingStrategy()
trade_manager = TradeManager()
risk_manager = RiskManager(
    risk_per_trade=0.01, 
    atr_period=14, 
    atr_multiplier=1, 
    risk_reward_ratio=2, 
    max_position_fraction=0.01, 
    max_open_positions=8,
    max_notional_ratio=0.50
)

def fetch_account_details():
    # -------------------
    # Fetch account details
    # -------------------
    account_info = account_manager.get_account_details()
    print(f"📈 Equity: ${account_info['equity']}")
    print(f"💰 Account Balance: ${account_info['cash']}")
    print(f"💵 Buying Power: ${account_info['buying_power']}")
    print(f"🔄 Profit/Loss Today: ${round(account_info['realized_pnl'], 2)}")
    print(f"📊 Margin Available: ${account_info['margin_available']}")

    # -------------------
    # Fetch open positions
    # -------------------
    positions = account_manager.get_positions()
    if positions:
        print("\n📌 Open Positions:")
        for pos in positions:
            print(f" - {pos['symbol']}: {pos['qty']} shares, Market Value: ${pos['market_value']}, "
                f"Unrealized P/L: ${pos['unrealized_pl']} ({pos['unrealized_plpc']:.2f}%)")
    else:
        print("\n❌ No open positions.")


def run_day_trader():
    """
    Runs the main trading loop for the day-trading bot.

    - Continuously checks market open/close status.
    - Iterates through the securities and fetches historical data.
    - Updates the strategy buffer and generates trade signals.
    - Applies risk management constraints before placing trades.
    - Places market orders with stop-loss and take-profit parameters.
    - Sleeps between iterations to align with 15-minute trading cycles.
    
    The loop runs until an hour before market close, at which point all positions are closed.
    """

    while True:
        
        # -------------------
        # Check Market Status
        # -------------------
        # - If the market is closed, the script sleeps until the next open + 1 hour.
        # - If it's close to the end of the trading session, all positions are closed to avoid overnight risk.

        current_time, is_open, next_open, next_close = account_manager.get_market_clock_data()
        time_until_close = (next_close - current_time).total_seconds()

        # Define trading start time (one hour after market open)
        trading_start_time = next_open + datetime.timedelta(hours=1)

        if not is_open:
            sleep_time = max((trading_start_time - current_time).total_seconds(), 0)
            print(f"\n⏳ Market closed at {current_time.strftime('%Y-%m-%d %H:%M:%S')}. Waiting {int(sleep_time // 60)} minutes until trading starts at {trading_start_time}...")
            time.sleep(sleep_time)
            continue  # Restart loop after waking up

        # Stop trading if market close time is less than an hour away
        if int(time_until_close / 60) <= 60:
            print("\n🏁 Market about to close in an hour. Closing all positions.")
            account_manager.close_all_positions()
            print("✅ All positions closed. Waiting for next market open.")

            # Ensure bot sleeps until next market open + 1 hour
            sleep_time = max((trading_start_time - current_time).total_seconds(), 0)
            print(f"🛑 Market closed. Sleeping {int(sleep_time // 60)} minutes until {trading_start_time}.")
            time.sleep(sleep_time)
            continue  # Restart loop after waking up

        # -------------------
        # Loop through all securities
        # -------------------
        print(f'\n🚀 Running Day Trader at {current_time.strftime("%Y-%m-%d %H:%M:%S")}...')

        for symbol in market_data_manager.etfs + market_data_manager.stock_tickers:
            symbol_data = market_data_manager.fetch_historical_data(symbol=symbol)

            if symbol_data is None or symbol_data.empty:
                print(f"⚠️ No data found for {symbol}, skipping...")
                continue

            # Update strategy buffer
            for _, row in symbol_data.iterrows():
                trading_strategy.update_buffer(symbol, row)

            # Generate trade signal
            trade_signal = trading_strategy.generate_trade_signal(symbol)
            print(f"📈 Trade Signal for {symbol}: {trade_signal}")

            if trade_signal in ["BUY", "SELL"]:
                # Re-fetch latest account details
                account_info = account_manager.get_account_details()
                open_positions = account_manager.get_positions()

                # Skip if we already have an open position for this symbol
                if any(pos["symbol"] == symbol for pos in open_positions):
                    print(f"🚫 Skipping {symbol}. Already have an open position.")
                    continue  # Skip this symbol and move to the next one

                entry_price = symbol_data.iloc[-1]['close']
                print(f"Entry Price for {symbol}: {entry_price}")

                # Calculate trade parameters (includes portfolio-level risk checks)
                risk_params = risk_manager.calculate_trade_parameters(
                    df=symbol_data,
                    entry_price=entry_price,
                    account_info=account_info,
                    open_positions=open_positions,
                    side=trade_signal
                )

                print("Calculated Risk Parameters:", risk_params)

                if risk_params["quantity"] <= 0:
                    print(f"🚫 Skipping {symbol}. Quantity is zero or invalid.")
                    continue  # Skip trade if no valid quantity

                print(f"Trade Signal: {trade_signal}, Entry Price: {entry_price}, "
                    f"Quantity: {risk_params['quantity']}, Total Value: {entry_price * risk_params['quantity']}")

                # If all checks pass, place the order
                print(f"Placing {trade_signal} order for {symbol}...")
                try:
                    order_response = trade_manager.place_market_order(
                        symbol=symbol,
                        qty=risk_params["quantity"],
                        side=trade_signal.lower(),
                        stop_loss_price=None,
                        take_profit_price=risk_params["take_profit"]
                    )

                    if order_response:
                        minimal_order_info = {
                            "id": order_response.id,
                            "symbol": order_response.symbol,
                            "qty": order_response.qty,
                            "filled_qty": order_response.filled_qty,
                            "side": order_response.side,
                            "type": order_response.type,
                            "status": order_response.status,
                            "created_at": order_response.created_at,
                            "filled_at": order_response.filled_at
                        }
                        print("✅ Successfully executed market order:", minimal_order_info)

                        trail_amt = risk_params["trail_amount"]
                        # Wait for order to fill before placing the trailing stop
                        filled_qty = trade_manager.wait_for_order_fill(order_response.id)

                        if filled_qty and filled_qty > 0:
                            ts_response = trade_manager.place_trailing_stop_order(
                                symbol=symbol,
                                qty=filled_qty,  # Use actual filled quantity
                                side=trade_signal.lower(),
                                trail_price=trail_amt
                            )
                            
                            if ts_response:
                                print(f"✅ Trailing stop placed for {symbol} at ${trail_amt} offset.")
                            else:
                                print(f"⚠️ Could not place trailing stop for {symbol}.")
                        else:
                            print(f"⚠️ Skipping trailing stop for {symbol} because order was not fully filled.")
                    else:
                        print("⚠️ No order response received.")
                except Exception as e:
                    print(f"🚨 Error placing order for {symbol}: {e}")
                    continue
        
        # Sleep before fetching new data
        sleep_time = min(time_until_close, 900)  # Sleep 15 minutes
        print(f"🕒 Sleeping for {sleep_time} seconds before next check...")
        time.sleep(sleep_time)

if __name__ == '__main__':
    fetch_account_details()
    run_day_trader()