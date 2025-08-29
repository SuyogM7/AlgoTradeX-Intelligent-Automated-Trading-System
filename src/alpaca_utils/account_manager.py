import os
from datetime import datetime, timezone

import pandas as pd
import pytz
from dotenv import load_dotenv

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest, GetPortfolioHistoryRequest
from alpaca.trading.enums import QueryOrderStatus, OrderSide, OrderStatus
from alpaca.common.exceptions import APIError

class AccountManager:
    """
    Manages account and portfolio tasks (excluding order placement).
    """

    def __init__(self, paper=True):
        """Initialize the Alpaca Trading Client for account management."""
        # Load environment variables
        load_dotenv()

        self.API_KEY = os.getenv("ALPACA_API_KEY")
        self.SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

        if not self.API_KEY or not self.SECRET_KEY:
            raise ValueError("Missing API credentials. Ensure they are set in the .env file.")

        # Initialize Alpaca Trading Client (paper trading mode enabled by default)
        self.client = TradingClient(self.API_KEY, self.SECRET_KEY, paper=paper)

    def get_account_details(self):
        """
        Fetch and return account details like balance, equity, and buying power.
        Includes account status, cash/margin breakdown, and realized PnL.
        """
        try:
            account = self.client.get_account()
            return {
                "status": account.status,
                "equity": float(account.equity),
                "cash": float(account.cash),
                "buying_power": float(account.buying_power),
                "maintenance_margin": float(account.maintenance_margin),
                "realized_pnl": float(account.equity) - float(account.last_equity),  # Realized PnL today
                "margin_available": float(account.regt_buying_power),
            }
        except APIError as e:
            print(f"❌ Error fetching account details: {e}")
            return None

    def get_positions(self):
        """
        Fetch all open positions and return them as a list of dictionaries.
        """
        try:
            positions = self.client.get_all_positions()
            position_data = []
            
            for pos in positions:
                position_data.append({
                    "symbol": pos.symbol,
                    "qty": float(pos.qty),
                    "market_value": float(pos.market_value),
                    "cost_basis": float(pos.cost_basis),
                    "unrealized_pl": float(pos.unrealized_pl),
                    "unrealized_plpc": float(pos.unrealized_plpc) * 100,  # Convert to percentage
                })

            return position_data

        except APIError as e:
            print(f"❌ Error fetching positions: {e}")
            return []

    def get_closed_positions(self):
        """
        Fetch closed positions from trade history by retrieving past completed orders.
        Properly calculates cost basis by matching SELL orders to prior BUY orders.
        """
        try:
            closed_positions = []
            request = GetOrdersRequest(status=QueryOrderStatus.CLOSED)
            orders = self.client.get_orders(request)

            # Step 1: Separate BUY and SELL orders
            buy_orders = {}
            sell_orders = []

            for order in orders:
                if order.side == OrderSide.BUY and order.status == OrderStatus.FILLED:
                    buy_orders.setdefault(order.symbol, []).append(order)  # Store buy orders by symbol
                elif order.side == OrderSide.SELL and order.status == OrderStatus.FILLED:
                    sell_orders.append(order)  # Store sell orders

            # Step 2: Calculate Realized P/L by matching sells with previous buys
            for sell_order in sell_orders:
                symbol = sell_order.symbol
                filled_qty = float(sell_order.filled_qty)
                avg_sell_price = float(sell_order.filled_avg_price)

                # 🔹 Market Value (Total revenue from selling)
                market_value = filled_qty * avg_sell_price

                # 🔹 Find matching BUY order(s) for cost basis
                if symbol in buy_orders:
                    total_cost = 0
                    remaining_qty = filled_qty

                    for buy_order in buy_orders[symbol]:
                        if remaining_qty <= 0:
                            break  # We matched all the sell quantity

                        buy_qty = float(buy_order.filled_qty)
                        avg_buy_price = float(buy_order.filled_avg_price)

                        # Match available buy quantity to the sell quantity
                        match_qty = min(remaining_qty, buy_qty)
                        total_cost += match_qty * avg_buy_price
                        remaining_qty -= match_qty  # Reduce remaining sell qty

                    # 🔹 Cost Basis (Total cost of originally bought shares)
                    cost_basis = total_cost

                    # 🔹 Realized P/L (Profit or loss from selling)
                    realized_pnl = round(market_value - cost_basis, 2)

                    closed_positions.append({
                        "symbol": symbol,
                        "filled_qty": filled_qty,
                        "avg_fill_price": avg_sell_price,
                        "market_value": round(market_value, 2),
                        "cost_basis": round(cost_basis, 2),
                        "realized_pnl": realized_pnl,
                    })

            return closed_positions

        except APIError as e:
            print(f"❌ Error fetching closed positions: {e}")
            return []

    def get_account_history(self, period="7D", timeframe="1H"):
        """
        Fetch portfolio history for tracking PnL and performance.
        :param period: Time period (e.g., '5D' for 5 days, '1M' for 1 month)
        :param timeframe: Time interval (e.g., '1H' for hourly, '1D' for daily)
        """
        try:
            request = GetPortfolioHistoryRequest(
                period=period,
                timeframe=timeframe,
                extended_hours=False,
                pnl_reset="1D",
                intraday_reporting="extended_hours"
            )
            history = self.client.get_portfolio_history(request)

            # Convert timestamps to Eastern Time (ET)
            utc_zone = pytz.utc
            et_zone = pytz.timezone("America/New_York")

            data = {
                "timestamp": history.timestamp,
                "equity": history.equity,
                "profit_loss": history.profit_loss,
                "profit_loss_pct": history.profit_loss_pct,
                "base_value": history.base_value,
                "timeframe": history.timeframe,
                "datetime": [
                    datetime.fromtimestamp(ts, timezone.utc)
                    .replace(tzinfo=utc_zone)
                    .astimezone(et_zone)
                    .strftime('%Y-%m-%d %H:%M:%S %Z')  # Includes timezone info
                    for ts in history.timestamp
                ],
            }

            df = pd.DataFrame(data)
            df = df[['datetime', 'equity', 'profit_loss', 'profit_loss_pct', 'base_value', 'timeframe']]
            # Save to CSV
            csv_filename = "./data/alpaca_portfolio_history.csv"
            df.to_csv(csv_filename, index=False)

            print(f"✅ Portfolio history saved to {csv_filename}")
            return df

        except APIError as e:
            print(f"❌ Error fetching portfolio history: {e}")
            return None

    def get_open_orders(self):
        """
        Fetch all currently open orders.
        """
        try:
            request = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            open_orders = self.client.get_orders(request)
            return open_orders

        except APIError as e:
            print(f"❌ Error fetching open orders: {e}")
            return []

    def get_market_clock_data(self):
        """
        Check if the stock market is open before fetching real-time positions or placing trades.
        """
        try:
            clock = self.client.get_clock()
            return clock.timestamp, clock.is_open, clock.next_open, clock.next_close
        except APIError as e:
            print(f"❌ Error fetching market clock: {e}")
            return None

    def close_all_positions(self):
        """
        Close all open positions and cancel any existing open orders.
        """
        try:
            self.client.close_all_positions(cancel_orders=True)
            print("✅ All positions closed, and open orders canceled.")
        except APIError as e:
            print(f"❌ Error closing positions: {e}")
