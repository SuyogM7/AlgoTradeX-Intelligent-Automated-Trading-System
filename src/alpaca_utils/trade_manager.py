import os
import time
from uuid import UUID

from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest, LimitOrderRequest,
    ClosePositionRequest, TakeProfitRequest, StopLossRequest,
    TrailingStopOrderRequest, GetOrderByIdRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, OrderType, OrderStatus
from alpaca.trading.requests import GetOrdersRequest

class TradeManager:
    """
    Handles trade execution using Alpaca API.
    Supports market orders, stop orders, and bracket orders.
    """

    def __init__(self, paper=True):
        """Initialize the Alpaca Trading Client for executing trades."""
        load_dotenv()

        self.api_key = os.getenv("ALPACA_API_KEY")
        self.secret_key = os.getenv("ALPACA_SECRET_KEY")

        if not self.api_key or not self.secret_key:
            raise ValueError("Missing API credentials. Ensure they are set in the .env file.")

        self.client = TradingClient(self.api_key, self.secret_key, paper=paper)

    def validate_trade(self, symbol, qty, side):
        """Ensure sufficient buying power for BUY and allow SELL to open short if we don't hold shares."""
        account = self.client.get_account()
        buying_power = float(account.buying_power)

        if side.lower() == "buy":
            # Check if we have enough buying power to go long
            if buying_power <= 0:
                print(f"❌ Insufficient buying power to BUY {qty} shares of {symbol}.")
                return False
        else:  # SELL case => either closing a position or opening a short
            # For a short sale, we only need to confirm we have enough margin (buying_power).
            # If user is actually SELLing existing shares, that should also be fine.
            if buying_power <= 0:
                print(f"❌ Insufficient buying power to SHORT {qty} shares of {symbol}.")
                return False

        return True

    def place_market_order(self, symbol, qty, side, stop_loss_price=None, take_profit_price=None):
        """
        Places a market order (buy/sell) with **bracket stop-loss and take-profit** orders.

        :param symbol: Stock ticker
        :param qty: Quantity to buy/sell
        :param side: 'buy' or 'sell'
        :param stop_loss_price: Price at which to trigger stop loss (optional)
        :param take_profit_price: Price at which to take profit (optional)
        """
        if not self.validate_trade(symbol, qty, side):
            return None

        try:
            # ✅ Correct Bracket Order Handling
            order_class = OrderClass.BRACKET if stop_loss_price and take_profit_price else OrderClass.SIMPLE

            # ✅ Ensure Stop-Loss isn't too close
            if stop_loss_price:
                min_sl_distance = stop_loss_price * 0.003  # 0.3% minimum distance
                stop_price = max(stop_loss_price, stop_loss_price - min_sl_distance if side.lower() == "buy" else stop_loss_price + min_sl_distance)
                stop_price = round(stop_price, 2)
                stop_limit_offset = stop_price * 0.001  # 0.1% offset
                stop_limit_price = round(stop_price - stop_limit_offset if side.lower() == "buy" else stop_price + stop_limit_offset, 2)
            
            take_profit_price = round(take_profit_price, 2) if take_profit_price else None

            order = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                order_class=order_class,
                take_profit=TakeProfitRequest(limit_price=take_profit_price) if take_profit_price else None,
                stop_loss=StopLossRequest(
                    stop_price=stop_price,
                    limit_price=stop_limit_price
                ) if stop_loss_price else None
            )

            response = self.client.submit_order(order)
            print(f"✅ Market order placed: {side.upper()} {qty} shares of {symbol}")

            return response

        except Exception as e:
            print(f"❌ Error placing market order for {symbol}: {e}")
            return None


    def place_bracket_order(self, symbol, qty, side, limit_price, stop_loss_price, take_profit_price):
        """
        Places a **proper bracket order** (entry + stop-loss + take-profit).
        :param symbol: Stock ticker
        :param qty: Quantity to buy/sell
        :param side: 'buy' or 'sell'
        :param limit_price: Entry price
        :param stop_loss_price: Price at which to trigger stop loss
        :param take_profit_price: Price at which to take profit
        """
        if not self.validate_trade(symbol, qty, side):
            return None

        try:
            order = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
                time_in_force=TimeInForce.GTC,
                order_class=OrderClass.BRACKET,
                limit_price=limit_price,
                stop_loss=StopLossRequest(stop_price=round(stop_loss_price,2)),
                take_profit=TakeProfitRequest(limit_price=round(take_profit_price, 2)),
            )

            response = self.client.submit_order(order)
            print(f"✅ Bracket order placed: {side.upper()} {qty} shares of {symbol} at ${limit_price}")
            return response

        except Exception as e:
            print(f"❌ Error placing bracket order for {symbol}: {e}")
            return None
    
    def place_trailing_stop_order(self, symbol, qty, side, trail_price=None, trail_percent=None):
        """
        Places a trailing stop order using Alpaca's TrailingStopOrderRequest.
        
        :param symbol: Stock ticker
        :param qty: Shares to be closed with this trailing stop
        :param side: 'buy' if the existing position is short, 'sell' if the existing position is long
                    (the trailing stop will be the opposite side of how we opened the position)
        :param trail_price: The absolute dollar offset from the high water mark
        :param trail_percent: The percent offset from the high water mark
        """
        # Choose exactly one of trail_price or trail_percent

        # For a long position, we SELL to exit.
        # For a short position, we BUY to exit.
        order_side = OrderSide.SELL if side.lower() == "buy" else OrderSide.BUY

        if not self.validate_trade(symbol, qty, side):
            return None

        try:
            order_request = TrailingStopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                type=OrderType.TRAILING_STOP,
                time_in_force=TimeInForce.GTC,
                trail_price=trail_price,       # or trail_percent, choose one
                trail_percent=trail_percent
            )

            response = self.client.submit_order(order_request)
            print(f"✅ Trailing stop order placed: {order_side.name} {qty} shares of {symbol} "
                f"with trail {'$'+str(trail_price) if trail_price else str(trail_percent)+'%'}")
            return response
        except Exception as e:
            print(f"❌ Error placing trailing stop order for {symbol}: {e}")
            return None

    def wait_for_order_fill(self, order_id, max_wait=10):
        """
        Waits for a market order to be fully filled before proceeding.

        :param order_id: The ID of the submitted market order.
        :param max_wait: Maximum seconds to wait before giving up.
        :return: Filled quantity if successful, otherwise None.
        """
        waited = 0
        while waited < max_wait:
            try:
                # Fetch latest order status using get_order_by_id
                order = self.client.get_order_by_id(order_id)

                if order.status == OrderStatus.FILLED:
                    print(f"✅ Order {order_id} filled with {order.filled_qty} shares.")
                    return float(order.filled_qty)  # Return filled quantity
                
                elif order.status in [OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                    print(f"⚠️ Order {order_id} failed with status: {order.status}.")
                    return None  # Order didn't fill
                
                print(f"⌛ Waiting for order {order_id} to fill... ({waited+1}/{max_wait} sec)")
                time.sleep(1)  # Wait 1 second before checking again
                waited += 1

            except Exception as e:
                print(f"🚨 Error fetching order status for {order_id}: {e}")
                return None  # Stop waiting if there's an API error

        print(f"⚠️ Timeout reached. Order {order_id} not fully filled.")
        return None  # Return None if it didn't fill within max wait time
    
    def get_open_orders(self):
        """
        Fetches all currently open orders.
        """
        try:
            request = GetOrdersRequest(status="open")
            open_orders = self.client.get_orders(request)
            print(f"📌 Found {len(open_orders)} open orders.")
            return open_orders

        except Exception as e:
            print(f"❌ Error fetching open orders: {e}")
            return []

    def close_position(self, symbol, percentage=100):
        """
        Fully or partially closes a position.
        :param symbol: Stock ticker
        :param percentage: Percentage of position to close (default = 100%)
        """
        try:
            if percentage == 100:
                self.client.close_position(symbol)
                print(f"✅ Closed entire position in {symbol}.")
            else:
                request = ClosePositionRequest(percentage=str(percentage))
                self.client.close_position(symbol, request)
                print(f"✅ Closed {percentage}% of position in {symbol}.")

        except Exception as e:
            print(f"❌ Error closing position in {symbol}: {e}")

    def cancel_order(self, order_id):
        """
        Cancels an order given its ID.
        :param order_id: The ID of the order to cancel
        """
        try:
            self.client.cancel_order_by_id(order_id)
            print(f"✅ Order {order_id} canceled successfully.")
        except Exception as e:
            print(f"❌ Error canceling order {order_id}: {e}")

    def cancel_all_orders(self):
        """
        Cancels all open orders.
        """
        try:
            self.client.cancel_orders()
            print("✅ All open orders have been canceled.")
        except Exception as e:
            print(f"❌ Error canceling all orders: {e}")
