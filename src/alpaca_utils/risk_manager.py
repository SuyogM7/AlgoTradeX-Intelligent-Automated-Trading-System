import pandas as pd
import numpy as np

class RiskManager:
    def __init__(
        self,
        risk_per_trade=0.01,        # e.g., 1% of equity at risk
        atr_period=14,
        atr_multiplier=1,
        risk_reward_ratio=2,
        max_position_fraction=0.01,  # e.g., 1% of buying power per trade
        max_open_positions=10,       # Max concurrent positions
        max_notional_ratio=0.50      # Max 50% of buying power in positions
    ):
        """
        :param risk_per_trade: Fraction of equity to risk per trade (default = 0.01).
        :param atr_period: ATR calculation period (Wilder's smoothing).
        :param atr_multiplier: Multiplier for stop loss.
        :param risk_reward_ratio: Take-profit ratio relative to risk.
        :param max_position_fraction: Fraction of buying power allocated to one trade.
        :param max_open_positions: Maximum allowed open positions.
        :param max_notional_ratio: Maximum % of buying power allocated to total positions.
        """
        self.risk_per_trade = risk_per_trade
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.risk_reward_ratio = risk_reward_ratio
        self.max_position_fraction = max_position_fraction
        self.max_open_positions = max_open_positions
        self.max_notional_ratio = max_notional_ratio

    def compute_atr(self, df: pd.DataFrame) -> float:
        """Computes the Average True Range (ATR) using Wilder's smoothing."""
        df = df.copy()
        df["previous_close"] = df["close"].shift(1)
        df["true_range"] = df[["high", "low", "previous_close"]].apply(
            lambda row: max(row["high"] - row["low"], abs(row["high"] - row["previous_close"]), abs(row["low"] - row["previous_close"])), axis=1
        )
        df["atr"] = df["true_range"].ewm(alpha=1/self.atr_period, adjust=False).mean()
        return df["atr"].iloc[-1]

    def validate_portfolio_risk(self, account_info: dict, open_positions: list, proposed_trade_value: float):
        """
        Checks portfolio-level constraints:
        - Max number of open positions.
        - Max notional exposure (50% of buying power, configurable).
        """
        # Check max open positions
        if len(open_positions) >= self.max_open_positions:
            print(f"🚫 Skipping trade: Already at max open positions ({self.max_open_positions}).")
            return False

        # Check total notional risk
        total_notional_open = sum(float(pos["market_value"]) for pos in open_positions)
        new_notional = total_notional_open + proposed_trade_value
        max_notional_allowed = account_info["buying_power"] * self.max_notional_ratio

        if new_notional > max_notional_allowed:
            print(f"🚫 Skipping trade: Total notional {new_notional:.2f} exceeds {self.max_notional_ratio*100}% of buying power.")
            return False

        return True  # Trade is valid

    def calculate_trade_parameters(
        self,
        df: pd.DataFrame,
        entry_price: float,
        account_info: dict,
        open_positions: list,
        side: str = "BUY"
    ):
        """
        1. Computes an ATR-based stop loss and take profit.
        2. Calculates risk-based share count (risk_per_trade of equity).
        3. Caps by max_position_fraction * available funds.
        4. Ensures affordability for BUY trades (uses 'cash').
        5. Skips the trade if quantity <= 0 or portfolio constraints are violated.

        :param df: DataFrame with 'high','low','close' columns.
        :param entry_price: Entry price for the trade.
        :param account_info: Account details (equity, cash, buying power).
        :param open_positions: List of current open positions.
        :param side: 'BUY' or 'SELL'.
        :return: dict with keys: 'quantity', 'stop_loss', 'take_profit', 'atr'.
        """
        # Compute ATR
        atr = self.compute_atr(df)
        if np.isnan(atr) or atr < 1e-5:
            print("🚫 Skipping trade: ATR too low (market stagnant).")
            return {"quantity": 0, "stop_loss": None, "take_profit": None, "atr": round(atr, 4)}
        
        trail_amount = atr * self.atr_multiplier

        # Determine Stop Loss & Take Profit
        if side.upper() == "BUY":
            stop_loss = max(entry_price - (atr * self.atr_multiplier), 0.01)  # Ensure SL is valid
            risk_per_share = entry_price - stop_loss
            take_profit = entry_price + (self.risk_reward_ratio * risk_per_share)
        elif side.upper() == "SELL":
            stop_loss = entry_price + (atr * self.atr_multiplier)
            risk_per_share = stop_loss - entry_price
            take_profit = entry_price - (self.risk_reward_ratio * risk_per_share)
        else:
            print("🚫 Invalid trade side.")
            return {"quantity": 0}

        # Compute risk-based quantity
        risk_amount = account_info["equity"] * self.risk_per_trade
        quantity_risk_based = int(risk_amount // risk_per_share)

        # Determine Available Funds (Cash for BUY, Buying Power for SELL)
        available_funds = account_info["cash"] if side.upper() == "BUY" else account_info["buying_power"]
        max_notional = available_funds * self.max_position_fraction
        quantity_notional_cap = int(max_notional // entry_price)

        # Ensure BUY trades only use cash
        quantity_cash_cap = int(account_info["cash"] // entry_price) if side.upper() == "BUY" else 999999

        # Final quantity decision
        final_quantity = min(quantity_risk_based, quantity_notional_cap, quantity_cash_cap)

        # Validate portfolio-level risk
        proposed_trade_value = entry_price * final_quantity
        if not self.validate_portfolio_risk(account_info, open_positions, proposed_trade_value):
            return {"quantity": 0, "stop_loss": None, "take_profit": None, "atr": round(atr, 4)}

        return {
            "quantity": final_quantity,
            "stop_loss": round(stop_loss, 2),
            "take_profit": round(take_profit, 2),
            "atr": round(atr, 4),
            "trail_amount": round(trail_amount, 2)
        }