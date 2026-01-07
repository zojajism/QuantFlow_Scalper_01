# file: src/orders/sl_manager.py
# English-only comments

from __future__ import annotations

from decimal import Decimal
from typing import List, Dict, Any, Optional
import logging

from database.db_general import fetch_sl_config

logger = logging.getLogger(__name__)


class SLManager:
    """
    Manages Stop Loss configurations for trailing SL based on price movement towards TP.
    
    Config is loaded from DB with levels defined by distance_percentage and sl_percentage.
    """
    
    def __init__(self) -> None:
        self.configs: List[Dict[str, Any]] = []
    
    def load_configs(self) -> None:
        """
        Load SL configurations from the database.
        """
        try:
            self.configs = fetch_sl_config()
            logger.info(f"Loaded {len(self.configs)} SL config records")
        except Exception as e:
            logger.error(f"Failed to load SL configs: {e}")
            self.configs = []
    
    def get_trailing_sl_price(
        self,
        entry_price: Decimal,
        tp_price: Decimal,
        order_side: str,  # "buy" or "sell"
        current_price: Decimal,
        symbol: str = "",
        broker_trade_id: str = ""
    ) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Calculate the trailing SL price based on current price position.
        
        For BUY orders: SL trails as price moves above entry towards TP.
        For SELL orders: SL trails as price moves below entry towards TP.
        
        Returns the SL price if a matching level is found, None otherwise.
        """
        if not self.configs:
            logger.warning("No SL configs loaded")
            return None, None
        
        # Calculate total distance from entry to TP
        if order_side.lower() == "buy":
            if current_price <= entry_price:
                # Price hasn't moved in profit direction yet
                return None, None
            total_distance = tp_price - entry_price
            current_distance = current_price - entry_price
        elif order_side.lower() == "sell":
            if current_price >= entry_price:
                # Price hasn't moved in profit direction yet
                return None, None
            total_distance = entry_price - tp_price
            current_distance = entry_price - current_price
        else:
            logger.error(f"Invalid order_side: {order_side}")
            return None, None
        
        if total_distance <= 0:
            logger.warning("Invalid TP price relative to entry")
            return None, None
        
        # Calculate percentage of distance covered
        distance_pct = (current_distance / total_distance) * 100
        
        # Find the appropriate SL level
        # Use the highest level where distance_pct >= config distance_percentage
        applicable_config = None
        for config in sorted(self.configs, key=lambda x: x["distance_percentage"], reverse=True):
            if distance_pct >= config["distance_percentage"]:
                applicable_config = config
                break
        
        if not applicable_config:
            # No level reached yet
            return None, None
        
        # Calculate SL price
        sl_pct = Decimal(str(applicable_config["sl_percentage"])) / Decimal('100')
        if order_side.lower() == "buy":
            sl_price = entry_price + (total_distance * sl_pct)
        else:  # sell
            sl_price = entry_price - (total_distance * sl_pct)
        
        # Round to 5 decimal places for forex precision
        sl_price = sl_price.quantize(Decimal('0.00001'))
        
        logger.info(
            f"symbol: {symbol}, trade_id: {broker_trade_id}, "
            f"SL calc: side={order_side}, dist_pct={distance_pct:.2f}, "
            f"level={applicable_config['distance_percentage']}%, "
            f"sl_pct={applicable_config['sl_percentage']}%, sl_price={sl_price}"
        )
        return sl_price, applicable_config['sl_percentage']


# Global instance
_sl_manager: Optional[SLManager] = None


def get_sl_manager() -> SLManager:
    """
    Get the global SL manager instance, creating it if needed.
    """
    global _sl_manager
    if _sl_manager is None:
        _sl_manager = SLManager()
    return _sl_manager


def load_sl_configs() -> None:
    """
    Load SL configurations into the global manager.
    """
    manager = get_sl_manager()
    manager.load_configs()


def calculate_trailing_sl(
    entry_price: Decimal,
    tp_price: Decimal,
    order_side: str,
    current_price: Decimal,
    symbol: str = "",
    broker_trade_id: str = ""
) -> tuple[Optional[Decimal]:, Optional[Decimal]]:
    """
    Convenience function to calculate trailing SL using the global manager.
    """
    manager = get_sl_manager()
    return manager.get_trailing_sl_price(entry_price, tp_price, order_side, current_price, symbol, broker_trade_id)