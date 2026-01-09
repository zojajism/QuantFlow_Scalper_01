
from typing import Any, Dict, List, Optional, Tuple

from datetime import datetime
from decimal import Decimal
import logging
import math
from typing import Deque, Dict, Any, List, Optional

import requests
import psycopg
import public_module
from decimal import Decimal, ROUND_DOWN
from database.db_general import get_pg_conn
from orders.order_executor import send_market_order, sync_broker_orders
from telegram_notifier import notify_telegram, ChatType
from signals.open_signal_registry import get_open_signal_registry, OpenSignal
from buffers.tick_registry_provider import get_tick_registry
# Access CandleBuffer to read prices
from buffers import buffer_initializer as buffers
from buffers.candle_buffer import Keys

MAX_TICK_AGE_SEC = 10  # if last tick older than this (in seconds), fallback to candle close


logger = logging.getLogger(__name__)
fmt = lambda v, n: "N/A" if v is None else truncate(v, n)


def truncate(value: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.trunc(value * factor) / factor

def _insert_signals(conn: psycopg.Connection, rows: List[tuple]) -> None:
    """
    Insert generated signals in one batch.
    """
    sql = """
        INSERT INTO signals (
            event_time,
            signal_symbol,
            confirm_symbols,
            position_type,
            price_source,
            position_price,
            target_pips,
            target_price,
            ref_symbol,
            ref_type,
            pivot_time,
            found_at,
            reject_reason,
            spread,
            sl_pips,
            sl_price
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        logger.error("[DB ERROR] insert signals failed:", e)


def _update_signals_with_orders(
    conn: psycopg.Connection,
    rows: List[tuple],
) -> None:
    """
    Batch update for order-related columns in signals table.
    """
    if not rows:
        return

    sql = """
        UPDATE signals
           SET order_env        = %s,
               order_sent       = %s,
               order_sent_time  = %s,
               broker_order_id  = %s,
               broker_trade_id  = %s,
               allocation_block = %s,
               order_units      = %s,
               actual_entry_time  = %s,
               actual_entry_price = %s,
               actual_tp_price    = %s,
               actual_target_pips = %s,
               actual_exit_time   = %s,
               actual_exit_price  = %s,
               order_status       = %s,
               slippage_pips      = %s,
               profit_pips        = %s,
               profit_ccy         = %s,
               exec_latency_ms    = %s
         WHERE signal_symbol = %s
           AND position_type = %s
           AND event_time    = %s
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def calculate_ema(
    candles: Deque[Dict[str, Any]],
    price_key: str = "close",
) -> Optional[Decimal]:
    
    period = public_module.EMA_LENGTH
    
    if len(candles) < period:
        return None

    # k = 2 / (period + 1) as Decimal
    k = Decimal(2) / Decimal(period + 1)
    one_minus_k = Decimal(1) - k

    first = list(candles)[:period]

    # Ensure prices are Decimal (in case some are int/float/str)
    ema = sum(Decimal(c[price_key]) for c in first) / Decimal(period)

    for c in list(candles)[period:]:
        price = Decimal(c[price_key])
        ema = price * k + ema * one_minus_k
        
    ema = round(ema, 5)
    
    return ema

def calculate_atr(
    candles: Deque[Dict[str, Any]],
    period: int = 14
) -> Optional[Decimal]:
    """
    Calculate the Average True Range (ATR) for a given candle series.
    :param candles: Deque of dicts with 'high', 'low', 'close' keys.
    :param period: ATR period (default 14)
    :return: ATR value or None if not enough data
    """
    if len(candles) < period + 1:
        return None

    candles_list = list(candles)[- (period + 1):]  # Get the most recent period+1 candles
    tr_list = []
    for i in range(1, len(candles_list)):
        high = Decimal(candles_list[i]["high"])
        low = Decimal(candles_list[i]["low"])
        prev_close = Decimal(candles_list[i - 1]["close"])
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        tr_list.append(tr)

    atr = sum(tr_list) / Decimal(period)
    return round(atr, 5)

def calculate_rsi(
    candles: Deque[Dict[str, Any]],
    price_key: str = "close",
) -> Optional[Decimal]:
    """
    Compute RSI (Wilder's method) from close prices.

    Returns:
        RSI value (0-100) or None if not enough data
    """
    period = public_module.RSI_LENGTH
    
    if len(candles) < period + 1:
        return None

    closes = [Decimal(c[price_key]) for c in candles]

    gains = []
    losses = []

    # Step 1: initial gains/losses
    for i in range(1, period + 1):
        delta = closes[i] - closes[i - 1]
        if delta >= 0:
            gains.append(delta)
            losses.append(Decimal(0))
        else:
            gains.append(Decimal(0))
            losses.append(abs(delta))

    avg_gain = sum(gains) / Decimal(period)
    avg_loss = sum(losses) / Decimal(period)

    # Step 2: Wilder smoothing
    for i in range(period + 1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gain = max(delta, Decimal(0))
        loss = max(-delta, Decimal(0))

        avg_gain = (avg_gain * (Decimal(period) - Decimal(1)) + gain) / Decimal(period)
        avg_loss = (avg_loss * (Decimal(period) - Decimal(1)) + loss) / Decimal(period)

    if avg_loss == 0:
        return Decimal(100)

    rs = avg_gain / avg_loss
    rsi = Decimal(100) - (Decimal(100) / (Decimal(1) + rs))

    return round(rsi, 5)

def calc_price_to_ema(EMA: Decimal, ATR: Decimal, open: Decimal, close: Decimal) -> str: 
    
    price_to_ema = "N/A"

    if EMA is not None and ATR is not None:
        if close < EMA and open < EMA and (EMA - open) > (Decimal(ATR) * Decimal(public_module.PRICE_DISTANCE_TO_EMA)):
            price_to_ema = "UNDER"
        elif close > EMA and open > EMA and (open - EMA) > (Decimal(ATR) * Decimal(public_module.PRICE_DISTANCE_TO_EMA)):
            price_to_ema = "UPPER"

    return price_to_ema

def _to_oanda_instrument(symbol: str) -> str:
    """
    Convert internal symbol like 'EUR/USD' to OANDA instrument 'EUR_USD'.
    """
    return symbol.replace("/", "_")

def is_bullish_engulfing_candle(candles: Deque[Dict[str, Any]], ATR: Decimal) -> bool:
    if len(candles) < 2:
        return False

    current = candles[-1]
    previous = candles[-2]

    prev_open = Decimal(previous["open"])
    prev_close = Decimal(previous["close"])
    curr_open = Decimal(current["open"])
    curr_close = Decimal(current["close"])

    # Previous candle should be bearish
    if prev_close >= prev_open:
        return False

    # Current candle should be bullish
    if curr_close <= curr_open:
        return False

    # Current body should cover the entire previous body
    if curr_open > prev_close or curr_close < prev_open:
        return False

    if abs(curr_close - curr_open) < (Decimal(ATR) * Decimal(public_module.ENGULFING_MIN_BODY_SIZE)):
        return False
    
    return True

def is_bearish_engulfing_candle(candles: Deque[Dict[str, Any]], ATR: Decimal) -> bool:
    if len(candles) < 2:
        return False

    current = candles[-1]
    previous = candles[-2]

    prev_open = Decimal(previous["open"])
    prev_close = Decimal(previous["close"])
    curr_open = Decimal(current["open"])
    curr_close = Decimal(current["close"])

    # Previous candle should be bullish
    if prev_close <= prev_open:
        return False

    # Current candle should be bearish
    if curr_close >= curr_open:
        return False

    # Current body should cover the entire previous body
    if curr_open < prev_close or curr_close > prev_open:
        return False

    if abs(curr_close - curr_open) < (Decimal(ATR) * Decimal(public_module.ENGULFING_MIN_BODY_SIZE)):
        return False
    
    return True


def manage_signal(
                    exchange: str,
                    timeframe: str,
                    symbol: str, 
                    side: str, 
                    event_time:datetime,
                    close:Decimal,
                    high:Decimal,
                    low:Decimal,
                    EMA: Decimal, # for print log only
                    RSI: Decimal, # for print log only
                    ):
    try:
        tick_registry = get_tick_registry()
        position_price_Tick, price_source, spread = _get_entry_price_with_tick_fallback(
                            tick_registry=tick_registry,
                            exchange=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            side=side,
                            event_time=event_time,
                        )
        
        if side == "buy":
            close_dec = close
            low_dec = low
            risk = close_dec - low_dec
            position_price = position_price_Tick
            sl_price = position_price - risk.quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
            target_price = position_price + ( risk * Decimal("1.5").quantize(Decimal("0.00001"), rounding=ROUND_DOWN) )

        if side == "sell":
            close_dec = close
            high_dec = high
            risk = high_dec - close_dec
            position_price = position_price_Tick
            sl_price = position_price + risk.quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
            target_price = position_price - ( risk * Decimal("1.5").quantize(Decimal("0.00001"), rounding=ROUND_DOWN) )

        logger.info(f"Placing {side.upper()} order for {symbol}, EMA = {EMA}, RSI = {RSI}, target_price={target_price}, sl_price={sl_price}, event_time = {event_time.strftime('%Y-%m-%d %H:%M')}")

    except Exception as e:
        logger.error(f"[ERROR] manage_signal failed to get entry price: {e}")
        position_price = Decimal(position_price)

    open_sig_registry = get_open_signal_registry()

    # Ensure all values are Decimal for consistent calculations
    position_price = Decimal(position_price)
    pip_size = Decimal(public_module._pip_size(symbol))


    try:
        target_pips = (target_price - position_price) / pip_size
        target_pips = Decimal(target_pips)

        if symbol == "USD/JPY":
            profit_jpy = (Decimal(public_module.ORDER_UNITS) * target_pips) / Decimal("100")
            profit_est = profit_jpy / position_price
        else:
            profit_est = (Decimal(public_module.ORDER_UNITS) * target_pips) / Decimal("10000")
    except Exception as e:
        logger.error(f"[ERROR] manage_signal pip/profit calc failed: {e}")
    
    profit_est = truncate(profit_est, 2)

    order_units = public_module.ORDER_UNITS

    order_info = None

    '''
    try:
        instrument = _to_oanda_instrument(symbol)
        client_order_id = (
            f"qf-{event_time.strftime('%Y%m%d%H%M%S')}-"
            f"{instrument.replace('_', '')}"
        )

        logger.info(
                    f"[ORDER] Sending market order: instrument={instrument}, side={side}, units={order_units}, "
                    f"tp_price={target_price}, sl_price={sl_price}"
                )

        order_info = send_market_order(
            symbol=instrument,
            side=side,
            units=order_units,
            tp_price=target_price,
            sl_price=sl_price,
            client_order_id=client_order_id,
        )
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = None
        if e.response is not None:
            try:
                body = e.response.json()
            except Exception:
                body = e.response.text
                notify_telegram(body, ChatType.ALERT)

        print(f"[ORDER] HTTP error {status} from OANDA: {body}")
        order_info = None


    if order_info is None:
        print(
            f"[ORDER] send_market_order returned None for {symbol} "
            f"(side={side}, tp_price={target_price})"
        )
    else:
        cancel = None
        raw = getattr(order_info, "raw_response", None)
        if isinstance(raw, dict):
            cancel = raw.get("orderCancelTransaction")

        if cancel:
            reason = cancel.get("reason")
            print(f"[ORDER] Broker cancelled order. reason={reason}")

            # Optional telegram for cancel
            try:

                msg = (
                    "🆑 Order cancelled\n"
                    f"Reason:         {reason}\n\n"
                    f"Symbol:         {symbol}\n"
                    f"Side:           {side.upper()}\n"
                    f"Entry price:    {truncate(position_price,5)}\n"
                    f"Target price:   {truncate(target_price,5)}\n"
                    f"Distance:       {truncate(target_pips,2)} pips\n"
                    f"Spread:         {truncate(spread,1)}\n"
                    f"SL_Price:       {fmt(sl_price,5)}\n"
                    f"Est. Profit:    ${truncate(profit_est,2)}\n\n"
                    f"Event time:      {event_time.strftime('%Y-%m-%d %H:%M')}\n\n"
                )
                notify_telegram(msg, ChatType.INFO)
            except Exception as te:
                print(f"[WARN] telegram cancel notify failed: {te}")
        
    '''

    # ------------------------------------------------
    # 8) Register OpenSignal (with optional order info)
    # ------------------------------------------------
    open_sig_registry.add_signal(
        OpenSignal(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            side=side,
            event_time=event_time,
            target_price=target_price,
            position_price=position_price,
            created_at=event_time,
            order_env=(order_info.env if order_info else None),
            broker_order_id=(
                order_info.broker_order_id if order_info else None
            ),
            broker_trade_id=(
                order_info.broker_trade_id if order_info else None
            ),
            order_units=(
                order_info.units if order_info else None
            ),
            actual_entry_time=(
                order_info.actual_entry_time if order_info else None
            ),
            actual_entry_price=(
                order_info.actual_entry_price if order_info else None
            ),
            actual_tp_price=(
                order_info.actual_tp_price if order_info else None
            ),
            order_status=(
                order_info.status if order_info else "none"
            ),
            exec_latency_ms=(
                order_info.exec_latency_ms if order_info else None
            ),
        )
    )

    # Telegram notification ONLY for broker-eligible & not-blocked signals
    try:

        msg = (
            "⚡ Pivot Correlation Signal\n"
            f"Symbol:         {symbol}\n"
            f"Side:           {side.upper()}\n"
            f"Entry price:     {truncate(position_price,5)}\n"
            f"Target price:   {truncate(target_price,5)}\n"
            f"Distance:       {truncate(target_pips,2)}\n"
            f"Spread:         {truncate(spread,2)}\n"
            f"SL_Price:       {fmt(sl_price,5)}\n"
            f"Est. Profit:    ${truncate(profit_est,2)}\n\n"
            f"Event time:     {event_time.strftime('%Y-%m-%d %H:%M')}\n\n"
        )
        notify_telegram(msg, ChatType.INFO)
    except Exception as e:
        logger.error(f"[WARN] telegram notify failed: {e}")

  
    # ------------------------------------------------
    # 10) Queue row for signals table INSERT (adjusted pips/price)
    # ------------------------------------------------
    conn = get_pg_conn()
    if conn is not None:
        _insert_signals(conn, [(
            event_time,             # event_time (trigger)
            symbol,                 # signal_symbol
            "",                    # confirm_symbols
            side,                   # position_type
            "",                    # price_source
            position_price,         # position_price
            abs(target_pips),            # target_pips (ADJUSTED, magnitude)
            target_price,           # target_price (ADJUSTED)
            "",                    # ref_symbol (context)
            "",                    # ref_type (context)
            event_time,             # pivot_time (ref anchor)
            event_time,             # found_at (target pivot time)
            "",                    # reject_reason
            spread,                   # spread
            None,                   # sl_pips
            sl_price,               # sl_price
        )])

  # ------------------------------------------------
    # 9) Queue DB updates for order-related columns
    # ------------------------------------------------
    '''
    try:
        if order_info is not None:
            conn = get_pg_conn()
            if conn is not None:
                _update_signals_with_orders(conn, [(
                    order_info.env,               # order_env
                    True,                         # order_sent
                    order_info.order_sent_time,   # order_sent_time
                    order_info.broker_order_id,   # broker_order_id
                    order_info.broker_trade_id,   # broker_trade_id
                    None,                         # allocation_block (later)
                    order_info.units,             # order_units
                    order_info.actual_entry_time, # actual_entry_time
                    order_info.actual_entry_price,# actual_entry_price
                    order_info.actual_tp_price,   # actual_tp_price
                    target_pips,                  # actual_target_pips
                    None,                         # actual_exit_time
                    None,                         # actual_exit_price
                    order_info.status,            # order_status
                    None,                         # slippage_pips
                    None,                         # profit_pips
                    None,                         # profit_ccy
                    order_info.exec_latency_ms,   # exec_latency_ms
                    symbol,                       # signal_symbol
                    side,                         # position_type
                    event_time,                   # event_time
                )])
    except Exception as e:
        print(f"[DB ERROR] update_signals_with_orders failed: {e}")

    '''

    #sync_broker_orders(conn)
    

def _get_entry_price_with_tick_fallback(
        *,
        tick_registry,
        exchange: str,
        symbol: str,
        timeframe: str,
        side: str,
        event_time: datetime,
    ) -> Tuple[Optional[Decimal], str]:
        """
        Determine entry price for a signal:

        1) Try to use the latest Tick from TickRegistry:
            - If a tick exists and abs(age_sec) <= MAX_TICK_AGE_SEC:
                BUY  -> ASK
                SELL -> BID
        2) Otherwise, fallback to the latest candle close from CandleBuffer.

        Returns:
        (price, source) where source in {"tick", "candle_close", "none"}.
        """

        #if we found the real spread based on the tick data, we will use it, otherwise, we consider it as 2 pip
        Spread = 2

        price_source = "none"
        try:
            tick = tick_registry.get_last_tick(exchange, symbol)
        except Exception:
            tick = None
            logger.error(f"[WARN] could not get tick for {symbol} from TickRegistry")

        if tick is not None and isinstance(event_time, datetime):
            try:
                age_sec = abs((tick.time - event_time).total_seconds())
            except Exception:
                age_sec = None
                logger.error(f"[WARN] could not compute tick age for {symbol}")

            if age_sec is not None and age_sec <= MAX_TICK_AGE_SEC:
                side_upper = side.upper()
                if side_upper == "BUY":
                    px = Decimal(str(tick.ask))
                else:  # SELL
                    px = Decimal(str(tick.bid))
                price_source = "tick"
            
                Spread = Decimal(str(tick.ask)) - Decimal(str(tick.bid))
                if symbol == "USD/JPY":
                    Spread = Spread * 100
                else:
                    Spread = Spread * 10000

                return px, price_source, Spread

        # Fallback to latest candle close
        close_px = _get_latest_close_price(exchange, symbol, timeframe)
        if close_px is not None:
            price_source = "candle_close"
            return close_px, price_source, Spread

        return None, price_source, Spread


def _get_latest_close_price(
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[Decimal]:
    """
    Get the latest (most recent) close price for a symbol/timeframe
    from CandleBuffer.
    """
    try:
        cb = getattr(buffers, "CANDLE_BUFFER", None)
        if cb is None:
            return None

        key = Keys(exchange, symbol, timeframe)
        candles = cb.last_n(key, 1)
        if not candles:
            return None

        c = candles[0]

        for attr in ("Close", "close", "c", "ClosePrice", "C"):
            if isinstance(c, dict) and attr in c:
                return Decimal(str(c[attr]))

        return None
    except Exception:
        return None