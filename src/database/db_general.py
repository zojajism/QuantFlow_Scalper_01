import os
import decimal
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import psycopg
from psycopg.rows import tuple_row  # default row factory; returns tuples

from buffers.candle_buffer import Keys

load_dotenv()

# ----------------- Client -----------------
def get_pg_conn() -> psycopg.Connection:
    """
    Open a new PostgreSQL connection using environment variables.
    """
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        autocommit=True,
        row_factory=tuple_row,  # rows as tuples (like ClickHouse client)
    )

def get_pg_conn_candle() -> psycopg.Connection:
    """
    Open a new PostgreSQL connection using environment variables.
    """
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB_CANDLE"),
        autocommit=True,
        row_factory=tuple_row,  # rows as tuples (like ClickHouse client)
    )

# ----------------- Queries -----------------
def get_last_timestamp(
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[Any]:
    """
    Returns the MAX(open_time) from candles or None if empty.
    """
    sql = """
        SELECT MAX(open_time) AS last_candle_open_time
        FROM candles
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe))
        row = cur.fetchone()
        # row[0] will be None if no rows
        return row[0] if row else None

def get_candles_from_db(key: Keys, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch latest `limit` candles in DESC order, then return ASC list of dicts.
    """
    sql = """
        SELECT
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume
        FROM candles
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
        ORDER BY open_time DESC
        LIMIT %s
    """

    with get_pg_conn_candle() as conn, conn.cursor() as cur:
        cur.execute(sql, (key.exchange, key.symbol, key.timeframe, limit))
        rows = cur.fetchall()
    
    # Convert to list of dicts in ASC order
    candles = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "open_time": row[0],
            "close_time": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in reversed(rows)
    ]
    return candles

def get_indicators_from_db(key: Keys, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch latest `limit` indicators in DESC order, then return ASC list of dicts.
    """
    sql = """
        SELECT
            timestamp,
            indicator,
            value
        FROM indicators
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
        ORDER BY timestamp DESC
        LIMIT %s
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (key.exchange, key.symbol, key.timeframe, limit))
        rows = cur.fetchall()

    indicators = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "timestamp": row[0],
            "indicator": row[1],
            "value": row[2],
        }
        for row in reversed(rows)
    ]
    return indicators

def get_last_bios_from_db(exchange: str, symbol: str, timeframe: str) -> str:
    sql = """
            select bios from bios_signal bs 
            where 
                exchange = %s
                and symbol = %s
                and timeframe = %s
            order by "timestamp" desc
            limit 1
        """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe))
        rows = cur.fetchall()

    for row in reversed(rows):
      return row[0]


def fetch_sl_config() -> List[Dict[str, Any]]:
    """
    Returns list of dicts with the config data.
    """
    sql = """
        SELECT id, distance_percentage, sl_percentage
        FROM sl_config
        ORDER BY distance_percentage ASC
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    
    configs = []
    for row in rows:
        configs.append({
            "id": row[0],
            "distance_percentage": float(row[1]),
            "sl_percentage": float(row[2])
        })
    return configs


def get_ema_states_from_db(exchange: str, symbol: str, timeframe: str, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch latest `limit` ema_states in DESC order, then return ASC list of dicts.
    """
    sql = """
        SELECT
            exchange,
            symbol,
            timeframe,
            close_time,
            ema_slow,
            ema_fast,
            bios,
            trend,
            flipped,
            candle_open,
            candle_close,
            ema_distance
        FROM ema_state
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
        ORDER BY close_time DESC
        LIMIT %s
    """

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe, limit))
        rows = cur.fetchall()
    
    # Convert to list of dicts in ASC order
    ema_states_list = [
        {
            "exchange": row[0],
            "symbol": row[1],
            "timeframe": row[2],
            "close_time": row[3],
            "ema_slow": decimal.Decimal(row[4]) if row[4] is not None else None,
            "ema_fast": decimal.Decimal(row[5]) if row[5] is not None else None,
            "bios": row[6],
            "trend": row[7],
            "flipped": row[8],
            "candle_open": row[9],
            "candle_close": row[10],
            "ema_distance": decimal.Decimal(row[11]) if row[11] is not None else None,
        }
        for row in reversed(rows)
    ]
    return ema_states_list


async def insert_ema_state_to_db(exchange: str, symbol: str, timeframe: str, close_time, ema_slow, ema_fast, bios: int, trend: str, flipped: bool, candle_open: str, candle_close: str, ema_distance: decimal) -> None:
    """
    Insert an EMAState record into the ema_state table.
    """
    sql = """
    INSERT INTO ema_state (exchange, symbol, timeframe, close_time, ema_slow, ema_fast, bios, trend, flipped, candle_open, candle_close, ema_distance)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe, close_time, ema_slow, ema_fast, bios, trend, flipped, candle_open, candle_close, ema_distance))
