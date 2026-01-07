import asyncio
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal
import json
from nats.aio.client import Client as NATS
from nats.js import api
import os

from logger_config import setup_logger
from database.insert_indicators import get_pg_pool
from NATS_setup import ensure_streams_from_yaml
import buffers.buffer_initializer as buffers
from buffers.candle_buffer import Keys
from indicators.indicator_calculator import compute_and_append_on_close
from database.insert_indicators import insert_indicators_to_db_async
from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType
from dotenv import load_dotenv
import yaml
from pathlib import Path
from entry import on_candle_closed, init_entry
from buffers.tick_registry_provider import get_tick_registry

from signals import open_signal_registry
from signals.open_signal_registry import get_open_signal_registry, OpenSignal


from database.db_general import get_pg_conn
from public_module import config_data
# from orders.order_executor import sync_broker_orders, update_account_summary  # Moved inside main to avoid circular import
from strategy.fx_correlation import refresh_correlation_cache
from indicators.atr_15m import ATR_Update, get_latest_atr_15m

from strategy.scalper import calculate_ema, calculate_rsi, is_bearish_engulfing_candle, is_bullish_engulfing_candle, manage_signal, truncate

import public_module


Candle_SUBJECT = "candles.>"   
Candle_STREAM = "STREAM_CANDLES"   

Tick_SUBJECT = "ticks.>"   
Tick_STREAM  = "STREAM_TICKS"              

Tick_DURABLE_NAME = 'tick-Scalper-01-1m'
Candle_DURABLE_NAME = 'candle-Scalper-01-1m'

async def main():

    await start_telegram_notifier()   
    

    try:

        # Try the Docker volume location first
        env_path = Path("/data/.env")
        # Fallback for local dev
        if not env_path.exists():
            env_path = Path(__file__).resolve().parent / "data" / ".env"
        load_dotenv(dotenv_path=env_path)

        DB_Conn = get_pg_conn()


        logger = setup_logger()
        logger.info("Starting QuantFlow_Fx_DecEng_1m system...")
        logger.info(
                    json.dumps({
                            "EventCode": 0,
                            "Message": f"Starting QuantFlow_Scalper_01 system..."
                        })
                )
        notify_telegram(f"❇️ QuantFlow_Scalper_01 started....", ChatType.ALERT)
        
        symbols = [str(s) for s in config_data.get("symbols", [])]
        timeframes = [str(t) for t in config_data.get("timeframes", [])]

        nc = NATS()
        await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
        await ensure_streams_from_yaml(nc, "streams.yaml")
        js = nc.jetstream()

        CANDLE_BUFFER, INDICATOR_BUFFER = buffers.init_buffers("OANDA", symbols, timeframes)
        init_entry(CANDLE_BUFFER)

        
        logger.info(
                    json.dumps({
                            "EventCode": 0,
                            "Message": f"Candle buffer initialized."
                        })
                )
        
        open_sig_registry = open_signal_registry.get_open_signal_registry()

        open_sig_registry.bootstrap_from_db(DB_Conn) 
        open_count = open_sig_registry.get_count() 
        logger.info( json.dumps({ "EventCode": 0, "Message": f"open_sig_registry initialized. open_signals={open_count}" }) )

        # --- Consumer 1: Tick Engine (receives NEW messages)
        try:
            await js.delete_consumer(Tick_STREAM, Tick_DURABLE_NAME)
        except Exception:
            pass
        
        await js.add_consumer(
            Tick_STREAM,
            api.ConsumerConfig(
                durable_name=Tick_DURABLE_NAME,
                filter_subject=Tick_SUBJECT,
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.NEW,
                ack_wait=60,  
                max_ack_pending=5000,
            )
        )

        # --- Consumer 2: Candle Engine
        try:
            await js.delete_consumer(Candle_STREAM, Candle_DURABLE_NAME)
        except Exception:
            pass    

        await js.add_consumer(
            Candle_STREAM,
            api.ConsumerConfig(
                durable_name=Candle_DURABLE_NAME,
                filter_subject=Candle_SUBJECT,
                ack_policy=api.AckPolicy.EXPLICIT,
                deliver_policy=api.DeliverPolicy.NEW, 
                ack_wait=60,
                max_ack_pending=5000,
            )
        )
       
        # Pull-based subscription for Tick Engine
        sub_Tick = await js.pull_subscribe(Tick_SUBJECT, durable=Tick_DURABLE_NAME)
    
        # Pull-based subscription for Candle Engine
        sub_Candle = await js.pull_subscribe(Candle_SUBJECT, durable=Candle_DURABLE_NAME)


        async def tick_engine_worker():
            while True:
                try:
                    msgs = await sub_Tick.fetch(100, timeout=1)
                    for msg in msgs:
                        #logger.info(f"Received from {msg.subject}")
                        
                        tick_data = json.loads(msg.data.decode("utf-8"))
                        symbol = tick_data["symbol"]
                        exchange = tick_data["exchange"]
                        if (exchange == "OANDA" and symbol in symbols):

                            raw_tick_time = tick_data.get("tick_time")
                            if raw_tick_time:
                                try:
                                    if raw_tick_time.endswith("Z"):
                                        parsed_time = datetime.fromisoformat(raw_tick_time.replace("Z", "+00:00"))
                                    else:
                                        parsed_time = datetime.fromisoformat(raw_tick_time)
                                except Exception:
                                    parsed_time = datetime.utcnow()
                            else:
                                parsed_time = datetime.utcnow()
                                
                            bid = float(tick_data["bid"])
                            ask = float(tick_data["ask"])
                            #logger.info(f"Tick for {symbol}, tick_time:{parsed_time}")   
                            tick_registry = get_tick_registry()
                            tick_registry.update_tick(exchange, symbol, bid, ask, parsed_time)

                        await msg.ack()
                except Exception as e:
                    #logger.error(
                    #        json.dumps({
                    #                "EventCode": -1,
                    #                "Message": f"NATS error: Tick, {e}"
                    #            })
                    #    )
                    #notify_telegram(f"⛔️ NATS-Error-Tick-Engine \n" + str(e), ChatType.ALERT)                    
                    await asyncio.sleep(0.05)


        async def candle_engine_worker():
            
            last_close_times = {}

            while True:
                try:
                    msgs = await sub_Candle.fetch(100, timeout=1)

                    for msg in msgs:

                        '''      
                        md = msg.metadata
                        logger.info(
                            f"JS meta: stream_seq={md.sequence.stream} "
                            f"consumer_seq={md.sequence.consumer} "
                            f"redelivered={md.num_delivered}"
                        )
                        '''

                        tokens = msg.subject.split(".")
                        timeframe = tokens[-1]
                        candle_data = json.loads(msg.data.decode("utf-8"))
                        symbol = candle_data["symbol"]
                        exchange = candle_data["exchange"]

                        #========== Main section, getting the candles we need ====================================
                        if (exchange == "OANDA" and timeframe in timeframes and symbol in symbols):
                            logger.info(f"Received from {msg.subject}: ")

                            candle_data["open_time"] = datetime.fromisoformat(candle_data["open_time"])
                            candle_data["close_time"] = datetime.fromisoformat(candle_data["close_time"])
                            candle_data["message_datetime"] = datetime.fromisoformat(candle_data["insert_ts"])
                           
                            await msg.ack()

                            key = Keys(exchange, symbol, timeframe)
                            
                            #Deduplicate the candles based on close_time
                            last_candle_close_time = last_close_times.get(key, None)
                            if last_candle_close_time is None:
                                last_close_times[key] = candle_data["close_time"]
                            elif candle_data["close_time"] == last_candle_close_time + timedelta(minutes=1):
                                last_close_times[key] = candle_data["close_time"]
                            else:
                                last_close_times[key] = candle_data["close_time"]
                                continue

                            #------------------------------------------------------------------------------------------------


                            buffers.CANDLE_BUFFER.append(key, candle_data)


                            EMA = Decimal(0.0)
                            RSI = Decimal(0.0)
                            bearish_engulfing_candle = False
                            bullish_engulfing_candle = False
                            price_to_ema = "N" # UPPER/UNDER
                            
                            #Step 1: Calculate EMA200
                            dq = buffers.CANDLE_BUFFER.get_or_create(key)
              

                            try:
                                EMA = calculate_ema(dq)
                            except Exception as e:
                                logger.error(f"Error calculating EMA for {symbol}: {e}")
                            
                            #Step 2: Calculate RSI14
                            try:
                                RSI = calculate_rsi(dq)
                            except Exception as e:
                                logger.error(f"Error calculating RSI for {symbol}: {e}")

                            #Step 3: Check for Engulfing Candle
                            try:
                                bearish_engulfing_candle = is_bearish_engulfing_candle(dq)
                                bullish_engulfing_candle = is_bullish_engulfing_candle(dq)
                            except Exception as e:
                                logger.error(f"Error checking Engulfing Candle for {symbol}: {e}")

                            #Step 4: Check if Close price is under EMA200
                            if EMA is not None:
                                if candle_data["close"] < EMA:
                                    price_to_ema = "UNDER"
                                elif candle_data["close"] > EMA:
                                    price_to_ema = "UPPER"

                            logger.info(f"{symbol}, EMA = {EMA}, RSI = {RSI}, event_time = {candle_data['close_time'].strftime('%Y-%m-%d %H:%M')}, Price_to_EMA = {price_to_ema}, bearish_eng. = {bearish_engulfing_candle}, bullish_eng. = {bullish_engulfing_candle}")

                            got_signal = False
                            side = ""
                            
                            if price_to_ema == "UPPER" and RSI is not None and RSI > public_module.RSI_UPPER_THRESHOLD and bullish_engulfing_candle == True:
                            #if Decimal(str(candle_data["close"])) > Decimal(str(candle_data["open"])):
                                try:
                                    close_dec = Decimal(str(candle_data["close"]))
                                    low_dec = Decimal(str(candle_data["low"]))
                                    sl_price = low_dec.quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
                                    risk = close_dec - low_dec
                                    target_price = (close_dec + risk * Decimal("2")).quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
                                    position_price = close_dec.quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
                                    side = "buy"    
                                    got_signal = True
                                except Exception as e:
                                    logger.error(f"Error preparing BUY signal for {symbol}: {e}")

                            if price_to_ema == "UNDER" and RSI is not None and RSI < public_module.RSI_LOWER_THRESHOLD and bearish_engulfing_candle == True:
                            #if Decimal(str(candle_data["close"])) < Decimal(str(candle_data["open"])):
                                try:
                                    close_dec = Decimal(str(candle_data["close"]))
                                    high_dec = Decimal(str(candle_data["high"]))
                                    sl_price = high_dec.quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
                                    risk = high_dec - close_dec
                                    target_price = (close_dec - risk * Decimal("2")).quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
                                    position_price = close_dec.quantize(Decimal("0.00001"), rounding=ROUND_DOWN)
                                    side = "sell"
                                    got_signal = True
                                except Exception as e:
                                    logger.error(f"Error preparing SELL signal for {symbol}: {e}")
                            
                            if got_signal == True:
                                logger.info(f"Placing {side.upper()} order for {symbol}, EMA = {EMA}, RSI = {RSI}, target_price={target_price}, sl_price={sl_price}, event_time = {candle_data['close_time'].strftime('%Y-%m-%d %H:%M')}")
                                manage_signal(exchange=exchange, timeframe=timeframe, symbol=symbol, side=side, target_price=target_price, sl_price=sl_price, event_time=candle_data["close_time"], position_price=position_price)

                            
                           
                       
                except Exception as e:
                    await asyncio.sleep(0.05)

         
        logger.info(
                json.dumps({
                            "EventCode": 0,
                            "Message": f"Subscriber QuantFlow_Scalper_01 starts...."
                        })
                )
    
        await asyncio.gather(tick_engine_worker(), candle_engine_worker())

    finally:
        notify_telegram(f"⛔️ QuantFlow_Scalper_01 App stopped.", ChatType.ALERT)
        await close_telegram_notifier()

    
if __name__ == "__main__":
    asyncio.run(main())
