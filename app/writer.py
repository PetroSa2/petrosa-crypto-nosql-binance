import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Iterable

from petrosa.observability import opentel
from opentelemetry.metrics import CallbackOptions, Observation
from petrosa.database import mongo
import os

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "no-service-name")

MAX_WORKERS = 10

class PETROSAWriter(object):
    def __init__(self):
        self.queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

        self.client_mg = mongo.get_client()
        self.mongo_update_obs = 0
        
        opentel.meter.create_observable_gauge(
            opentel.SERVICE_NAME + ".write.mongo.update.single.doc",
            callbacks=[self.send_mongo_update_obs],
            description="Time updating mongo single doc",
        )
        
        opentel.meter.create_observable_gauge(
            opentel.SERVICE_NAME + ".write.gauge.queue.size",
            callbacks=[self.send_queue_size],
            description="Size of the queue for writing",
        )

        threading.Thread(target=self.update_forever).start()

    @opentel.tracer.start_as_current_span(name=f"{SERVICE_NAME}.PETROSAWriter.get_msg")
    def get_msg(self, table, msg):
        # print("msg on get_msg", msg)
        try:
            if(msg['x'] is True):
                candle = {}
                candle['datetime'] = datetime.fromtimestamp(msg['t']/1000.0)
                candle['timestamp'] = int(time.time() * 1000)
                candle['ticker'] = msg['s']
                candle['open'] = float(msg['o'])
                candle['high'] = float(msg['h'])
                candle['low'] = float(msg['l'])
                candle['close'] = float(msg['c'])
                candle['closed_candle'] = msg['x']

                if('T' in msg):
                    candle['close_time'] = datetime.fromtimestamp(
                        msg['T']/1000.0)
                candle['insert_time'] = datetime.utcnow()
                if('n' in msg):
                    candle['qty'] = float(msg['n'])
                if('q' in msg):
                    candle['quote_asset_volume'] = float(msg['q'])
                if('V' in msg):
                    candle['taker_buy_base_asset_volume'] = float(msg['V'])
                if('Q' in msg):
                    candle['taker_buy_quote_asset_volume'] = float(msg['Q'])
                if('n' in msg):
                    candle['vol'] = float(msg['n'])
                if('f' in msg):
                    candle['first_trade_id'] = msg['f']
                if('L' in msg):
                    candle['last_trade_id'] = msg['L']
                if('origin' in msg):
                    candle['origin'] = msg['origin']

                msg_table = {}
                msg_table["table"] = table
                msg_table["data"] = candle

                self.queue.put(msg_table)
        except Exception as e:
            print('Error in writer.py get_mesage()', e)
            pass

        self.last_data = time.time()

        return True


    def send_mongo_update_obs(self, options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(
            self.mongo_update_obs
        )

    def send_queue_size(self, options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(
            self.queue.qsize()
        )

    @opentel.tracer.start_as_current_span(name=f"{SERVICE_NAME}.PETROSAWriter.update_forever")
    def update_forever(self):

        while True:
            msg_table = self.queue.get()
            # print("message on writer", msg_table)

            future = self.executor.submit(self.update_mongo, msg_table)
            # print("future", future.result())

    @opentel.tracer.start_as_current_span(name=f"{SERVICE_NAME}.PETROSAWriter.update_mongo")
    def update_mongo(self, candle):
        start_time = (time.time_ns() // 1_000_000)
        mg_table = self.client_mg.petrosa_crypto[candle["table"]]
        result_update = mg_table.update_one({"ticker": candle["data"]['ticker'],
                             "datetime": candle["data"]['datetime']},
                            {"$set": candle["data"]}, upsert=True)

        self.mongo_update_obs = (time.time_ns() // 1_000_000) - start_time
        # print(result_update.raw_result)
        return result_update