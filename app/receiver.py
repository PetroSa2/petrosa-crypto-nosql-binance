import json
import logging
import os
import time

from petrosa.observability import opentel
from petrosa.messaging import kafkareceiver

from app import writer

class PETROSAReceiver(object):
    def __init__(self,
                 topic
                 ) -> None:
        try:
            self.consumer = kafkareceiver.get_consumer(topic)
            self.writer = writer.PETROSAWriter()
            self.topic = topic
        except:
            print('Error in Kafka Consumer')

    @opentel.tracer.start_as_current_span(name=f"{opentel.SERVICE_NAME}.PETROSAReceiver.run")
    def run(self):
        work_counter_general = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv."+self.topic, unit="1", description="Msgs Received on topic " + self.topic
        )
        work_counter_m1 = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv.m1."+self.topic, unit="1", description="M1 Msgs Received on topic " + self.topic
        )
        work_counter_m5 = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv.m5."+self.topic, unit="1", description="M5 Msgs Received on topic " + self.topic
        )
        work_counter_m15 = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv.m15."+self.topic, unit="1", description="M15 Msgs Received on topic " + self.topic
        )
        work_counter_m30 = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv.m30."+self.topic, unit="1", description="m30 Msgs Received on topic " + self.topic
        )
        work_counter_h1 = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv.h1."+self.topic, unit="1", description="H1 Msgs Received on topic " + self.topic
        )
        work_counter_d1 = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv.d1."+self.topic, unit="1", description="D1 Msgs Received on topic " + self.topic
        )
        work_counter_w1 = opentel.meter.create_counter(
            opentel.SERVICE_NAME + ".rcv.w1."+self.topic, unit="1", description="W1 Msgs Received on topic " + self.topic
        )
        try:
            for msg in self.consumer:
                msg = json.loads(msg.value.decode())
                msg['k']['petrosa_db_timestamp'] = time.time()

                work_counter_general.add(1)

                if('k' in msg):
                    if(msg['k']['i'] == '1m'):
                        self.writer.get_msg("candles_m1", msg['k'])
                        work_counter_m1.add(1)
                    if (msg['k']['i'] == '5m'):
                        self.writer.get_msg("candles_m5", msg['k'])
                        work_counter_m5.add(1)
                    elif(msg['k']['i'] == '15m'):
                        self.writer.get_msg("candles_m15", msg['k'])
                        work_counter_m15.add(1)
                    elif(msg['k']['i'] == '30m'):
                        self.writer.get_msg("candles_m30", msg['k'])
                        work_counter_m30.add(1)
                    elif(msg['k']['i'] == '1h'):
                        self.writer.get_msg("candles_h1", msg['k'])
                        work_counter_h1.add(1)
                    elif(msg['k']['i'] == '1d'):
                        self.writer.get_msg("candles_d1", msg['k'])
                        work_counter_d1.add(1)
                    elif(msg['k']['i'] == '1w'):
                        self.writer.get_msg("candles_w1", msg['k'])
                        work_counter_w1.add(1)
                    else:
                        print('WE ALL GONNA DIEEEEEE')
                        print(msg)
        except Exception as e:
            logging.error(e)
            logging.error('Error in Kafka Consumer')
            os._exit(1)
        

        return True
