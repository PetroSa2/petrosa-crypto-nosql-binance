import logging
import threading
from typing import Iterable

from petrosa.observability import opentel
from opentelemetry.metrics import CallbackOptions, Observation

from app import receiver
import os


def send_number_of_threads(options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(
        threading.active_count()
    )


opentel.meter.create_observable_gauge(
    opentel.SERVICE_NAME + ".number.of.threads",
    callbacks=[send_number_of_threads],
    description="Number of Threads",
)

logging.basicConfig(level=logging.INFO)

receiver_socket = receiver.PETROSAReceiver(
                                'binance_klines_current',
                                )


receiver_backfill = receiver.PETROSAReceiver(
                                'binance_backfill',
                                )


logging.warning('Starting the writer')

th_receiver_socket = threading.Thread(target=receiver_socket.run)
th_receiver_backfill = threading.Thread(target=receiver_backfill.run)
th_receiver_socket.start()
th_receiver_backfill.start()
th_receiver_socket.join()
th_receiver_backfill.join()
