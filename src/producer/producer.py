
import json
import time
from typing import Dict, Any

import pandas as pd
from kafka import KafkaProducer

from src.utils.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    RAW_DATA_PATH,
    PRODUCER_SLEEP_SECONDS,
)
from src.utils.logger import get_logger


logger = get_logger(__name__)


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, default=str).encode('utf-8'),
    )


def build_event(row: pd.Series) -> Dict[str, Any]:
    return {
        'key': row.get('key'),
        'fare_amount': row.get('fare_amount'),
        'pickup_datetime': row.get('pickup_datetime'),
        'pickup_longitude': row.get('pickup_longitude'),
        'pickup_latitude': row.get('pickup_latitude'),
        'dropoff_longitude': row.get('dropoff_longitude'),
        'dropoff_latitude': row.get('dropoff_latitude'),
        'passenger_count': row.get('passenger_count'),
    }


def main() -> None:
    logger.info('Starting NYC Taxi producer')
    logger.info(f'Reading dataset from: {RAW_DATA_PATH}')

    producer = create_producer()

    total_events = 0

    for chunk in pd.read_csv(RAW_DATA_PATH, chunksize=1000):
        for _, row in chunk.iterrows():
            event = build_event(row)

            producer.send(KAFKA_TOPIC, value=event)

            total_events += 1

            if total_events % 100 == 0:
                logger.info(f'Total events sent: {total_events}')

            time.sleep(PRODUCER_SLEEP_SECONDS)

    producer.flush()
    producer.close()

    logger.info(f'Producer finished. Total events sent: {total_events}')


if __name__ == '__main__':
    main()
