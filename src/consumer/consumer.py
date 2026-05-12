import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
from kafka import KafkaConsumer

from src.utils.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    BRONZE_PATH,
    CONSUMER_BATCH_SIZE,
)
from src.utils.logger import get_logger


logger = get_logger(__name__)


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="taxi-trips-consumer-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def is_valid_event(event: Dict[str, Any]) -> bool:
    required_fields = [
        "key",
        "fare_amount",
        "pickup_datetime",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "passenger_count",
    ]

    for field in required_fields:
        if event.get(field) is None:
            return False

    return True


def event_matches_filters(
    event: Dict[str, Any],
    start_date: Optional[str] = None,
    min_latitude: Optional[float] = None,
    max_latitude: Optional[float] = None,
    min_longitude: Optional[float] = None,
    max_longitude: Optional[float] = None,
) -> bool:
    pickup_datetime = pd.to_datetime(event.get("pickup_datetime"), errors="coerce")

    if pd.isna(pickup_datetime):
        return False

    if start_date:
        start_date_converted = pd.to_datetime(start_date)
        if pickup_datetime < start_date_converted:
            return False

    try:
        pickup_latitude = float(event.get("pickup_latitude"))
        pickup_longitude = float(event.get("pickup_longitude"))
    except (TypeError, ValueError):
        return False

    if min_latitude is not None and pickup_latitude < min_latitude:
        return False

    if max_latitude is not None and pickup_latitude > max_latitude:
        return False

    if min_longitude is not None and pickup_longitude < min_longitude:
        return False

    if max_longitude is not None and pickup_longitude > max_longitude:
        return False

    return True


def save_to_bronze(events: List[Dict[str, Any]]) -> None:
    if not events:
        return

    os.makedirs(BRONZE_PATH, exist_ok=True)

    df = pd.DataFrame(events)
    df["ingestion_timestamp"] = datetime.utcnow()

    file_name = f"part-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.parquet"
    output_path = os.path.join(BRONZE_PATH, file_name)

    df.to_parquet(output_path, index=False)

    logger.info(f"Saved {len(events)} events to {output_path}")


def main() -> None:
    logger.info("Starting NYC Taxi consumer")

    consumer = create_consumer()
    buffer = []

    for message in consumer:
        event = message.value

        if not is_valid_event(event):
            continue

        if not event_matches_filters(
            event=event,
            start_date=None,
            min_latitude=40.50,
            max_latitude=41.00,
            min_longitude=-74.30,
            max_longitude=-73.60,
        ):
            continue

        buffer.append(event)

        if len(buffer) >= CONSUMER_BATCH_SIZE:
            save_to_bronze(buffer)
            buffer = []


if __name__ == "__main__":
    main()