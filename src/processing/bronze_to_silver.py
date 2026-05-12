import os
import shutil

import pandas as pd

from src.utils.config import BRONZE_PATH, SILVER_PATH
from src.utils.logger import get_logger


logger = get_logger(__name__)


def transform_bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["key"] = df["key"].astype("string")
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")

    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
    df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce")
    df["pickup_latitude"] = pd.to_numeric(df["pickup_latitude"], errors="coerce")
    df["pickup_longitude"] = pd.to_numeric(df["pickup_longitude"], errors="coerce")
    df["dropoff_latitude"] = pd.to_numeric(df["dropoff_latitude"], errors="coerce")
    df["dropoff_longitude"] = pd.to_numeric(df["dropoff_longitude"], errors="coerce")

    df = df.dropna(
        subset=[
            "key",
            "pickup_datetime",
            "fare_amount",
            "passenger_count",
            "pickup_latitude",
            "pickup_longitude",
            "dropoff_latitude",
            "dropoff_longitude",
        ]
    )

    df = df[df["fare_amount"] > 0]
    df = df[df["passenger_count"] > 0]

    df = df[df["pickup_latitude"].between(40.0, 42.0)]
    df = df[df["pickup_longitude"].between(-75.0, -72.0)]
    df = df[df["dropoff_latitude"].between(40.0, 42.0)]
    df = df[df["dropoff_longitude"].between(-75.0, -72.0)]

    df["pickup_date"] = df["pickup_datetime"].dt.date.astype(str)
    df["pickup_year"] = df["pickup_datetime"].dt.year
    df["pickup_month"] = df["pickup_datetime"].dt.month
    df["pickup_day"] = df["pickup_datetime"].dt.day
    df["pickup_hour"] = df["pickup_datetime"].dt.hour

    return df


def main() -> None:
    logger.info("Starting bronze to silver processing")

    if os.path.exists(SILVER_PATH):
        shutil.rmtree(SILVER_PATH)

    os.makedirs(SILVER_PATH, exist_ok=True)

    df_bronze = pd.read_parquet(BRONZE_PATH)
    logger.info(f"Bronze records read: {len(df_bronze)}")

    df_silver = transform_bronze_to_silver(df_bronze)
    logger.info(f"Silver records after transformation: {len(df_silver)}")

    output_file = os.path.join(SILVER_PATH, "silver_taxi_trips.parquet")

    if os.path.exists(output_file):
        os.remove(output_file)

    df_silver.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
    )

    logger.info(f"Silver data saved at: {output_file}")


if __name__ == "__main__":
    main()
