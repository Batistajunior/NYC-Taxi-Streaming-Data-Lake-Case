import os
import shutil

import pandas as pd

from src.utils.config import SILVER_PATH, GOLD_PATH
from src.utils.logger import get_logger


logger = get_logger(__name__)


def create_gold_metrics(df: pd.DataFrame) -> pd.DataFrame:
    metrics = (
        df.groupby(["pickup_date", "pickup_hour"])
        .agg(
            total_trips=("key", "count"),
            avg_fare_amount=("fare_amount", "mean"),
            total_fare_amount=("fare_amount", "sum"),
            avg_passenger_count=("passenger_count", "mean"),
        )
        .reset_index()
    )

    metrics["avg_fare_amount"] = metrics["avg_fare_amount"].round(2)
    metrics["total_fare_amount"] = metrics["total_fare_amount"].round(2)
    metrics["avg_passenger_count"] = metrics["avg_passenger_count"].round(2)

    return metrics


def main() -> None:
    logger.info("Starting silver to gold processing")

    if os.path.exists(GOLD_PATH):
        shutil.rmtree(GOLD_PATH)

    os.makedirs(GOLD_PATH, exist_ok=True)

    input_file = os.path.join(SILVER_PATH, "silver_taxi_trips.parquet")

    df_silver = pd.read_parquet(input_file, engine="pyarrow")
    logger.info(f"Silver records read: {len(df_silver)}")

    df_gold = create_gold_metrics(df_silver)
    logger.info(f"Gold records created: {len(df_gold)}")

    output_file = os.path.join(GOLD_PATH, "gold_metrics.parquet")

    if os.path.exists(output_file):
        os.remove(output_file)

    df_gold.to_parquet(
        output_file,
        index=False,
        engine="pyarrow",
    )

    logger.info(f"Gold data saved at: {output_file}")


if __name__ == "__main__":
    main()
