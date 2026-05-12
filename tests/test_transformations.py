import pandas as pd

from src.processing.bronze_to_silver import transform_bronze_to_silver
from src.processing.silver_to_gold import create_gold_metrics


def test_transform_bronze_to_silver_removes_invalid_fares():
    df = pd.DataFrame(
        [
            {
                'key': '1',
                'fare_amount': 10.0,
                'pickup_datetime': '2014-01-01 10:00:00 UTC',
                'pickup_longitude': -73.98,
                'pickup_latitude': 40.75,
                'dropoff_longitude': -73.99,
                'dropoff_latitude': 40.76,
                'passenger_count': 1,
            },
            {
                'key': '2',
                'fare_amount': -5.0,
                'pickup_datetime': '2014-01-01 11:00:00 UTC',
                'pickup_longitude': -73.98,
                'pickup_latitude': 40.75,
                'dropoff_longitude': -73.99,
                'dropoff_latitude': 40.76,
                'passenger_count': 1,
            },
        ]
    )

    result = transform_bronze_to_silver(df)

    assert len(result) == 1
    assert result.iloc[0]['key'] == '1'


def test_create_gold_metrics():
    df = pd.DataFrame(
        [
            {
                'key': '1',
                'pickup_date': '2014-01-01',
                'pickup_hour': 10,
                'fare_amount': 10.0,
                'passenger_count': 1,
            },
            {
                'key': '2',
                'pickup_date': '2014-01-01',
                'pickup_hour': 10,
                'fare_amount': 20.0,
                'passenger_count': 2,
            },
        ]
    )

    result = create_gold_metrics(df)

    assert len(result) == 1
    assert result.iloc[0]['total_trips'] == 2
    assert result.iloc[0]['avg_fare_amount'] == 15.0
    assert result.iloc[0]['total_fare_amount'] == 30.0
