
SELECT
    pickup_date,
    pickup_hour,
    total_trips,
    avg_fare_amount,
    total_fare_amount,
    avg_passenger_count
FROM read_parquet('data/lake/gold/taxi_trips_daily/*.parquet')
ORDER BY pickup_date, pickup_hour;
