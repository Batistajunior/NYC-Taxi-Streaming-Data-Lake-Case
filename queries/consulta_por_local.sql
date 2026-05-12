
SELECT
    pickup_date,
    pickup_hour,
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
    ROUND(SUM(fare_amount), 2) AS total_fare_amount,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM read_parquet('data/lake/silver/taxi_trips/**/*.parquet')
WHERE pickup_latitude BETWEEN 40.70 AND 40.80
  AND pickup_longitude BETWEEN -74.02 AND -73.90
GROUP BY pickup_date, pickup_hour
ORDER BY pickup_date, pickup_hour;
