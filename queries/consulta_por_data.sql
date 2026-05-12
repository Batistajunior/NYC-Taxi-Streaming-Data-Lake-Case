
SELECT
    pickup_date,
    SUM(total_trips) AS total_trips,
    ROUND(SUM(total_fare_amount), 2) AS total_fare_amount,
    ROUND(AVG(avg_fare_amount), 2) AS avg_fare_amount,
    ROUND(AVG(avg_passenger_count), 2) AS avg_passenger_count
FROM read_parquet('data/lake/gold/taxi_trips_daily/*.parquet')
GROUP BY pickup_date
ORDER BY pickup_date;
