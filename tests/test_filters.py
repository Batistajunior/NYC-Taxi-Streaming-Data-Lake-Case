from src.consumer.consumer import is_valid_event, event_matches_filters


def test_valid_event():
    event = {
        'key': 'abc',
        'fare_amount': 10.5,
        'pickup_datetime': '2014-01-01 10:00:00 UTC',
        'pickup_longitude': -73.98,
        'pickup_latitude': 40.75,
        'dropoff_longitude': -73.99,
        'dropoff_latitude': 40.76,
        'passenger_count': 1,
    }

    assert is_valid_event(event) is True


def test_invalid_event_without_fare():
    event = {
        'key': 'abc',
        'fare_amount': None,
        'pickup_datetime': '2014-01-01 10:00:00 UTC',
        'pickup_longitude': -73.98,
        'pickup_latitude': 40.75,
        'dropoff_longitude': -73.99,
        'dropoff_latitude': 40.76,
        'passenger_count': 1,
    }

    assert is_valid_event(event) is False


def test_event_matches_location_filter():
    event = {
        'pickup_datetime': '2014-01-01 10:00:00 UTC',
        'pickup_longitude': -73.98,
        'pickup_latitude': 40.75,
    }

    result = event_matches_filters(
        event=event,
        min_latitude=40.70,
        max_latitude=40.80,
        min_longitude=-74.00,
        max_longitude=-73.90,
    )

    assert result is True
