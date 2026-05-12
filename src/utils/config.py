import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'taxi-trips')

RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', 'data/raw/train.csv')
BRONZE_PATH = os.getenv('BRONZE_PATH', 'data/lake/bronze/taxi_trips')
SILVER_PATH = os.getenv('SILVER_PATH', 'data/lake/silver/taxi_trips')
GOLD_PATH = os.getenv('GOLD_PATH', 'data/lake/gold/taxi_trips_daily')

PRODUCER_SLEEP_SECONDS = float(os.getenv('PRODUCER_SLEEP_SECONDS', '0.05'))
CONSUMER_BATCH_SIZE = int(os.getenv('CONSUMER_BATCH_SIZE', '100'))