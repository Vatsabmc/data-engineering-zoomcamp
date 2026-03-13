import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from kafka import KafkaConsumer
from models import GreenRide, ride_deserializer

server = "localhost:9092"
topic_name = "green-trips"

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="postgres",
)
conn.autocommit = True
cur = conn.cursor()

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset="earliest",
    group_id="rides-to-postgres",
    value_deserializer=lambda data: ride_deserializer(ride_class=GreenRide, data=data),
)

print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0
for message in consumer:
    ride = message.value
    cur.execute(
        """INSERT INTO processed_events
           (PULocationID, DOLocationID, trip_distance, tip_amount, total_amount, passenger_count, pickup_datetime, dropoff_datetime)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            ride.PULocationID,
            ride.DOLocationID,
            ride.trip_distance,
            ride.tip_amount,
            ride.total_amount,
            ride.passenger_count,
            ride.lpep_pickup_datetime,
            ride.lpep_dropoff_datetime
        ),
    )
    count += 1
    if count % 1000 == 0:
        print(f"Inserted {count} rows...")
print(f"Finished inserting {count} rows.")

consumer.close()
cur.close()
conn.close()
