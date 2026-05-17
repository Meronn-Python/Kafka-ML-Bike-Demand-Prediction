"""A simple consumer that reads from predictions 
and prints each result to the console in a readable format as it arrives."""
from pathlib import Path
import os
import json

from confluent_kafka import Consumer
from dotenv import load_dotenv

# 1. Load environment variables
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
API_KEY = os.getenv("KAFKA_API_KEY")
API_SECRET = os.getenv("KAFKA_API_SECRET")
if not BOOTSTRAP_SERVER or not API_KEY or not API_SECRET:
    raise ValueError("Kafka credentials are missing.")

# 2. Kafka Consumer configuration
KAFKA_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,

    # Consumer group id
    "group.id": "bike-demand-output-consumer-demo",
    "auto.offset.reset": "earliest",
}
TOPIC_NAME = "predictions"

# 3. Create consumer and subscribe to predictions topic
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([TOPIC_NAME])

print(f"Consumer Reading from topic: {TOPIC_NAME}")

# 4. Read and print prediction results
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        result = json.loads(msg.value().decode("utf-8"))

        print(
            f"Prediction result | "
            f"instant={result['instant']:<3} | "
            f"date={result['dteday']} | "
            f"hour={result['hour']:<3} | "
            f"actual_count={result['actual_count']:<3} | "
            f"prediction={result['prediction_label']}"
        )

except KeyboardInterrupt: # ctrl + C
    print("\nConsumer stopped by user.")

finally:
    consumer.close()
    print("Consumer closed.")