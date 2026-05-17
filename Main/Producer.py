"""Reads rows from your dataset and publishes 
each one as a JSON message to the raw-data topic at ~1 row/second."""

from pathlib import Path
import os
from dotenv import load_dotenv

import pandas as pd
import json
import time
from confluent_kafka import Producer, Consumer

# 1、Read/Load from csv
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
dataset = pd.read_csv(ROOT / "Data" / "hour.csv")

# 2、Kafka Configuration
# pip install confluent-kafka --quiet
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
API_KEY = os.getenv("KAFKA_API_KEY")
API_SECRET = os.getenv("KAFKA_API_SECRET")
if not BOOTSTRAP_SERVER or not API_KEY or not API_SECRET:
    raise ValueError("Kafka credentials are missing.")
KAFKA_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
}
TOPIC_NAME = "raw-data"

# delivery feedback
def on_delivery(err, msg):
    """Callback fired after each message is acknowledged by the broker."""
    if err:
        print(f" Delivery failed: {err}")
    else:# key().decode()
        print(f" '{msg.key().decode()}'  →  partition {msg.partition()}  offset {msg.offset()}")

# 3、Transform JSON
# 4、Send Kafka the raw-data topic at ~1 row/second
producer  = Producer(KAFKA_CONFIG) # == from kafka import KafkaProducer
print("Config ready connecting to:", BOOTSTRAP_SERVER)
print(f"Loaded {len(dataset)} rows from hour.csv")

print("Send rows to Kafka at 1 row/second...\n")
for index, row in dataset.head(300).iterrows(): #iterrows return index + data
    # dataset -→ dict(bytes) -→ JSON
    msg = row.to_dict()
    # prepare send to Kafka
    producer.produce(
        topic       = TOPIC_NAME,
        key         = str(msg["instant"]).encode("utf-8"),
        value       = json.dumps(msg, default=str).encode("utf-8"),
        on_delivery = on_delivery
    )
    producer.poll(0)
    print(f"Sent row {index}: instant={msg['instant']}, hr={msg['hr']}, cnt={msg['cnt']}")
    time.sleep(1)

producer.flush()
print("\nProducer finished sending row data.") 



