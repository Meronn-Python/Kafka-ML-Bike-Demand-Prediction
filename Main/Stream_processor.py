"""Uses Faust (Python) or Kafka Streams (Java/Scala) to consume raw-data, 
run the pre-trained ML model on each record, and produce a new message to the predictions topic."""

from pathlib import Path
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import ssl

import pandas as pd
import joblib
import faust
from faust.auth import SASLCredentials

# 1、Set env, credential, creat Faust to connect Kafka 
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
API_KEY = os.getenv("KAFKA_API_KEY")
API_SECRET = os.getenv("KAFKA_API_SECRET")
if not BOOTSTRAP_SERVER or not API_KEY or not API_SECRET:
    raise ValueError("Kafka credentials are missing.")

# Create Faust to connect Kafka broker, message type choose json
credentials = SASLCredentials(
    username=API_KEY,
    password=API_SECRET,
    mechanism="PLAIN",
    ssl_context=ssl.create_default_context()
)
app = faust.App(
    "bike-demand-stream-processor",
    broker=f"kafka://{BOOTSTRAP_SERVER}",
    broker_credentials=credentials,
    value_serializer="json",
    topic_replication_factor=3
)
# set topic objection: raw-data (producer sended)  / predictions in Kafka
raw_topic = app.topic("raw-data", value_serializer="json")
predictions_topic = app.topic("predictions", value_serializer="json")

# 2、Load trained model
model_package = joblib.load(ROOT / "Model" / "bike_demand_model.joblib") 
model = model_package["model"] # RandomForestClassifier() Type
features = model_package["features"]
accuracy = model_package["accuracy"]
f1 = model_package["f1"]
median_count = model_package["median_count"]
print(f"Model accuracy: {accuracy:.4f} \nModel F1-score: {f1:.4f} \n"
       + f"Features: {features} \nmedian_count {median_count}\n")


# 3、read input_data → model.predict() → prediction
@app.agent(raw_topic) # label: like poll
async def process_bike_data(stream):
    async for event in stream:
        # read
        input_data = pd.DataFrame( # put list as a data line []
            # like: result = {n: n * n for n in numbers}, return a [dict]
            [{feature: event[feature] for feature in features}] # Train.py
        )
        # predict
        prediction = int(model.predict(input_data)[0]) # return list[0]
        # set label
        prediction_label = "high demand" if prediction == 1 else "low demand"

        # 4、set callback form and send to predictions topic
        result = {
            "instant": event["instant"],
            "dteday": event["dteday"],
            "hour": event["hr"],
            "actual_count": event["cnt"],
            "prediction": prediction,
            "prediction_label": prediction_label,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
        # send to predictions topic
        await predictions_topic.send(value=result)
        print(f"Processed instant={event['instant']:<3}, hr={event['hr']:<3}, cnt={event['cnt']:<3} -> {prediction_label}")
