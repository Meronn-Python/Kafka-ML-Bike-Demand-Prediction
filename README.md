# Real-Time Bike Demand Prediction with Kafka and Faust

## 1. Project Overview

This project builds a real-time ML inference pipeline with three components:

1. **Producer**: reads rows from **hour.csv** and sends each row as a JSON message to the **raw-data** Kafka topic.
2. **Stream Processor**: uses Faust to consume **raw-data**, loads a trained ML model, predicts high/low bike demand, and sends the result to the **predictions** topic.
3. **Output Consumer**: reads from **predictions** and prints prediction results in the terminal.

Pipeline:

```
Producer → raw-data → Faust Stream Processor → predictions → Output Consumer
```

## 2. Dataset

This project uses the **Bike Sharing Dataset**, specifically **hour.csv**.

```
archive.ics.uci.edu/dataset/275
```

The original task is to predict hourly rental count (**cnt**). Since the assignment rubric requires Accuracy and F1-score, this project converts the regression task into a binary classification task:

```
- high_demand = 1 if "cnt" is greater than the median value
- high_demand = 0 otherwise
```

The selected input features are:

```
season, yr, mnth, hr, holiday, weekday, workingday, weathersit, temp, atemp, hum, windspeed
```

Excluded columns:

```
casual, registered, cnt
```

**casual** and **registered** were excluded because **casual + registered = cnt**, so using them would leak the answer.

## 3. Streams Library

This project uses **Python Faust** as the stream processing library.

Faust is used to define a stream processor with an agent that consumes records from the **raw-data** topic, applies the trained model, and publishes predictions to the **predictions** topic.

## 4. Machine Learning Model

Model used:

```
RandomForestClassifier
```

**Model performance** using an 80/20 train-test split:

```
Accuracy: 0.9298
F1-score: 0.9303
```

The model was trained offline in **TrainByOffline/TrainModel.py** and saved as:

```
Model/bike_demand_model.joblib
```

## 5. Project Structure

```
ML_Personal_Project/
├── Data/
│   ├── hour.csv
│   └── Readme.txt
├── Main/
│   ├── Consumer.py
│   ├── Producer.py
│   └── Stream_processor.py
├── Model/
│   └── bike_demand_model.joblib
├── TrainByOffline/
│   └── TrainModel.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

## 6. Setup

Go to the project folder:

```
cd ML_Personal_Project
```

Install dependencies:

```
pip install -r requirements.txt
```

Python version used:

```
Python 3.10.11
```

Create a **.env** file in the project root based on **.env.example**:

```env
KAFKA_BOOTSTRAP_SERVER=your_bootstrap_server
KAFKA_API_KEY=your_api_key
KAFKA_API_SECRET=your_api_secret
```

Create these **Kafka topics** in Confluent Cloud:

```
raw-data
predictions
```

Check that the **trained model** file exists:

```
Model/bike_demand_model.joblib
```

If the train model does not existing, train the model:

```
python TrainByOffline/TrainModel.py
```

## 7. How to Run

Open three terminals:

```
cd ML_Personal_Project/Main
```

Then run the following commands **in order**.

### Terminal 1 - Stream Processor

```
python -m faust -A Stream_processor worker -l info
```

### Terminal 2 - Output Consumer

```
python Consumer.py
```

### Terminal 3 - Producer

```
python Producer.py
```

## 8. Demo Video

Video link: 

```
https://drive.google.com/file/d/1vyo1FyT8xhzTC5X12q3PZ7RaNd4jyfjl/view?usp=drive_link
```

