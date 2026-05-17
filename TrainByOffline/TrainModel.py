from pathlib import Path

import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score

# 1: Load hour.csv
ROOT = Path(__file__).resolve().parents[1]
dataset = pd.read_csv(ROOT / "Data" / "hour.csv")
# print(dataset.head()) # First 5 line of data

# 2: Create high_demand label for classification - Accuracy + F1 beforehand
median_count = dataset["cnt"].median()
dataset["high_demand"] = (dataset["cnt"]>median_count).astype(int) #transform. false=0,true=1
# print(dataset["high_demand"].value_counts()) # check for is it apprapriate?
# print("median_count = "+ str(median_count)+"\n", dataset["median_count"])

# 3: Train RandomForestClassifier
''' 
x is question(feature without answer), y is answer, 80% : 20% ;RandomForestClassifier
# instant,dteday,season,yr,mnth,hr,holiday,weekday,workingday,weathersit,temp,atemp,hum,windspeed,casual,registered,cnt
# cnt = casual+registered, which shoud be excluded
'''
# Related factors
features = [
    "season","yr","mnth","hr",
    "holiday","weekday","workingday","weathersit",
    "temp","atemp","hum","windspeed"
]
x = dataset[features]
y = dataset["high_demand"]
# train_test_split, return 4 types data - 80% : 20%
x_train, x_test, y_train, y_test = train_test_split(x,y,test_size=0.2)
model = RandomForestClassifier(n_estimators=100,random_state=42)
model.fit(x_train, y_train)

# 4: Print Accuracy + F1
y_pred = model.predict(x_test)
accuracy = accuracy_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.4f}")
print(f"F1-score: {f1:.4f}")

# 5: Save model.joblib
model_path = ROOT / "Model" / "bike_demand_model.joblib"
joblib.dump(
    {
        "model": model,
        "features": features,
        "accuracy": accuracy,
        "f1": f1,
        "median_count": median_count
    },
    model_path
)

print(f"Model saved to: {model_path}")