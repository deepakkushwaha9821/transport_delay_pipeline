import pandas as pd
import psycopg2
from sklearn.linear_model import LinearRegression

conn = psycopg2.connect(
    host="localhost",
    database="transportdb",
    user="postgres",
    password="deepak123"   # change this
)

df = pd.read_sql("SELECT * FROM transport_delay", conn)
conn.close()

df["hour"] = pd.to_datetime(df["scheduled_arrival"]).dt.hour

X = df[["hour"]]
y = df["delay_minutes"]

model = LinearRegression()
model.fit(X, y)

pred = model.predict([[10]])[0]

print("Predicted delay at 10 AM:", round(pred,2), "minutes")