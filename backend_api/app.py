from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import pandas as pd
from sklearn.linear_model import LinearRegression

app = Flask(__name__)

# ✅ ENABLE CORS
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ---------- Database connection ----------
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="transportdb",
        user="postgres",
        password="deepak123"   # your password
    )

# ---------- Route 1: Top delayed routes ----------
@app.route("/api/top-delays")
def top_delays():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT origin, destination, 
               ROUND(AVG(delay_minutes)::numeric,2) AS avg_delay
        FROM transport_delay
        GROUP BY origin, destination
        ORDER BY avg_delay DESC
        LIMIT 5;
    """, conn)
    conn.close()

    return jsonify(df.to_dict(orient="records"))

# ---------- Route 2: Delay prediction ----------
@app.route("/api/predict")
def predict_delay():
    hour = int(request.args.get("hour", 10))

    conn = get_connection()
    df = pd.read_sql(
        "SELECT scheduled_arrival, delay_minutes FROM transport_delay",
        conn
    )
    conn.close()

    df["hour"] = pd.to_datetime(df["scheduled_arrival"]).dt.hour
    X = df[["hour"]]
    y = df["delay_minutes"]

    model = LinearRegression()
    model.fit(X, y)

    prediction = model.predict([[hour]])[0]

    return jsonify({
        "hour": hour,
        "predicted_delay_minutes": round(float(prediction), 2)
    })

# ---------- Run server ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
