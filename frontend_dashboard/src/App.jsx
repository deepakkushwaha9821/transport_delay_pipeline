import React, { useEffect, useState } from "react";

function App() {
  const [routes, setRoutes] = useState([]);
  const [prediction, setPrediction] = useState(null);
  const [hour, setHour] = useState(10);
  const [loading, setLoading] = useState(false);

  // Load top delayed routes
  useEffect(() => {
    fetch("http://localhost:5000/api/top-delays")
      .then((res) => res.json())
      .then((data) => setRoutes(data))
      .catch((err) => console.error("Top delays error:", err));
  }, []);

  // Call prediction API
  const getPrediction = () => {
    setLoading(true);
    setPrediction(null);

    fetch(`http://localhost:5000/api/predict?hour=${hour}`)
      .then((res) => res.json())
      .then((data) => {
        console.log("Prediction API response:", data);
        setPrediction(data.predicted_delay_minutes);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Prediction error:", err);
        setLoading(false);
      });
  };

  return (
    <div style={{ padding: "30px", fontFamily: "Arial, sans-serif" }}>
      <h2>🚆 Smart Transportation Delay Dashboard</h2>

      {/* Top delayed routes */}
      <h3>Top Delayed Routes</h3>
      {routes.length === 0 ? (
        <p>Loading routes...</p>
      ) : (
        <ul>
          {routes.map((r, i) => (
            <li key={i}>
              {r.origin} → {r.destination} : <b>{r.avg_delay}</b> mins
            </li>
          ))}
        </ul>
      )}

      <hr />

      {/* Prediction */}
      <h3>Predict Delay by Hour</h3>

      <input
        type="number"
        value={hour}
        onChange={(e) => setHour(Number(e.target.value))}
        min="0"
        max="23"
        style={{ marginRight: "10px", padding: "5px" }}
      />

      <button onClick={getPrediction} style={{ padding: "6px 12px" }}>
        Predict
      </button>

      {loading && <p>Predicting...</p>}

      {prediction !== null && !loading && (
        <p style={{ marginTop: "10px", fontSize: "18px" }}>
          ⏱️ Predicted Delay at <b>{hour}:00</b> =
          <b> {prediction.toFixed(2)}</b> minutes
        </p>
      )}
    </div>
  );
}

export default App;
