from fastapi import FastAPI
from prometheus_client import Counter, make_asgi_app
from pydantic import BaseModel
import mlflow.pyfunc
import pandas as pd

app = FastAPI(title="DataOps/MLOps Demo Model API")

PREDICTION_COUNTER = Counter("predictions_total", "Total predictions served")

# Prometheus scrape endpoint
app.mount("/metrics", make_asgi_app())

# Connect to MLflow and load the Staging model
mlflow.set_tracking_uri("http://mlflow:5000")
try:
    model = mlflow.pyfunc.load_model(model_uri="models:/Manufacturing_Defects_RF_Model/Staging")
except Exception as e:
    print(f"Error loading model: {e}")
    model = None

# Update to accept a list of 16 numerical features instead of just 2
class PredictRequest(BaseModel):
    features: list

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict")
def predict(req: PredictRequest):
    PREDICTION_COUNTER.inc()
    
    if not model:
        return {"error": "Model failed to load"}
    
    # Convert the incoming list into a DataFrame for the model
    df = pd.DataFrame([req.features])
    prediction = model.predict(df)
    
    return {"defect_prediction": int(prediction[0])}