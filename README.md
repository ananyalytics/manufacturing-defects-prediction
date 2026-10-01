# Manufacturing Quality Prediction — End-to-End DataOps & MLOps Pipeline

An enterprise-grade, containerized DataOps and MLOps platform automating data ingestion, validation, feature transformation, model training, artifact tracking, real-time inference, and observability for predictive manufacturing quality control.

---

## Academic Context & Team Details

* **Institution:** Jain University & HCL GUVI
* **Program:** DataOps & MLOps Corporate Training / Capstone Project
* **Semester:** Semester III

### Team Members

| Name | USN | Individual Contribution |
| --- | --- | --- |
| **Ananya Jha** | 25BSR00580 | Airflow Pipeline Automation & Grafana Observability Dashboard |
| **Devraj Sharma** | 25BSR00219 | Data Ingestion, Directory Setup & Data Quality Validation |
| **Mohammed Habibulla** | 25BSR00545 | Feature Engineering, Dataset Splitting & Preprocessing |
| **Ayush Babu** | 25BSR00476 | Baseline Model Training (Random Forest) & Evaluation |
| **Muddassir Ahmed** | 25BSR00237 | MLflow Tracking Server, Artifact Logging & Model Registry |

---

## Problem Statement

In modern manufacturing environments, undetected defects on the shop floor lead to substantial material waste, costly rework cycles, and severe downstream supply chain bottlenecks. The objective of this project is to build an automated, production-grade DataOps and MLOps pipeline that predicts component quality in real time.

Using telemetry from the `manufacturing_defects.csv` dataset, the system analyzes 16 numerical sensor and process features to classify the `DefectStatus` of a manufactured unit. By automating data ingestion, training orchestration, and real-time model serving via REST API, the platform enables quality control engineers to proactively isolate defective components before they advance further along the assembly line.

---

## Dataset Description

* **Source:** Manufacturing quality process telemetry ([Kaggle Dataset](https://www.kaggle.com/datasets/rabieelkharoua/predicting-manufacturing-defects-dataset))
* **Total Records:** 3,240 rows, 17 columns
* **Feature Scope:** Captures production volumes, supply chain input quality, maintenance history, equipment inspection metrics, inventory management status, workforce productivity metrics, energy consumption patterns, and additive manufacturing specifics.
* **Target Variable:** `DefectStatus`
* `0`: Low Defects / Acceptable Quality
* `1`: High Defects / Quality Anomaly



---

## System Architecture

```text
+-------------------+      +--------------------+      +--------------------+
|  Raw Data Source  | ---> |   Data Ingestion   | ---> |   Data Validation  |
|  (CSV Telemetry)  |      |   (Pandas / Disk)  |      |   (Missing Checks) |
+-------------------+      +--------------------+      +--------------------+
                                                                  |
+-------------------+      +--------------------+                 v
|   MLflow Model    | <--- |   Model Training   | <--- +--------------------+
|     Registry      |      |   (Random Forest)  |      | Feature Scaler /   |
+-------------------+      +--------------------+      | Train-Test Split   |
          |                                            +--------------------+
          v
+-------------------+      +--------------------+      +--------------------+
| Airflow DAG Run   | ---> | FastAPI Deployment | ---> | Prometheus Scraping|
| (Automation Hub)  |      |  (/predict Service)|      |   & Grafana UI     |
+-------------------+      +--------------------+      +--------------------+

```

1. **Data Ingestion & Quality:** Load structured batch telemetry and inspect schema and completeness.
2. **Transformation:** Split features into train/test subsets (80/20) and scale features using `StandardScaler`.
3. **Training & Tracking:** Fit a `RandomForestClassifier`, evaluate performance, and stream metrics/artifacts to MLflow.
4. **Validation Gate:** Apply a programmatic threshold ($\ge 90\%$ accuracy) before registering to the MLflow Model Registry under `Staging`.
5. **Orchestration:** Schedule recurring execution using an Apache Airflow DAG (`manufacturing_quality_pipeline`).


6. **Inference & Serving:** Expose a FastAPI REST endpoint (`/predict`) packaging the registered versioned model.
7. **Observability:** Scrape system and inference metrics using Prometheus; display real-time telemetry on Grafana.



---

## Repository Structure

```text
├── dags/
│   ├── ml_pipeline_dag.py
│   ├── starter_pipeline.py   
│   └── __pycache__/
│
├── evidence-screenshots/
│   ├── airflow-run.png
│   ├── fasapi-demo.png
│   ├── grafana-visuals.png
│   ├── mlflow-overview.png 
│   └── model-metrics.png
│
├── fastapi-demo/
│   ├── Dockerfile                
│   ├── requirements.txt          
│   └── app.py                    
│
├── jupyter/
│   ├── requirements.txt          
│   └── app.py 
│
├── mlflow-data/
│   ├── Model Metrics.png         
│   └── Overview.png
│
├── notebooks/
│   ├── GUVI PRO.ipynb 
│   ├── manufacturing_dafects.csv
│   ├── ipynb_checkpoints/
│   └── mlruns/                  
│
├── prometheus/
│   └── prometheus.yml           
│
├── docker-compose.yml  
├── participant-requirements.txt
├── verify-setup.sh          
└── README.md                    

```

---

## Pipeline Implementation & Code

### 1. Data Ingestion & Quality Inspection

* **Library:** `pandas`
* **Task:** Load data from the shared volume, assess dimensions, verify column types, and confirm complete records.

```python
import pandas as pd

# Load CSV from mounted path
file_path = "manufacturing_defects.csv"
print(f"Loading data from {file_path}...")
df = pd.read_csv(file_path)

# Verify shape and missing records
print(f"Dataset successfully loaded. Shape: {df.shape}")
missing_data = df.isnull().sum()
print("\nMissing values per column:")
print(missing_data[missing_data > 0])

```

**Output:**

```text
Loading data from manufacturing_defects.csv...
Dataset successfully loaded. Shape: (3240, 17)
Missing values per column:
Series([], dtype: int64)

```

---

### 2. Feature Engineering & Preprocessing

* **Library:** `scikit-learn`
* **Task:** Separate predictor matrix ($X$) from target vector ($y$), partition into training and evaluation sets, and standardize numerical scale.

```python
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

X = df.drop('DefectStatus', axis=1)
y = df['DefectStatus']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
print(f"Training data shape: {X_train.shape}")
print(f"Testing data shape: {X_test.shape}")

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)
print("Feature scaling complete.")

```

**Output:**

```text
Training data shape: (2592, 16)
Testing data shape: (648, 16)
Feature scaling complete.

```

---

### 3. Model Training & Evaluation

* **Algorithm:** `RandomForestClassifier` (`n_estimators=100`, `random_state=42`)
* **Evaluation Baseline:** 95.52% overall test accuracy

```python
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train_scaled, y_train)

predictions = model.predict(X_test_scaled)
accuracy = accuracy_score(y_test, predictions)

print(f"Baseline Model Accuracy: {accuracy * 100:.2f}%\n")
print("Classification Report:")
print(classification_report(y_test, predictions))

```

**Classification Report:**

```text
Baseline Model Accuracy: 95.52%

Classification Report:
              precision    recall  f1-score   support

           0       0.93      0.77      0.84       102
           1       0.96      0.99      0.97       546

    accuracy                           0.96       648
   macro avg       0.94      0.88      0.91       648
weighted avg       0.95      0.96      0.95       648

```

---

### 4. MLflow Tracking & Stage Validation

* **Backend:** MLflow Tracking Server (`http://localhost:5000`)
* **Registration Policy:** Automatic promotion to `Staging` if test set accuracy exceeds 90.00%.

```python
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("Manufacturing_Defects_Prediction")

with mlflow.start_run() as run:
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("random_state", 42)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.sklearn.log_model(model, "random_forest_model")

# Threshold Validation Gate
THRESHOLD = 0.90
if accuracy >= THRESHOLD:
    client = MlflowClient()
    model_uri = f"runs:/{run.info.run_id}/random_forest_model"
    model_name = "Manufacturing_Defects_RF_Model"
    result = mlflow.register_model(model_uri, model_name)
    client.transition_model_version_stage(
        name=model_name,
        version=result.version,
        stage="Staging"
    )
    print(f"Model registered as: {result.name} (Version {result.version}) -> Staging")

```

---

### 5. Airflow Pipeline Automation

The scheduled DAG (`manufacturing_quality_pipeline`) runs on Apache Airflow (`http://localhost:8085`). It isolates tasks, verifies data presence in the container volume, executes scaling transformations, logs run metrics to MLflow, and registers qualifying model artifacts autonomously.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

def train_and_log_model():
    data_path = "/opt/airflow/notebooks/manufacturing_defects.csv"
    df = pd.read_csv(data_path) 
    
    X = df.drop('DefectStatus', axis=1)
    y = df['DefectStatus']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("Manufacturing_Defects_Prediction")

    with mlflow.start_run() as run:
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train_scaled, y_train)
        accuracy = accuracy_score(y_test, model.predict(X_test_scaled))
        
        mlflow.log_metric("accuracy", accuracy)
        mlflow.sklearn.log_model(model, "random_forest_model")

        if accuracy >= 0.90:
            client = MlflowClient()
            model_uri = f"runs:/{run.info.run_id}/random_forest_model"
            result = mlflow.register_model(model_uri, "Manufacturing_Defects_RF_Model")
            client.transition_model_version_stage(
                name="Manufacturing_Defects_RF_Model",
                version=result.version,
                stage="Staging"
            )

default_args = {
    'owner': 'dataops_admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'manufacturing_quality_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Automated ML training pipeline for manufacturing defects'
) as dag:

    run_pipeline = PythonOperator(
        task_id='train_evaluate_register_model',
        python_callable=train_and_log_model
    )

```

---

### 6. Inference Service & Observability

* **API Framework:** FastAPI (`http://localhost:8000/docs`)
* **Endpoint:** `POST /predict`
* **Metrics Scraper:** Prometheus (`http://localhost:9091`) pulling from `/metrics`
* **Visualization:** Grafana (`http://localhost:3000`)

```python
from fastapi import FastAPI
from prometheus_client import Counter, make_asgi_app
from pydantic import BaseModel
import mlflow.pyfunc
import pandas as pd

app = FastAPI(title="DataOps/MLOps Demo Model API")
PREDICTION_COUNTER = Counter("predictions_total", "Total predictions served")

app.mount("/metrics", make_asgi_app())

mlflow.set_tracking_uri("http://mlflow:5000")
try:
    model = mlflow.pyfunc.load_model(model_uri="models:/Manufacturing_Defects_RF_Model/1")
except Exception as e:
    model = None

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
    df = pd.DataFrame([req.features])
    prediction = model.predict(df)
    return {"defect_prediction": int(prediction[0])}

```

#### Monitored Grafana Panels

* `predictions_total`: Gauge monitoring inference volume and throughput surges.


* `process_resident_memory_bytes`: Physical RAM utilization tracking across the FastAPI runtime and Prometheus scraper.


* `process_cpu_seconds_total`: Computational run time across system/user space.


* `python_gc_objects_collected_total`: Runtime Python garbage collection metrics across generations 0, 1, and 2.



---

## Execution Guide

### 1. Prerequisites

* Docker Engine & Docker Compose installed
* Minimum 8 GB RAM allocated to Docker runtime

### 2. Start Services

```bash
# Clone the repository
git clone https://github.com/<your-username>/manufacturing-quality-prediction.git
cd manufacturing-quality-prediction

# Launch the complete multi-container stack
docker compose --profile full up -d

```

### 3. Service Dashboard Endpoints

| Component | Local URL | Default Credentials |
| --- | --- | --- |
| **Airflow Orchestrator** | `http://localhost:8085` | `admin` / `admin` |
| **Jupyter Notebook** | `http://localhost:8889` | Token: `dataopsmlops2026` |
| **MLflow Registry** | `http://localhost:5000` | None |
| **FastAPI Swagger Docs** | `http://localhost:8000/docs` | None |
| **Prometheus Telemetry** | `http://localhost:9091` | None |
| **Grafana Dashboards** | `http://localhost:3000` | `admin` / `admin` |

### 4. Stop Services

```bash
docker compose --profile full down

```

---

## References

* [Predicting Manufacturing Defects Dataset (Kaggle)](https://www.kaggle.com/datasets/rabieelkharoua/predicting-manufacturing-defects-dataset)
* [Python 3 Standard Library Documentation](https://docs.python.org/3/library/index.html)
* [GUVI Geek Networks & Jain University Capstone Curriculum](https://www.guvi.in)
