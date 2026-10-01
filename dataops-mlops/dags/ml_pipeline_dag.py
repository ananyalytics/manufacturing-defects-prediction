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
    # 1. Load Data (Path mapped inside the Airflow Docker container)
    data_path = "/opt/airflow/notebooks/manufacturing_defects.csv"
    df = pd.read_csv(data_path) 
    
    X = df.drop('DefectStatus', axis=1)
    y = df['DefectStatus']

    # 2. Transformation
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # 3. Model Training & MLflow Tracking
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("Manufacturing_Defects_Prediction")

    with mlflow.start_run() as run:
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train_scaled, y_train)
        
        predictions = model.predict(X_test_scaled)
        accuracy = accuracy_score(y_test, predictions)
        
        mlflow.log_metric("accuracy", accuracy)
        mlflow.sklearn.log_model(model, "random_forest_model")
        print(f"Model successfully trained. Accuracy: {accuracy * 100:.2f}%")

        # 4. Validation & Registration
        if accuracy >= 0.90:
            client = MlflowClient()
            model_uri = f"runs:/{run.info.run_id}/random_forest_model"
            result = mlflow.register_model(model_uri, "Manufacturing_Defects_RF_Model")
            
            client.transition_model_version_stage(
                name="Manufacturing_Defects_RF_Model", 
                version=result.version, 
                stage="Staging"
            )
            print(f"Validation Passed! Model registered as Version {result.version}")

# 5. Define Airflow DAG parameters
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

    run_ml_pipeline = PythonOperator(
        task_id='train_evaluate_register_model',
        python_callable=train_and_log_model
    )

    # If we had multiple tasks (e.g., ingest -> transform -> train), 
    # we would link them here using >> (e.g., task1 >> task2). 
    # For this capstone, everything is safely wrapped in one operator.
    run_ml_pipeline