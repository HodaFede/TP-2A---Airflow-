from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extraction():
    print("Extraction des données météo...")

def transformation():
    print("Transformation des données...")

def chargement():
    print("Chargement des données en base...")

with DAG(
    dag_id="meteo_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tp", "airflow"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extraction
    )

    task_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transformation
    )

    task_load = PythonOperator(
        task_id="load_data",
        python_callable=chargement
    )

    task_extract >> task_transform >> task_load
