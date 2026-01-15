from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Définition du DAG
with DAG(
    dag_id='airsoft_scraper_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,  # <--- C'est ici la correction pour Airflow 3
    catchup=False
) as dag:

    # Étape 1 : Lancer le scraping
    task_scrape = BashOperator(
        task_id='run_airsoft_scraper',
        bash_command='python3 /opt/airflow/scripts/scraper.py'
    )

    # Étape 2 : Traiter les données avec Spark
    task_spark = BashOperator(
        task_id='process_with_spark',
        bash_command='python3 /opt/airflow/scripts/spark_process.py'
    )

    # L'ordre : on scrape d'abord, on traite ensuite
    task_scrape >> task_spark
