from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os
from dotenv import load_dotenv

load_dotenv()

def run_bronze_ipca():
    script_path = os.getenv('script_bronze_ipca_433_path')
    print(script_path)
    subprocess.run(["python", script_path], check=True)

def run_silver_cepea():
    script_path = os.getenv('script_silver_cepea_path')
    subprocess.run(["python", script_path], check=True)

def run_gold_cepea():
    script_path = os.getenv('script_gold_cepea_path')
    subprocess.run(["python", script_path], check=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0    
}

with DAG(
    dag_id='dag_boi_gordo',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,  
    catchup=False,
) as dag:
    
    task_bronze_ipca = PythonOperator(
        task_id='run_bronze_ipca',
        python_callable=run_bronze_ipca
    )
    
    task_silver_cepea = PythonOperator(
        task_id='run_silver_cepea',
        python_callable=run_silver_cepea
    )
    
    task_gold_cepea = PythonOperator(
        task_id='run_gold_cepea',
        python_callable=run_gold_cepea
    )

    task_bronze_ipca >> task_silver_cepea >> task_gold_cepea
