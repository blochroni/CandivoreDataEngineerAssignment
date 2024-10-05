from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from datetime import datetime
from pipeline import run

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'influencer_campaign_data_pipeline',
    default_args=default_args,
    description='A simple influencer data pipeline',
    schedule_interval='@daily',
)

# Task to run the pipeline
run_pipeline = PythonOperator(
    task_id='run_pipeline',
    python_callable=run,
    dag=dag,
)

run_pipeline
