from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
from twitter_etl import extract_tweets
from twitter_etl import load_to_s3

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2022,11,4),
    'email':['patilrishikesh1995@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='DAG1'
)

extraction_task = PythonOperator(
    task_id = 'Extract_tweets',
    python_callable = extract_tweets,
    dag = dag
)

load_task = PythonOperator(
    task_id = 'Load_tweets',
    python_callable = load_to_s3,
    dag = dag
)

extraction_task >> load_task
