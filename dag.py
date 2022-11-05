from datetime import timedelta
from airflow import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import extract_tweets

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

run_etl = PythonOperator(
    task_id = 'Extract_tweets_task',
    python_callable = extract_tweets,
    dag = dag
)

run_etl
