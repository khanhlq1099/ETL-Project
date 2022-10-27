from http import server
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

import extract.service as service
import extract.crawler as crawler

default_args ={
    'owner': 'khanhlq',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def crawl():
    crawler.extract_daily_market_history_lookup_price_data_by_bs4('HOSE',datetime(2022,10,25),datetime(2022,10,25))

def sv():
    service.etl_daily_history_lookup(data_destination_type='SQL_SERVER',period_type='TODAY',business_date=datetime(2022,10,25))

with DAG(
    dag_id='cafef',
    default_args=default_args,
    description='Python Operator',
    start_date=datetime(2022, 10, 20)
    # schedule_interval='@daily'
) as dag:
    task1= PythonOperator(
        task_id='cafef',
        # python_callable=service.etl_daily_history_lookup,
        # op_kwargs={'data_destination_type':'SQL_SERVER','period_type':'TODAY','business_date':datetime(2022,10,25)}
        # op_kwargs={'name': 'Khanh', 'age': 23}
        python_callable= crawl
    )

    task1