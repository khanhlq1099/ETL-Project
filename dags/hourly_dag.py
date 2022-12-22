from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from lib.core.constants import DATA_DESTINATION_TYPE, PERIOD_TYPE

import extract.cafef.service as service


default_args ={
    'owner': 'khanhlq',
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
    dag_id='cafef_etl_hourly_job',
    # tz="UTC+7",
    default_args=default_args,
    description='Python Operator',
    start_date=datetime(2022, 11, 9),
    # schedule_interval='0 2,5,8,11 * * *'
    schedule_interval='@once',
) as dag:
    task1 = PythonOperator(
        task_id = 'cafef_etl_hourly_stock_price',
        python_callable=service.etl_hourly_stock_price,
        op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.PERIOD,'from_date':datetime(2022, 11, 21), 'to_date':datetime(2022, 11, 22)}
    )
    [task1]
    