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
    dag_id='cafef_etl_daily_job',
    # tz="UTC+7",
    default_args=default_args,
    description='Python Operator',
    start_date=datetime(2022, 11, 9),
    schedule_interval='0 11 * * *'
) as dag:
    task1= PythonOperator(
        task_id='cafef_etl_daily_history_lookup',
        python_callable= service.etl_daily_history_lookup,
        op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.TODAY,'today':True}
    )

    task2 = PythonOperator(
        task_id = 'cafef_etl_daily_setting_command',
        python_callable=service.etl_daily_setting_command,
        op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.TODAY,'today':True}
    )

    task3 = PythonOperator(
        task_id = 'cafef_etl_daily_foreign_transactions',
        python_callable=service.etl_daily_foreign_transactions,
        op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.TODAY,'today':True}
    )
    task4 = PythonOperator(
        task_id = 'cafef_etl_daily_stock_price',
        python_callable=service.etl_daily_stock_price,
        op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.TODAY,'today':True}
    )

    # [task1, task2, task3, task4]
    [task1>>task2>>task3>>task4]
    