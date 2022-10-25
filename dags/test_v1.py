from email.policy import default
from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args ={
    'owner': 'khanhlq',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='hello_world_v3',
    default_args=default_args,
    description='This is my first dag',
    start_date=datetime(2021, 7, 29, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world'
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command='echo Task 2'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo Task 3'   
    )

    task1 >> task2
    task1 >> task3