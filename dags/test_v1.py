
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
    start_date=datetime(2022, 10, 27),
    schedule_interval='*/10 * * * *',
    # catchup=False,
    end_date=datetime(2022, 10, 28)
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