from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator


default_args ={
    'owner': 'khanhlq',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def hello(name,age):
    print(f"Hello World! My name is {name},"
            f" and I am {age} years old!")

with DAG(
    dag_id='python_operator_v2',
    default_args=default_args,
    description='Python Operator',
    start_date=datetime(2022, 10, 20),
    schedule_interval='@daily'
) as dag:
    task1= PythonOperator(
        task_id='Hello',
        python_callable=hello,
        op_kwargs={'name': 'Khanh', 'age': 23}
    )

    task1