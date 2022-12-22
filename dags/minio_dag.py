from airflow import DAG
from datetime import datetime,timedelta,date
from airflow.operators.python import PythonOperator
from lib.core.constants import DATA_DESTINATION_TYPE, PERIOD_TYPE

import extract.cafef.service as service
import boto3
from botocore.client import Config
from datetime import date

import os 
# def upload():
#     s3 = boto3.resource('s3',
#                         endpoint_url='http://minio:9000',
#                         aws_access_key_id='khanhlq10',
#                         aws_secret_access_key='khanhlq10',
#                         config=Config(signature_version='s3v4' )
#                         )
#     today = date.today()
#     current_date = today.strftime("%Y - %m - %d")

#     # upload a file from local file system
#     # file_history_lookup_log = 'dags/logs/etl_daily_history_lookup_log.csv'
#     for file in os.scandir('dags/logs'):
#         if(file.is_file()):
#             file_name = file.name
#             s3.Bucket('bucket-test').upload_file(file.path, file_name[:-4] + "/" +current_date + '.csv')
#             print("File uploaded")
#         else:
#             print("File does not exist")

# def delete_file():
#     # file = 'dags/logs/log.csv'
#     for file in os.scandir('dags/logs'):
#         if(file.is_file()):
#             os.remove(file)
#             print("File deleted")
#         else:
#             print("File not found")

default_args ={
    'owner': 'khanhlq',
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

with DAG(
    dag_id='Upload_MinIO',
    # tz="UTC+7",
    default_args=default_args,
    description='MinIO',
    start_date=datetime(2022, 11, 9),
    schedule_interval='@once',
    # catchup=False
) as dag:
    # task1= PythonOperator(
    #     task_id='cafef_etl_daily_history_lookup',
    #     python_callable= service.etl_daily_history_lookup,
    #     op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.YESTERDAY,'today':True},
    #     # do_xcom_push=True
    # )
    # task5 = PythonOperator(
    #     task_id = 'Upload_csv_file_to_MinIO',
    #     python_callable=upload,
    # )
    # task6 = PythonOperator(
    #     task_id = 'delete_file',
    #     python_callable=delete_file,
    # )
    task2 = PythonOperator(
        task_id = 'cafef_etl_daily_setting_command',
        python_callable=service.etl_daily_setting_command,
        op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.TODAY,'business_date':date(2022,12,12)}
    )
    # task3 = PythonOperator(
    #     task_id = 'cafef_etl_daily_foreign_transactions',
    #     python_callable=service.etl_daily_foreign_transactions,
    #     op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.YESTERDAY,'today':True}
    # )
    # task4 = PythonOperator(
    #     task_id = 'cafef_etl_daily_stock_price',
    #     python_callable=service.etl_daily_stock_price,
    #     op_kwargs={'data_destination_type':DATA_DESTINATION_TYPE.SQL_SERVER,'period_type':PERIOD_TYPE.YESTERDAY,'today':True}
    # )

    # task1>>task2>>task3

    # [task1,task2,task3,task4]
    task2