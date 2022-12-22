
import boto3
from botocore.client import Config
from datetime import date

s3 = boto3.resource('s3',
                    endpoint_url='http://127.0.0.1:9000',
                    aws_access_key_id='khanhlq10',
                    aws_secret_access_key='khanhlq10',
                    config=Config(signature_version='s3v4')
                    )
obj = s3.Bucket('bucket-test').Object("gold/daily_setting_command.parquet").get()
print(obj)

# today = date.today()
# current_date = today.strftime("%Y - %m - %d")

# for file in os.scandir('dags/logs'):
#         if(file.is_file()):
#             file_name = file.name
#             s3.Bucket('bucket-test').upload_file(file.path, 'raw/batch/crawl_stock_data/' +  file_name[:-4] + "/" +current_date + '.csv')
#             print("File uploaded")
#         else:
#             print("File does not exist")
# print(date)