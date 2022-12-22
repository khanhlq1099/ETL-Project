import extract.cafef.service as service
from datetime import datetime,date
from lib.core.constants import DATA_DESTINATION_TYPE, PERIOD_TYPE
import pandas as pd
import io
import boto3
from botocore.client import Config

# def data_frame(df:pd.DataFrame):
#     return df

# print(df)

s3 = boto3.client('s3',
                    endpoint_url='http://127.0.0.1:9000',
                    aws_access_key_id='khanhlq10',
                    aws_secret_access_key='khanhlq10',
                    config=Config(signature_version='s3v4')
                    )

# df = pd.DataFrame(service.etl_daily_setting_command(data_destination_type=DATA_DESTINATION_TYPE.SQL_SERVER,period_type=PERIOD_TYPE.TODAY,business_date=datetime(2022,12,12)))
# print(pd.DataFrame(service.etl_daily_setting_command(data_destination_type=DATA_DESTINATION_TYPE.SQL_SERVER,period_type=PERIOD_TYPE.TODAY,business_date=date(2022,12,13))))
# today = date.today()
# current_date = today.strftime("%Y_%m_%d")
# year = today.strftime("%Y")
# year_month = today.strftime("%Y_%m")
# with io.StringIO() as csv_buffer:
#     df.to_csv(csv_buffer,index=False)
#     response = s3.put_object(Bucket='bucket-test',Key = "bronze/cafef/daily_setting_command/"+year + "/" +year_month +"/" +"2022_12_12"+ ".csv",Body=csv_buffer.getvalue())

#     status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

#     if status == 200:
#         print(f"Successful S3 put_object response. Status - {status}")
#     else:
#         print(f"Unsuccessful S3 put_object response. Status - {status}")

service.etl_daily_setting_command(data_destination_type=DATA_DESTINATION_TYPE.SQL_SERVER,period_type=PERIOD_TYPE.TODAY,business_date=date(2022,12,16))