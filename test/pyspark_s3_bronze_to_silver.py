"""---Transform data from Bronze to Silver---"""

from datetime import date
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:3.2.2 pyspark-shell"


from pyspark.sql import SparkSession

"""------------------------
Set up and Connect"""

spark = SparkSession.builder \
        .appName("S3 - Read") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

access_id = 'khanhlq10'
access_key = 'khanhlq10'

sc=spark.sparkContext

hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set('fs.s3a.endpoint', 'http://127.0.0.1:9000')
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)
"""------------------------"""

"""------------------------
Read csv file from Bronze Layer
------------------------"""
# today = date.today()
# current_date = today.strftime("%Y_%m_%d")

# df_csv = spark.read \
#     .option('header', 'true') \
#     .format('csv') \
#     .csv('s3a://bucket-test/bronze/cafef/daily_setting_command/2022/2022_12/' + '*' +'.csv')

"""-----------------------
Create view and Select from csv file (Check data)
-----------------------"""
# df_csv.createOrReplaceTempView("Table")

# sql = spark.sql("SELECT distinct(ngay) FROM Table ORDER BY ngay")
# sql.show()

"""-----------------------
Write parquet file to Silver Layer
-----------------------"""
# df_csv.write.mode('overwrite').parquet('s3a://bucket-test/silver/daily_setting_command.parquet')

"""-----------------------
Read parquet file from Bronze Layer
-----------------------"""
df_pq = spark.read.parquet('s3a://bucket-test/silver/daily_setting_command.parquet')
# df_pq = df_pq.dropDuplicates()
"""-----------------------
Create view and Select from parquet table in Silver Layer
-----------------------"""
df_pq.createOrReplaceTempView("Table")

sql = spark.sql("SELECT * FROM Table WHERE ma = 'TCB' and market = 'VN30' and ngay = '2022-12-12' ")
sql.show()