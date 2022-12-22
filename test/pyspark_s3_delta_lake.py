from delta.tables import *

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:2.2.0 pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.sql.functions import md5

"""------------------------
Set up and Connect"""

spark = SparkSession.builder \
        .appName("S3 - Read") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

access_id = 'khanhlq10'
access_key = 'khanhlq10'

sc=spark.sparkContext

hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set('fs.s3a.endpoint', 'http://172.16.1.29:9000')
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)

"""-----------------------
Read parquet file from Silver Layer
-----------------------"""
df_pq = spark.read.parquet("s3a://bucket-test/silver/daily_setting_command.parquet")

"""-----------------------
Transform parquet file
-----------------------"""
columns = ['market','ma','ngay', 'du_mua', 'du_ban', 'gia', 'gia_tri_thay_doi'
, 'phan_tram_thay_doi', 'so_lenh_dat_mua', 'khoi_luong_dat_mua', 'kl_trung_binh_1_lenh_mua', 'so_lenh_dat_ban', 'khoi_luong_dat_ban', 'kl_trung_binh_1_lenh_ban', 'chenh_lech_mua_ban']
df_transform = df_pq.toDF(*columns)
# # Hash
df_transform = df_transform.withColumns({'ma_key':md5(df_transform['ma']),'market_key':md5(df_transform['market'])})
# df_transform.show(5)
"""-----------------------
Write parquet file to Gold Layer
-----------------------"""
# df_transform.write.mode("overwrite").format("delta").save("s3a://bucket-test/gold/delta_table/daily_setting_command")
spark.read.format("delta").load("s3a://bucket-test/gold/delta_table/daily_setting_command").show()