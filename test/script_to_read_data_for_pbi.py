import pandas as pd

df = pd.read_parquet(
    "s3://bucket-test/gold/fact/daily_setting_command.parquet",
    storage_options={
        "key": 'khanhlq10',
        "secret": 'khanhlq10',
        "client_kwargs": {"endpoint_url": "http://172.16.1.29:9000"}
    }
)