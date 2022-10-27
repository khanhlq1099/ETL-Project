import pandas as pd
import os

def save_dataframe_to_excel(df: pd.DataFrame, path: str):
    df.to_excel(path, index=False)


def save_dataframe_to_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)


def create_nested_directory(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)