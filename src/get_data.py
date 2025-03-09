import os
import pandas as pd
from google.cloud import bigquery


# Inicialización
client = bigquery.Client.from_service_account_json(r".\src\bigquery-credencial.json")

# Project and dataset
project_id = "airy-boulevard-453221-c6"
dataset_id = "dataset_jobs"

# Load data
def load_bq_table(table_name):
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
    return pd.DataFrame([dict(row) for row in client.query(sql).result()])

# df_hired = load_bq_table("departments_")
# df_hired = load_bq_table("jobs_")
df_hired = load_bq_table("hired_employees_")

print(df_hired)


# from cleaning_utils import trim_strings, clean_text, normalize_columns

# df_hired = load_bq_table("hired_employees")
# df_hired = trim_strings(df_hired)
# df_hired = clean_text(df_hired)
# df_hired = normalize_columns(df_hired)

