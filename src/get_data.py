import os
import pandas as pd
from google.cloud import bigquery
# from src.utils.clean_utils import trim_strings, remove_special_chars, drop_na, normalize_columns_names

# Inicialización
client = bigquery.Client.from_service_account_json(r".\src\bigquery-credencial.json")

# Project and dataset
project_id = "airy-boulevard-453221-c6"
dataset_id = "dataset_jobs"

# Load data
def load_bq_table(table_name):
    try:
        sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        query_job = client.query(sql)
        results = query_job.result()
        return pd.DataFrame([dict(row) for row in results])
    except Exception as e:
        print(f"Error: Failed to load table '{table_name}': {e}")
        return pd.DataFrame()  

# Set dataframes 
# df_deparments = load_bq_table("departments_")
# df_jobs = load_bq_table("jobs_")
df_hired = load_bq_table("hired_employees_")

# Validation 'hired_employees' table structure and enforce correct datetime formatting
def hired_employees_validation(df):
    expected_columns = {
        "id": int,
        "name": str,
        "datetime": str,
        "department_id": int,
        "job_id": int
    }

    os.makedirs("logs", exist_ok=True)
    log_path = "logs/logs_validation_errors.txt"

    with open(log_path, "w") as log:
        for col, expected_type in expected_columns.items():
            if col not in df.columns:
                log.write(f"ERROR: Missing column: {col}\n")
        try:
            print('df1', df.head(5))
            df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
            df["datetime"] = df["datetime"].dt.strftime('%Y-%m-%dT%H:%M:%S')  # ISO 8601 como string
        except Exception as e:
            log.write(f"DATETIME ERROR: Failed to convert datetime column: {e}\n")
    return df


hired_employees_validation(df_hired)







# trim_strings, remove_special_chars, drop_na, normalize_columns_names

# Separar válidos e inválidos
    # invalid_rows = df[df.isnull().any(axis=1)]
    # valid_rows = df.dropna()

    # # Guardar inválidos en log
    # if not invalid_rows.empty:
    #     os.makedirs("logs", exist_ok=True)
    #     invalid_rows.to_csv("logs/invalid_hired_employees.csv", index=False)
    #     print(f"[!] {len(invalid_rows)} filas inválidas guardadas en logs/invalid_hired_employees.csv")
