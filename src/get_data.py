import os
import time
import pandas as pd
from google.cloud import bigquery
from src.utils.clean_utils import (
    validate_and_complete_headers,
    format_datetime,
    trim_strings,
    remove_special_chars,
    drop_na
)

# Inicialización
# client = bigquery.Client.from_service_account_json(r".\src\bigquery-credencial.json")

# # Nombre del Proyecto y dataset
# project_id = "airy-boulevard-453221-c6"
# dataset_id = "dataset_jobs"

# Carga de data
def load_bq_table(table_name):
    try:
        # sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        # query_job = client.query(sql)
        # results = query_job.result() 
        results = pd.read_csv(f"./data_csv/{table_name}.csv",sep=",", header=None)
        # return pd.DataFrame([dict(row) for row in results])
        return results

    except Exception as e:
        print(f"Error: Failed to load table '{table_name}': {e}")
        return pd.DataFrame()  


# Creación de Logs
log_path = "logs/logs_validation_errors.txt"
os.makedirs("logs", exist_ok=True)


def hired_employees_validation(df):
    try:
        df = validate_and_complete_headers(df, ["id", "name", "datetime", "department_id", "job_id"], log_path)
        df = format_datetime(df, "datetime", log_path)
        df = trim_strings(df)
        df = remove_special_chars(df)
        df = drop_na(df)
        # print(df.head(5))
        # print(df.info())

        return df
    except Exception as e:
        print(f"Error:{e}")
        return pd.DataFrame()  


def department_validation(df):
    try:
        df = validate_and_complete_headers(df, ["id", "department"], log_path)
        df = trim_strings(df)
        df = remove_special_chars(df)
        df = drop_na(df)
        # print(df.head(5))
        # print(df.info())
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  

def job_validation(df):
    try:
        df = validate_and_complete_headers(df, ["id", "job"], log_path)
        df = trim_strings(df)
        df = remove_special_chars(df)
        df = drop_na(df)
        # print(df.info())
        # print(df.head(5))
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  


def get_validated_data():
    # Set dataframes 
    try:
        df_departments = load_bq_table("departments_")
        df_jobs = load_bq_table("jobs_")
        df_hired_employees = load_bq_table("hired_employees_")
    except Exception as e:
        print(f"Error: {e}")
        return

    # Validaciones individuales
    try:
        df_departments = department_validation(df_departments)
        df_jobs = job_validation(df_jobs)
        df_hired_employees = hired_employees_validation(df_hired_employees)
    except Exception as e:
        print(f"Error: {e}")

    return df_departments, df_jobs, df_hired_employees



if __name__ == "__main__":
    get_validated_data()
