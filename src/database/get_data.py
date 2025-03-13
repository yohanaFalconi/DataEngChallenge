import os
import time
import pandas as pd
from google.cloud import bigquery
from src.utils.config import settings
from src.utils.bd_utils import (get_connection)
from src.utils.clean_utils import (
    validate_and_complete_headers,
    format_datetime,
    trim_strings,
    drop_rows_with_mostly_nans,
    drop_duplicate_rows
)

# Inicialización
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"

config = settings['bd']
project_id = config.project_id

dataset_id_init = config.dataset_id_init
dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)


get_connection(project_id) 
# Carga de data
def load_bd_table(table_name, dataset_id = None):
    try:
        sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        query_job = client.query(sql)
        results = query_job.result()

        rows = [dict(row) for row in results]
        df = pd.DataFrame(rows)
        return df

    except Exception as e:
        print(f"Error: Failed to load table '{table_name}': {e}")
        return pd.DataFrame()
       
    
# Carga de data
# def load_bd_table_csv(table_name):
#     try:
#         # sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
#         # query_job = client.query(sql)
#         # results = query_job.result() 
#         results = pd.read_csv(f"./data_csv/{table_name}.csv",sep=",", header=None)
#         # return pd.DataFrame([dict(row) for row in results])
#         return results

#     except Exception as e:
#         print(f"Error: Failed to load table '{table_name}': {e}")
#         return pd.DataFrame()  


# Creación de Logs
log_path="logs/logs_data_cleaning.txt"
os.makedirs(os.path.dirname(log_path), exist_ok=True)

# Limpieza de la tabla joinned
def joinned_validation(df):
    try:
        df = trim_strings(df,log_path=log_path)
        df = drop_rows_with_mostly_nans(df, min_valid=3,log_path=log_path)
        df = format_datetime(df, "datetime", log_path=log_path)
        df = drop_duplicate_rows(df, log_path=log_path)
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

# Limpieza de la tabla hired_employees
def hired_employees_validation(df):
    try:
        required_columns = ["id", "name", "datetime", "department_id", "job_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            df = validate_and_complete_headers(df, required_columns, log_path=log_path)
        df = trim_strings(df,log_path=log_path)
        df = drop_rows_with_mostly_nans(df, min_valid=3,log_path=log_path)
        df = format_datetime(df, "datetime", log_path=log_path)
        df = drop_duplicate_rows(df, log_path=log_path)
        # print(df.head(5))

        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

# Limpieza de la tabla department_validation
def department_validation(df):
    try:
        required_columns = ["id", "department"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            df = validate_and_complete_headers(df, required_columns, log_path)

        df = trim_strings(df,log_path=log_path)
        df = drop_rows_with_mostly_nans(df, min_valid=2,log_path=log_path)
        df = drop_duplicate_rows(df, log_path=log_path)
        print(df.head(5))

        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

# Limpieza de la tabla job_validation 
def job_validation(df):
    try:
        required_columns = ["id", "job"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            df = validate_and_complete_headers(df, required_columns, log_path)

        df = trim_strings(df,log_path=log_path)
        df = drop_rows_with_mostly_nans(df, min_valid=2,log_path=log_path)
        df = drop_duplicate_rows(df, log_path=log_path)

        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()
     
#Lee las tablas, devuevle tablas validas y limpias
def get_validated_data():
    try:
        df_departments = load_bd_table("departments_",  dataset_id= dataset_id_init)
        df_jobs = load_bd_table("jobs_",  dataset_id= dataset_id_init)
        df_hired_employees = load_bd_table("hired_employees_",  dataset_id= dataset_id_init)

    except Exception as e:
        print(f"Error: {e}")
        return

    try:
        df_departments = department_validation(df_departments)
        df_jobs = job_validation(df_jobs)
        df_hired_employees = hired_employees_validation(df_hired_employees)
    except Exception as e:
        print(f"Error: {e}")

    return df_departments, df_jobs, df_hired_employees


if __name__ == "__main__":
    get_validated_data()
