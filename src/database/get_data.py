import os
import time
import pandas as pd
from google.cloud import bigquery
from src.config import settings
from src.utils.clean_utils import (
    validate_and_complete_headers,
    format_datetime,
    trim_strings,
    remove_special_chars,
    drop_na
)

# Inicialización
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"

config = settings['bd']
project_id = config.project_id
dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)

# Carga de data
def load_bd_table(table_name):
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
# def load_bd_table(table_name):
#     try:
#         sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
#         query_job = client.query(sql)
#         results = query_job.result() 
#         return results
#     except Exception as e:
#         print(f"Error: Failed to load table '{table_name}': {e}")
#         return pd.DataFrame()  
    

    
# Carga de data
def load_bd_table_csv(table_name):
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


# def hired_employees_validation(df):
#     try:
#         df = validate_and_complete_headers(df, ["id", "name", "datetime", "department_id", "job_id"], log_path)
#         df = format_datetime(df, "datetime", log_path)
#         df = trim_strings(df)
#         df = remove_special_chars(df)
#         df = drop_na(df)
#         # print(df.head(5))
#         # print(df.info())

#         return df
#     except Exception as e:
#         print(f"Error:{e}")
#         return pd.DataFrame()

# Limpieza de la tabla hired_employees
def hired_employees_validation(df):
    try:
        required_columns = ["id", "name", "datetime", "department_id", "job_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            df = validate_and_complete_headers(df, required_columns, log_path)
        
        df = format_datetime(df, "datetime", log_path)
        df = trim_strings(df)
        df = remove_special_chars(df)
        df = drop_na(df)

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

        df = trim_strings(df)
        df = remove_special_chars(df)
        df = drop_na(df)

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

        df = trim_strings(df)
        df = remove_special_chars(df)
        df = drop_na(df)

        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()
     

def get_validated_data():
    try:
        df_departments = load_bd_table_csv("departments_")
        df_jobs = load_bd_table_csv("jobs_")
        df_hired_employees = load_bd_table_csv("hired_employees_")
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
