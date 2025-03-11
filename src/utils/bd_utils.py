import pandas as pd
from google.cloud import bigquery
from src.config import settings
import os
import time
import uuid
from src.utils.clean_utils import (
    normalize_datetime_fields
)

#Agrega una columna con UUID únicos
def add_uuid_column(df, column_name='uuid'):
    if column_name not in df.columns:
        df[column_name] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

#Permite subir los dataframes a la bd
def load_bq_table(table_name,client,project_id,dataset_id):
    try:
        sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
        query_job = client.query(sql)
        results = query_job.result() 
        return pd.DataFrame([dict(row) for row in results])
    except Exception as e:
        print(f"Error: Failed to load table '{table_name}': {e}")
        return pd.DataFrame()  
    
#Permite subir los json a la bd
def load_bq_table_JSON(table_name,client,project_id,dataset_id):
    try:
        sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"

        query_job = client.query(sql)
        results = query_job.result() 
        json_data = [dict(row) for row in results]
        json_data = normalize_datetime_fields(json_data)

        print(f"Tabla '{table_name}' - Registros: {len(json_data)}")
        print(json_data)
        return json_data

    except Exception as e:
        print(f"Error: Failed to load table '{table_name}': {e}")
        return pd.DataFrame()  
    
# Genera la conexión a la bd
def get_connection(project_id):
    try:
        client = bigquery.Client(project=project_id)
        datasets = list(client.list_datasets())
        if datasets:
            print("Conexión exitosa. Datasets encontrados:")
            for dataset in datasets:
                print(f"- {dataset.dataset_id}")
    except Exception as e:
        print(f"Error al conectar a BigQuery: {e}")