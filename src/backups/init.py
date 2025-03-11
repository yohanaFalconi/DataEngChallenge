import os
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler
from google.cloud import bigquery
from src.utils.config import settings
from src.backups.restore import (
    read_avro_file_df,
    generate_restore_job_config_from_backup
)


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"
config = settings['bd']

project_id = config.project_id
dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)

def main(project_id, dataset_id, table_name, backup_path):
    try:
        df = read_avro_file_df(backup_path)
        print(df)
        if df.empty:
            print("El archivo AVRO está vacío o falló al leer.")
            return
        
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = generate_restore_job_config_from_backup(table_name, backup_path)
        
        with open(backup_path, "rb") as avro_file:
            load_job = client.load_table_from_file(
                avro_file,
                table_id,
                job_config=job_config
            )
        load_job.result()

        print(f"Tabla '{table_name}' restaurada exitosamente desde backup")

    except Exception as e:
        print(f"Error al restaurar la tabla {table_name}: {e}")

main(
    project_id,
    dataset_id,
    table_name="departments",
    backup_path="backups/departments.avro"
)

# table_name="hired_employees"
