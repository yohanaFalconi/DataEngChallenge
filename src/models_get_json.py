import os
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler
# from google.cloud import bigquery
# from src.utils.config import settings
from src.utils.bd_utils import (
    get_connection,
    fetch_table_as_json 
)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"
# config = settings['bd']
# project_id = config.project_id
# dataset_id_init = config.dataset_id_init
# dataset_id = config.dataset_id
# client = bigquery.Client(project=project_id)

#Extraer datos de la bd en json
def get_data_bd_json(table_name, client, project_id, dataset_id):
    try:
        get_connection(project_id)
        data = fetch_table_as_json (table_name, client, project_id, dataset_id)
        
        return JSONResponse(content={"data": data}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# get_data_bd_json('departments')
# get_data_bd_json('jobs')
# get_dataframe('hired_employees')
