from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exception_handlers import http_exception_handler
from google.cloud import bigquery
from src.config import settings
import os
import pandas as pd
from src.utils.bd_utils import (
    load_bq_table,
    get_connection,
    load_bq_table_JSON
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"
config = settings['bd']
project_id = config.project_id
dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)

#Inicialización
def get_dataframe(table_name):
    try:
        get_connection(project_id)
        data = load_bq_table_JSON(table_name, client, project_id, dataset_id)
        
        return JSONResponse(content={"data": data}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# get_dataframe('departments')
# get_dataframe('jobs')
get_dataframe('hired_employees')