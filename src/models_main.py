import pandas as pd
import os
import datetime
from google.cloud import bigquery
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
from fastapi import Request
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from typing import List, Dict, Type
from pydantic import BaseModel, constr
from starlette.exceptions import HTTPException as StarletteHTTPException
from src.utils.config import settings
from src.database.upload_data_to_db import( upload_dataframe_to_bd)
from src.database.get_data import ( load_bd_table, joinned_validation)
from src.backups.backup_Avro import backup_table_to_avro
from src.models_utils import ( validate_batch_size, remove_duplicates_items)
from src.models_get_json import get_data_bd_json 
from src.data_analysis.data_analysis_html import (hires_by_quarter_html, departments_above_average_html) 

# Inicialización
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"
config = settings['bd']
project_id = config.project_id
dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)

# RestApi Config
config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

# logs errores 
log_path = "logs/logs_data_errorPost.txt"
os.makedirs(os.path.dirname(log_path), exist_ok=True)

#Modelos de las tablas
short_string = constr(max_length=50)
class department(BaseModel):
    id: int
    department: short_string

class job(BaseModel):
    id: int
    job: short_string

class hired_employee(BaseModel):
    id: int
    name: short_string
    datetime: short_string
    department_id: int
    job_id: int

#Diccionario de clases permitidas
TABLE_MODELS: Dict[str, Type[BaseModel]] = {
    "departments": department,
    "jobs": job,
    "hired_employees": hired_employee
}

allowed_tables = TABLE_MODELS.keys()

@app.get("/")
async def root():
    return {
        "message": "REST API app initialized",
        "available_endpoints": {
            "/data/job": "Endpoint to access job data",
            "/data/department": "Endpoint to access department data",
            "/data/hired_employees": "Endpoint to access hired employees data",
            "/data/{table_name}/add": "POST endpoint to upload data to the specified table (job, department, or hired_employees)"
        }
    }
# Manejo de errores para rutas no encontradas
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return HTMLResponse(content="<h1>404 - Page Not Found</h1><p>The requested URL was not found on the server.</p>", status_code=404)
    elif exc.status_code == 405:
        return HTMLResponse(content="<h1>405 - Method Not Allowed</h1><p>The method is not allowed for the requested URL.</p>", status_code=405)
    return HTMLResponse(content=f"<h1>HTTP Error {exc.status_code}</h1>", status_code=exc.status_code)

# Manejo de errores de validación 422: validación del request y guarda en logs errores
@app.exception_handler(RequestValidationError)
async def custom_validation_exception_handler(request: Request, exc: RequestValidationError):
    log_path = "logs/logs_data_errorPost.txt"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    try:
        body = await request.body()
        parsed_body = body.decode("utf-8")
    except Exception as e:
        parsed_body = "Could not decode body."

    with open(log_path, "a", encoding="utf-8") as log:
        log.write("\n Validation Error \n")
        log.write(f"Timestamp: {datetime.datetime.now()}\n")
        log.write(f"Path: {request.url.path}\n")
        log.write(f"Validation errors: {exc.errors()}\n")
        log.write(f"Invalid body:\n{parsed_body}\n")

    first_error = exc.errors()[0]["msg"]
    return PlainTextResponse(content=first_error, status_code=422)


def fetch_data_route(model: Type[BaseModel], table_name: str, route_path: str):
    @app.get(route_path, response_model=List[model])
    async def get_data():
        try:
            data = get_data_bd_json(table_name, client, project_id, dataset_id)
            return data
        except Exception as e:
            return HTMLResponse(content=f"<h1>Error al obtener los datos:</h1><pre>{str(e)}</pre>", status_code=500)


def post_data_route(model: Type[BaseModel], table_name: str, route_path_post:str):
    @app.post(route_path_post)
    async def insert_data(items: List[model]):  
        try:
            batch_validation_error = validate_batch_size(items)
            if batch_validation_error:
                return batch_validation_error

            items = remove_duplicates_items(items)
            items = [item.dict() for item in items]
            df = pd.DataFrame(items)
            print('items',df)
            upload_dataframe_to_bd(df, table_name, config.project_id, config.dataset_id)
            backup_table_to_avro(table_name, config.project_id, config.dataset_id)
            return items
        
        except Exception as e:
            return HTMLResponse(content=f"<h1>Error:</h1><pre>{str(e)}</pre>", status_code=500)


for table_name, model in TABLE_MODELS.items():
    route_path = f"/data/{table_name}"
    fetch_data_route(model, table_name, route_path)

    route_path_post = f"/data/{table_name}/add"
    post_data_route(model, table_name,route_path_post)
    

df =  load_bd_table('joinned_table', dataset_id)
df_joinned = joinned_validation(df)

@app.get("/data/hires-by-quarter", response_class=HTMLResponse)
async def table_hires_by_quarter():
    return hires_by_quarter_html(df_joinned)


@app.get("/data/departments-above-avg", response_class=HTMLResponse)
async def table_departments_average():
    return departments_above_average_html(df_joinned) 

