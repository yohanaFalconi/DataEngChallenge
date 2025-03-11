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
from src.database.upload_data_to_db import( upload_dataframe_to_bq)
from src.database.get_data import ( load_bd_table, joinned_validation)
from src.backups.backup_Avro import backup_table_to_avro
from src.models_utils import ( validate_batch_size, remove_duplicates_items)
from src.models_get_json import get_data_bd_json 
from src.data_analysis import (get_hires_by_quarter, get_departments_above_avg_hires) 

# Inicialización
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r".\src\bigquery-credencial_2.json"

config = settings['bd']
project_id = config.project_id
dataset_id = config.dataset_id
client = bigquery.Client(project=project_id)

df =  load_bd_table('joinned_table', dataset_id)
df = joinned_validation(df)
get_hires_by_quarter(df)
get_departments_above_avg_hires(df)

# RestApi Config
config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

# BD Config
short_string = constr(max_length=50)
config = settings['bd']

# logs errores 
log_path = "logs/logs_data_errorPost.txt"
os.makedirs(os.path.dirname(log_path), exist_ok=True)

#Modelos de las tablas
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

# allowed_tables = {"departments", "jobs", "hired_employees"}
allowed_tables = TABLE_MODELS.keys()


@app.get("/")
async def root():
    return {"message": "FastAPI app initialized"}

# Manejo de errores para rutas no encontradas
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return HTMLResponse(content="<h1>Page Not Found</h1>", status_code=404)
    return HTMLResponse(content=f"<h1>HTTP Error {exc.status_code}</h1>", status_code=exc.status_code)

# Manejo de errores de validación 422 
@app.exception_handler(RequestValidationError)
async def custom_validation_exception_handler(request: Request, exc: RequestValidationError):
    first_error = exc.errors()[0]["msg"]
    return PlainTextResponse(content=first_error, status_code=422)


# Maneja errores de validación del request y guarda en logs errores
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


def get_route(model: Type[BaseModel], table_name: str, route_path: str):
    @app.get(route_path, response_model=List[model])
    async def get_data():
        try:
            data = get_data_bd_json(table_name)
            return data
        except Exception as e:
            return HTMLResponse(content=f"<h1>Error al obtener los datos:</h1><pre>{str(e)}</pre>", status_code=500)


def create_post_route(model: Type[BaseModel], table_name: str, route_path_post:str):
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
            upload_dataframe_to_bq(df, table_name, config.project_id, config.dataset_id)
            backup_table_to_avro(table_name, config.project_id, config.dataset_id)
            return items
        
        except Exception as e:
            return HTMLResponse(content=f"<h1>Error:</h1><pre>{str(e)}</pre>", status_code=500)


for table_name, model in TABLE_MODELS.items():

    route_path = f"/data/{table_name}"
    get_route(model, table_name, route_path)

    
    route_path_post = f"/data/{table_name}/add"
    create_post_route(model, table_name,route_path_post)
    

df =  load_bd_table('joinned_table', dataset_id)
df_joinned = joinned_validation(df)
get_hires_by_quarter(df)
# get_departments_above_avg_hires(df)


@app.get("/data/hires-by-quarter", response_class=HTMLResponse)
def hires_by_quarter_html():
    try:
        df_result = get_hires_by_quarter(df_joinned)

        # Convertimos a HTML con estilos básicos
        html_table = df_result.to_html(index=False, border=1, justify="center", classes="dataframe")

        # Envolvemos en un HTML simple
        html_content = f"""
        <html>
            <head>
                <title>Hires by Quarter</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        padding: 40px;
                        background-color: #f9f9f9;
                    }}
                    table.dataframe {{
                        width: 80%;
                        margin: auto;
                        border-collapse: collapse;
                        border: 1px solid #ccc;
                    }}
                    table.dataframe th, table.dataframe td {{
                        border: 1px solid #ccc;
                        padding: 8px 12px;
                        text-align: center;
                    }}
                    table.dataframe th {{
                        background-color: #eee;
                    }}
                </style>
            </head>
            <body>
                <h2 style="text-align:center;">Hires by Quarter</h2>
                {html_table}
            </body>
        </html>
        """
        return HTMLResponse(content=html_content)
    except Exception as e:
        return HTMLResponse(content=f"<h3>Error: {e}</h3>", status_code=500)

