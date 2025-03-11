import json
import pandas as pd
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
from fastapi import Request
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from typing import List, Dict, Type
from pydantic import BaseModel
from pydantic import BaseModel, constr

from starlette.exceptions import HTTPException as StarletteHTTPException
from src.utils.config import settings
from src.models_get_json import( get_data_bd_json )
from src.database.upload_data_to_db import( upload_dataframe_to_bq)
from src.backups.backup_Avro import backup_table_to_avro

config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

short_string = constr(max_length=50)
config = settings['bd']

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

# Permite insertar 1-1000 registros en una sola petición
def validate_batch_size(items: list, min_size: int = 1, max_size: int = 1000):
    if not (min_size < len(items) <= max_size):
        return HTMLResponse(
            content=f"<h1>Error:</h1><pre>Batch size must be between {min_size} and {max_size}. Received: {len(items)}</pre>",
            status_code=400
        )
    return None 

# Elimina duplicados en los items que ingresan 
def remove_duplicates_items(items: List[BaseModel]) -> List[BaseModel]:
    seen = set()
    unique_items = []    
    for item in items:
        item_tuple = tuple(sorted(item.model_dump().items()))
        if item_tuple not in seen:
            seen.add(item_tuple)
            unique_items.append(item)
    
    return unique_items


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

    @app.get(route_path, response_model=List[model])
    async def get_data(model=model, table_name=table_name):
        try:
            data = get_data_bd_json(table_name)
            return data
        except Exception as e:
            return HTMLResponse(content=f"<h1>Error:</h1><pre>{str(e)}</pre>", status_code=500)
    
    route_path_post = f"/data/{table_name}/add"
    create_post_route(model, table_name,route_path_post)
    


