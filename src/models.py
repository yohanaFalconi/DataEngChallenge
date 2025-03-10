from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi import Body
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
from fastapi import Request


from src.config import settings
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.exception_handlers import http_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException
from src.models_get_json import( get_data_bd_json )
import json
from typing import List, Dict, Type
from pydantic import BaseModel, ValidationError
from pydantic import BaseModel, constr

config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

short_string = constr(max_length=50)

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


''''''
def create_post_route(model: Type[BaseModel], table_name: str, route_path_post:str):

    @app.post(route_path_post)
    async def insert_data(items: List[model]):  
        try:
            print('items',items)
            rows = [item.dict() for item in items]

            return rows
        except Exception as e:
            return HTMLResponse(content=f"<h1>Error:</h1><pre>{str(e)}</pre>", status_code=500)
''''''

# items [Job(id=1, job='Developer'), Job(id=2, job='Data Anassslyst')]

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
    
  

