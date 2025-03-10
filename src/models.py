from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi import Body
from pydantic import BaseModel, ValidationError

from src.config import settings
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.exception_handlers import http_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException
from src.models_get_json import( get_data_bd_json )
import json
from typing import List, Dict, Type
from pydantic import BaseModel

config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

#Modelos de las tablas
class Department(BaseModel):
    id: int
    department: str

class Job(BaseModel):
    id: int
    job: str

class HiredEmployee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

#Diccionario de clases permitidas
TABLE_MODELS: Dict[str, Type[BaseModel]] = {
    "departments": Department,
    "jobs": Job,
    "hired_employees": HiredEmployee
}

# ALLOWED_TABLES = {"departments", "jobs", "hired_employees"}
ALLOWED_TABLES = TABLE_MODELS.keys()

# Custom handler for 404 Not Found
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return HTMLResponse(content="<h1>Page Not Found</h1>", status_code=404)
    return await http_exception_handler(request, exc)


@app.get("/")
async def root():
    return {"message": "FastAPI app initialized"}


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
    
  

