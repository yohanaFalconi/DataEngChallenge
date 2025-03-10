from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.config import settings
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.exception_handlers import http_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException
from src.models_get_json import( get_data_bd_json )
import json

# get_data_bd_json('departments')
# get_dataframe('jobs')
# get_dataframe('hired_employees')
config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)


# Custom handler for 404 Not Found
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return HTMLResponse(content="<h1>Page Not Found</h1>", status_code=404)
    return await http_exception_handler(request, exc)


@app.get("/")
async def root():
    return {"message": "FastAPI app initialized"}

ALLOWED_TABLES = {"departments", "jobs", "hired_employees"}

@app.get("/data/{table_name}")
async def get_data(table_name: str):  
    try:
        if table_name not in ALLOWED_TABLES:
            return HTMLResponse(
                content=f"<h1>Error:</h1><pre>Tabla '{table_name}' no permitida.</pre>",
                status_code=400
            )

        data = get_data_bd_json(table_name) 
        return data
    except Exception as e:
        return HTMLResponse(content=f"<h1>Error:</h1><pre>{str(e)}</pre>", status_code=500)


    
