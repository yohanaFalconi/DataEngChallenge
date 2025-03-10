from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exception_handlers import http_exception_handler
from google.cloud import bigquery
from src.config import settings
import os
from utils.bd_utils import (
    load_bq_table,
)
# Configuración del entorno
config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

# Inicializar cliente de BigQuery
client = bigquery.Client()

# Custom handler for 404 Not Found
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return HTMLResponse(content="<h1>Page Not Found</h1>", status_code=404)
    return await http_exception_handler(request, exc)

# Endpoint raíz
@app.get("/")
async def root():
    return {"message": "FastAPI app initialized"}


# Nuevo endpoint para leer datos de BigQuery
@app.get("/data")
async def get_data():
    try:
        query = """
            SELECT * FROM `your_project_id.your_dataset.your_table`
            LIMIT 100
        """
        query_job = client.query(query)
        results = query_job.result()

        # Convertir los resultados a lista de diccionarios
        data = [dict(row.items()) for row in results]

        return JSONResponse(content={"data": data}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
