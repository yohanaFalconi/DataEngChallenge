from fastapi import FastAPI
from src.config import settings
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.exception_handlers import http_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException



config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)



# Custom handler for 404 Not Found
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return HTMLResponse(content="<h1>Page Not Found</h1>", status_code=404)
    return await http_exception_handler(request, exc)


# Root endpoint
@app.get("/")
async def root():
    return {"message": "FastAPI app initialized"}