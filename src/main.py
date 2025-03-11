from fastapi import FastAPI
from src.config import settings
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from src.config import settings

config = settings['bd']
config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

