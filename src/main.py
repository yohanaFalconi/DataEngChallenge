from fastapi import FastAPI
from src.config import settings

config_class = settings['development']
app = FastAPI(debug=config_class.DEBUG)

@app.get("/")
def root():
    return {"message": "FastAPI app initialized"}


