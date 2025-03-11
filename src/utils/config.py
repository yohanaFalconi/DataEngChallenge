from decouple import config

class bd_config():
    project_id: str = config("big_query2__project_id")
    dataset_id: str = config("big_query2_dataset_id")

class config:
    SECRET_KEY = config("SECRET_KEY")
    DEBUG = config("DEBUG", cast=bool, default=False)

class development_config(config):
    ENV = "development"

settings = {
    'development': development_config,
    'bd': bd_config
}