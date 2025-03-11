from decouple import config

class bd_config():
    project_id: str = config("big_query_project_id")
    dataset_id_init: str = config("big_query_init_dataset_id")
    dataset_id: str = config("big_query_dataset_id")

class config:
    SECRET_KEY = config("SECRET_KEY")
    DEBUG = config("DEBUG", cast=bool, default=False)

class development_config(config):
    ENV = "development"

settings = {
    'development': development_config,
    'bd': bd_config
}