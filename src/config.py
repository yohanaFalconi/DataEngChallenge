from decouple import config

class Config:
    SECRET_KEY = config("SECRET_KEY")
    DEBUG = config("DEBUG", cast=bool, default=False)

class DevelopmentConfig(Config):
    ENV = "development"

settings = {
    'development': DevelopmentConfig
}