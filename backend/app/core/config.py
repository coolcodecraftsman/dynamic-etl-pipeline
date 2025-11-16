import os


class Settings:
    PROJECT_NAME = "Dynamic ETL Pipeline"
    API_V1_PREFIX = "/api/v1"

    POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_password")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "etl_db")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "etl_postgres")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

    MONGO_URI = os.getenv("MONGO_URI", "mongodb://etl_mongo:27017")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "etl")


settings = Settings()
