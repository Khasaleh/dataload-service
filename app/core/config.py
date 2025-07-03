from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, RedisDsn, HttpUrl, Field, model_validator
from typing import Optional

class Settings(BaseSettings):
    # Environment loading configuration
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')
    AUTH_VALIDATION_ENABLED: bool = Field(default=True, validation_alias="AUTH_VALIDATION_ENABLED")
    # General Application Settings
    PROJECT_NAME: str = "Catalog Data Load Service"
    API_PREFIX: str = "/graphql"
    ENVIRONMENT: str = Field("stage", validation_alias="ENV", alias_priority=2)
    LOG_LEVEL: str = "INFO"
    RELOAD: bool = False

    # Database Configuration
    DB_DRIVER: str = "postgresql+psycopg2"
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int = 5432
    DB_NAME: str
    DATABASE_URL: Optional[PostgresDsn] = None

    # Schema names
    CATALOG_SERVICE_SCHEMA: str = "catalog_management"
    BUSINESS_SERVICE_SCHEMA: str = "fazealbusiness"

    # Wasabi S3 Configuration
    WASABI_ENDPOINT_URL: HttpUrl
    WASABI_ACCESS_KEY: str
    WASABI_SECRET_KEY: str
    WASABI_BUCKET_NAME: str
    WASABI_REGION: Optional[str] = None

    # JWT Authentication Configuration
    JWT_SECRET: str = Field(..., validation_alias="SECRET_KEY")
    JWT_ALGORITHM: str = "HS512"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Redis Configuration
    REDIS_HOST: str = "192.168.1.245"
    REDIS_PORT: int = 6379
    REDIS_DB_ID_MAPPING: int = 1
    CELERY_BROKER_DB_NUMBER: int = 0
    CELERY_RESULT_BACKEND_DB_NUMBER: int = 0
    REDIS_SESSION_TTL_SECONDS: int = 86400

    # Redis password from Kubernetes secret or environment variable
    REDIS_PASSWORD: str = Field(..., env="REDIS_PASSWORD")  # This will be pulled from the environment (Kubernetes secret)

    CELERY_BROKER_URL: Optional[RedisDsn] = None
    CELERY_RESULT_BACKEND_URL: Optional[RedisDsn] = None

    @model_validator(mode='after')
    def _construct_derived_urls(self) -> 'Settings':
        # Construct the DATABASE_URL if not provided directly
        if self.DATABASE_URL is None:
            if all([self.DB_DRIVER, self.DB_USER, self.DB_PASSWORD, self.DB_HOST, self.DB_PORT, self.DB_NAME]):
                self.DATABASE_URL = PostgresDsn(
                    f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
                )

        # Construct Redis connection URLs for Celery
        if self.CELERY_BROKER_URL is None:
            self.CELERY_BROKER_URL = RedisDsn(
                f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_BROKER_DB_NUMBER}"
            )

        if self.CELERY_RESULT_BACKEND_URL is None:
            self.CELERY_RESULT_BACKEND_URL = RedisDsn(
                f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_RESULT_BACKEND_DB_NUMBER}"
            )

        return self

    @property
    def computed_redis_dsn_id_mapping(self) -> str:
        return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB_ID_MAPPING}"

# Instantiate settings
settings = Settings()

# Debug output for development
if settings.ENVIRONMENT == "development":
    print("--- Loaded Application Settings ---")
    for key, value in settings.dict().items():
        if "secret" in key.lower() or "password" in key.lower():
            print(f"{key}: ******")
        else:
            print(f"{key}: {value}")
    print("--- End Application Settings ---")
    print(f"DATABASE_URL (from validator or env): {settings.DATABASE_URL}")
    print(f"CELERY_BROKER_URL (from validator or env): {settings.CELERY_BROKER_URL}")
    print(f"CELERY_RESULT_BACKEND_URL (from validator or env): {settings.CELERY_RESULT_BACKEND_URL}")
