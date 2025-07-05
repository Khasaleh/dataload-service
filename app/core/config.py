from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, RedisDsn, HttpUrl, Field, model_validator, field_validator
from typing import Optional


@field_validator('WASABI_ACCESS_KEY', 'WASABI_SECRET_KEY', mode='before')
@classmethod
def strip_whitespace(cls, v):
    if isinstance(v, str):
        return v.strip()
    return v

@field_validator('WASABI_ACCESS_KEY')
@classmethod
def validate_ascii(cls, v):
    if not all(ord(c) < 128 for c in v):
        raise ValueError("WASABI_ACCESS_KEY contains invalid non-ASCII characters")
    return v.strip()


class Settings(BaseSettings):
    # Environment loading configuration
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')
    
    AUTH_VALIDATION_ENABLED: bool = False

    # General Application Settings
    PROJECT_NAME: str = "Catalog Data Load Service"
    API_PREFIX: str = "/graphql"
    ENVIRONMENT: str = Field("stage", validation_alias="ENV", alias_priority=2)
    LOG_LEVEL: str = "INFO"
    RELOAD: bool = False

    # Primary Database Configuration
    DB_DRIVER: str = "postgresql+psycopg2"
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int = 5432
    DB_NAME: str
    DATABASE_URL: Optional[PostgresDsn] = None

    # Secondary Database Configuration (for return_policies)
    DB_NAME2: Optional[str] = None
    DATABASE_URL_DB2: Optional[PostgresDsn] = None

    # Schema names (shared)
    CATALOG_SERVICE_SCHEMA: str = "public"
    BUSINESS_SERVICE_SCHEMA: str = "public"

    # Wasabi S3 Configuration
    WASABI_ENDPOINT_URL: HttpUrl
    WASABI_ACCESS_KEY: str
    WASABI_SECRET_KEY: str
    WASABI_BUCKET_NAME: str
    WASABI_REGION: Optional[str] = None
    LOCAL_STORAGE_PATH: str = "/data/uploads"

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
    REDIS_PASSWORD: str = Field(...)

    CELERY_BROKER_URL: Optional[RedisDsn] = None
    CELERY_RESULT_BACKEND_URL: Optional[RedisDsn] = None

    # Map load_types to which DB key to use
    # Only 'return_policies' will go to DB2. Everything else uses the default DATABASE_URL.
    LOADTYPE_DB_MAP: dict[str, str] = {
        "return_policies": "DB2"
    }

    @model_validator(mode='after')
    def _construct_derived_urls(self) -> 'Settings':
        # 1) Build primary DATABASE_URL if absent
        if self.DATABASE_URL is None:
            self.DATABASE_URL = PostgresDsn(
                f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            )

        # 2) Build secondary DATABASE_URL_DB2 if absent
        if self.DATABASE_URL_DB2 is None and self.DB_NAME2:
            self.DATABASE_URL_DB2 = PostgresDsn(
                f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME2}"
            )

        # 3) Construct Celery Redis URLs if absent
        if self.CELERY_BROKER_URL is None:
            self.CELERY_BROKER_URL = RedisDsn(
                f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}"
                f":{self.REDIS_PORT}/{self.CELERY_BROKER_DB_NUMBER}"
            )
        if self.CELERY_RESULT_BACKEND_URL is None:
            self.CELERY_RESULT_BACKEND_URL = RedisDsn(
                f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}"
                f":{self.REDIS_PORT}/{self.CELERY_RESULT_BACKEND_DB_NUMBER}"
            )

        return self

    @property
    def computed_redis_dsn_id_mapping(self) -> str:
        return (
            f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}"
            f":{self.REDIS_PORT}/{self.REDIS_DB_ID_MAPPING}"
        )


# Instantiate settings
settings = Settings()

# Debug output for development
if settings.ENVIRONMENT == "development":
    print("--- Loaded Application Settings ---")
    print(f"DATABASE_URL:    {settings.DATABASE_URL}")
    print(f"DATABASE_URL_DB2:{settings.DATABASE_URL_DB2}")
    print(f"CELERY_BROKER_URL:    {settings.CELERY_BROKER_URL}")
    print(f"CELERY_RESULT_BACKEND_URL: {settings.CELERY_RESULT_BACKEND_URL}")
    print("--- End Application Settings ---")
