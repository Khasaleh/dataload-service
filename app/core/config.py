from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, RedisDsn, HttpUrl, Field, model_validator, field_validator
from typing import Optional, Dict


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
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    # --- General ---
    AUTH_VALIDATION_ENABLED: bool = False
    PROJECT_NAME: str = "Catalog Data Load Service"
    API_PREFIX: str = "/graphql"
    ENVIRONMENT: str = Field("stage", validation_alias="ENV", alias_priority=2)
    LOG_LEVEL: str = "INFO"
    RELOAD: bool = False

    # --- Primary DB ---
    DB_DRIVER: str = "postgresql+psycopg2"
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int = 5432
    DB_NAME: str
    DATABASE_URL: Optional[PostgresDsn] = None

    # --- Secondary DB for return_policies ---
    DB_NAME2: Optional[str] = None
    DATABASE_URL_DB2: Optional[PostgresDsn] = None

    # --- Schemas ---
    CATALOG_SERVICE_SCHEMA: str = "public"
    BUSINESS_SERVICE_SCHEMA: str = "public"

    # --- Wasabi / Local storage ---
    WASABI_ENDPOINT_URL: HttpUrl
    WASABI_ACCESS_KEY: str
    WASABI_SECRET_KEY: str
    WASABI_BUCKET_NAME: str
    WASABI_REGION: Optional[str] = None
    LOCAL_STORAGE_PATH: str = "/data/uploads"

    # --- JWT ---
    JWT_SECRET: str = Field(..., validation_alias="SECRET_KEY")
    JWT_ALGORITHM: str = "HS512"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # --- Redis ---
    REDIS_HOST: str = "192.168.1.245"
    REDIS_PORT: int = 6379
    REDIS_DB_ID_MAPPING: int = 1
    REDIS_PASSWORD: str = Field(..., env="REDIS_PASSWORD")
    REDIS_SESSION_TTL_SECONDS: int = 86400

    # --- Celery Redis ---
    CELERY_BROKER_DB_NUMBER: int = 0
    CELERY_RESULT_BACKEND_DB_NUMBER: int = 0
    CELERY_BROKER_URL: Optional[RedisDsn] = None
    CELERY_RESULT_BACKEND_URL: Optional[RedisDsn] = None

    # --- Load-type → DB routing ---
    LOADTYPE_DB_MAP: Dict[str, str] = {
        "return_policies": "DB2"
    }

    @model_validator(mode='after')
    def _construct_derived_urls(self) -> 'Settings':
        # 1) Primary DATABASE_URL
        if self.DATABASE_URL is None:
            self.DATABASE_URL = PostgresDsn(
                f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            )

        # 2) Secondary DATABASE_URL_DB2 (if DB_NAME2 provided)
        if self.DATABASE_URL_DB2 is None and self.DB_NAME2:
            self.DATABASE_URL_DB2 = PostgresDsn(
                f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME2}"
            )

        # 3) Celery Redis URLs
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
        return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB_ID_MAPPING}"


# Instantiate
settings = Settings()

# Debug print in development
if settings.ENVIRONMENT == "development":
    print("--- Loaded Application Settings ---")
    for k, v in settings.dict().items():
        if "secret" in k.lower() or "password" in k.lower():
            print(f"{k}: ******")
        else:
            print(f"{k}: {v}")
    print("--- End Application Settings ---")
    print("DATABASE_URL:", settings.DATABASE_URL)
    print("DATABASE_URL_DB2:", settings.DATABASE_URL_DB2)
    print("CELERY_BROKER_URL:", settings.CELERY_BROKER_URL)
    print("CELERY_RESULT_BACKEND_URL:", settings.CELERY_RESULT_BACKEND_URL)
