from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pydantic import PostgresDsn, RedisDsn, HttpUrl, Field

class Settings(BaseSettings):
    # Environment loading configuration
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    # General Application Settings
    PROJECT_NAME: str = "Catalog Data Load Service"
    API_PREFIX: str = "/graphql" # Or "/api" if a general prefix is preferred outside GraphQL
    ENVIRONMENT: str = Field("development", validation_alias="ENV", alias_priority=2) # e.g., development, staging, production
    LOG_LEVEL: str = "INFO"
    RELOAD: bool = False # For FastAPI live reload in dev

    # Database Configuration
    DB_DRIVER: str = "postgresql+psycopg2"
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int = 5432
    DB_NAME: str
    # Constructed DSNs
    DATABASE_URL: Optional[PostgresDsn] = None

    # Schema names (defaults match current usage)
    CATALOG_SERVICE_SCHEMA: str = "catalog"
    BUSINESS_SERVICE_SCHEMA: str = "business"

    # Wasabi S3 Configuration
    WASABI_ENDPOINT_URL: HttpUrl
    WASABI_ACCESS_KEY: str
    WASABI_SECRET_KEY: str
    WASABI_BUCKET_NAME: str
    WASABI_REGION: Optional[str] = None # Some S3 providers don't strictly need region for Wasabi

    # JWT Authentication Configuration
    JWT_SECRET: str = Field(..., validation_alias="SECRET_KEY") # Using alias for common env var name
    JWT_ALGORITHM: str = "HS512"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Redis Configuration
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    # Constructed Redis DSN (can be useful for some libraries)
    REDIS_DSN: Optional[RedisDsn] = None

    # Redis DB Numbers for different purposes
    REDIS_DB_ID_MAPPING: int = 1
    CELERY_BROKER_DB_NUMBER: int = 0
    CELERY_RESULT_BACKEND_DB_NUMBER: int = 0
    # CACHING_REDIS_DB_NUMBER: int = 2 # Example if general caching is added

    # TTL for session-related keys in Redis
    REDIS_SESSION_TTL_SECONDS: int = 86400  # Default: 24 hours

    # Celery Configuration (URLs constructed from Redis settings)
    CELERY_BROKER_URL: Optional[RedisDsn] = None
    CELERY_RESULT_BACKEND_URL: Optional[RedisDsn] = None


    @property
    def computed_database_url(self) -> str:
        return f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def computed_celery_broker_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_BROKER_DB_NUMBER}"

    @property
    def computed_celery_result_backend_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_RESULT_BACKEND_DB_NUMBER}"

    @property
    def computed_redis_dsn_id_mapping(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB_ID_MAPPING}"


# Instantiate settings
settings = Settings()

# For debugging purposes during development, you might want to print loaded settings
if settings.ENVIRONMENT == "development":
    print("--- Loaded Application Settings ---")
    for key, value in settings.model_dump().items():
        if "secret" in key.lower() or "password" in key.lower():
            print(f"{key}: ******")
        else:
            print(f"{key}: {value}")
    print("--- End Application Settings ---")
    print(f"Computed DB URL: {settings.computed_database_url}")
    print(f"Computed Celery Broker URL: {settings.computed_celery_broker_url}")
    print(f"Computed Celery Result Backend URL: {settings.computed_celery_result_backend_url}")


from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, List
from pydantic import PostgresDsn, RedisDsn, HttpUrl, Field, model_validator, AliasChoices

class Settings(BaseSettings):
    # Environment loading configuration
    # Looks for a .env file and loads variables from there, overriding with actual environment variables.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding='utf-8',
        extra='ignore' # Ignore extra fields not defined in the model
    )

    # General Application Settings
    PROJECT_NAME: str = "Catalog Data Load Service"
    API_PREFIX: str = "/graphql"
    ENVIRONMENT: str = Field("development", validation_alias=AliasChoices("ENV", "ENVIRONMENT"))
    LOG_LEVEL: str = "INFO"
    RELOAD: bool = False # For FastAPI live reload in dev, usually set via CLI flag for uvicorn

    # Database Configuration
    DB_DRIVER: str = "postgresql+psycopg2"
    DB_USER: Optional[str] = None
    DB_PASSWORD: Optional[str] = None
    DB_HOST: Optional[str] = None
    DB_PORT: Optional[int] = 5432
    DB_NAME: Optional[str] = None

    # Full DATABASE_URL can be provided, or it will be constructed
    DATABASE_URL: Optional[PostgresDsn] = None

    CATALOG_SERVICE_SCHEMA: str = "catalog"
    BUSINESS_SERVICE_SCHEMA: str = "business"

    # Wasabi S3 Configuration
    WASABI_ENDPOINT_URL: Optional[HttpUrl] = None
    WASABI_ACCESS_KEY: Optional[str] = None
    WASABI_SECRET_KEY: Optional[str] = None
    WASABI_BUCKET_NAME: Optional[str] = None
    WASABI_REGION: Optional[str] = None # e.g., 'us-east-1', though often not strictly needed for Wasabi custom endpoints

    # JWT Authentication Configuration
    # Env var can be JWT_SECRET or SECRET_KEY. JWT_SECRET takes precedence if both are set.
    JWT_SECRET: str = Field("your_super_secret_jwt_key_minimum_32_characters_long", validation_alias=AliasChoices("JWT_SECRET", "SECRET_KEY"))
    JWT_ALGORITHM: str = "HS512"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Redis Configuration
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    REDIS_DB_ID_MAPPING: int = 1
    CELERY_BROKER_DB_NUMBER: int = 0
    CELERY_RESULT_BACKEND_DB_NUMBER: int = 0
    # CACHING_REDIS_DB_NUMBER: int = 2 # Example

    REDIS_SESSION_TTL_SECONDS: int = 86400

    # Full Celery URLs can be provided, or they will be constructed
    CELERY_BROKER_URL: Optional[RedisDsn] = None
    CELERY_RESULT_BACKEND_URL: Optional[RedisDsn] = None

    # Feature Flags (example)
    # FEATURE_XYZ_ENABLED: bool = False

    @model_validator(mode='after')
    def construct_derived_urls(cls, values):
        # Construct DATABASE_URL if not provided and components are available
        if values.get('DATABASE_URL') is None:
            if all(values.get(k) for k in ['DB_DRIVER', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']):
                values['DATABASE_URL'] = PostgresDsn(
                    f"{values['DB_DRIVER']}://{values['DB_USER']}:{values['DB_PASSWORD']}@{values['DB_HOST']}:{values['DB_PORT']}/{values['DB_NAME']}"
                )
            elif values.get('ENVIRONMENT', 'development') != 'test': # Don't error out for tests if DB isn't fully configured
                 # Allow DATABASE_URL to be None if components are missing, consumers must handle it
                 pass


        # Construct Celery URLs if not provided
        if values.get('CELERY_BROKER_URL') is None and values.get('REDIS_HOST') and values.get('REDIS_PORT') is not None and values.get('CELERY_BROKER_DB_NUMBER') is not None:
            values['CELERY_BROKER_URL'] = RedisDsn(
                f"redis://{values['REDIS_HOST']}:{values['REDIS_PORT']}/{values['CELERY_BROKER_DB_NUMBER']}"
            )

        if values.get('CELERY_RESULT_BACKEND_URL') is None and values.get('REDIS_HOST') and values.get('REDIS_PORT') is not None and values.get('CELERY_RESULT_BACKEND_DB_NUMBER') is not None:
            values['CELERY_RESULT_BACKEND_URL'] = RedisDsn(
                f"redis://{values['REDIS_HOST']}:{values['REDIS_PORT']}/{values['CELERY_RESULT_BACKEND_DB_NUMBER']}"
            )
        return values

    # Utility property for Redis used by ID mapping, ensuring it uses the correct DB
    @property
    def REDIS_URL_ID_MAPPING(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB_ID_MAPPING}"

# Global settings instance
settings = Settings()

# Optional: Print settings during development for verification
# This should be conditional and not run in production.
if settings.ENVIRONMENT == "development":
    import json
    print("--- Application Settings Loaded ---")
    # Use model_dump_json for a cleaner way to see derived values too, excluding secrets
    # Need to be careful with secrets. Pydantic doesn't auto-hide them in model_dump.

    sensitive_keys = {"db_password", "jwt_secret", "wasabi_secret_key"}
    printable_settings = {}
    for k, v in settings.model_dump().items():
        if k.lower() in sensitive_keys:
            printable_settings[k] = "*******"
        else:
            printable_settings[k] = v

    try:
        print(json.dumps(printable_settings, indent=2, default=str))
    except TypeError: # Fallback if some types are not JSON serializable by default
        for key, value in printable_settings.items():
             print(f"{key}: {value}")

    print("---------------------------------")