from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, List # Added List, though not strictly needed for this fix, good for general Pydantic use
from pydantic import PostgresDsn, RedisDsn, HttpUrl, Field, model_validator, AliasChoices # Added model_validator, AliasChoices

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

    # Placeholder for feature flags if needed in the future
    # FEATURE_NEW_THING_ENABLED: bool = False

    @model_validator(mode='after')
    def _construct_derived_urls(self) -> 'Settings': # Changed signature to use self
        # Construct DATABASE_URL if not provided and components are available
        if self.DATABASE_URL is None: # Use self
            if (self.DB_DRIVER and
                self.DB_USER and
                self.DB_PASSWORD and
                self.DB_HOST and
                self.DB_PORT is not None and
                self.DB_NAME):
                self.DATABASE_URL = PostgresDsn(
                    f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
                )

        # Construct Celery URLs if not provided
        if self.CELERY_BROKER_URL is None and \
           self.REDIS_HOST and \
           self.REDIS_PORT is not None and \
           self.CELERY_BROKER_DB_NUMBER is not None:
            self.CELERY_BROKER_URL = RedisDsn(
                f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_BROKER_DB_NUMBER}"
            )

        if self.CELERY_RESULT_BACKEND_URL is None and \
           self.REDIS_HOST and \
           self.REDIS_PORT is not None and \
           self.CELERY_RESULT_BACKEND_DB_NUMBER is not None:
            self.CELERY_RESULT_BACKEND_URL = RedisDsn(
                f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_RESULT_BACKEND_DB_NUMBER}"
            )
        return self # Return self

    # @property
    # def computed_database_url(self) -> str: # Now populated by validator
    #     if self.DATABASE_URL:
    #         return str(self.DATABASE_URL)
    #     # Fallback if components were insufficient for validator, though validator should handle it
    #     if self.DB_USER and self.DB_PASSWORD and self.DB_HOST and self.DB_NAME: # Check all required components
    #        return f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    #     return "Error: DB components missing"


    # @property
    # def computed_celery_broker_url(self) -> str: # Now populated by validator
    #     if self.CELERY_BROKER_URL:
    #         return str(self.CELERY_BROKER_URL)
    #     if self.REDIS_HOST and self.REDIS_PORT is not None and self.CELERY_BROKER_DB_NUMBER is not None:
    #        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_BROKER_DB_NUMBER}"
    #     return "Error: Celery Broker Redis components missing"


    # @property
    # def computed_celery_result_backend_url(self) -> str: # Now populated by validator
    #     if self.CELERY_RESULT_BACKEND_URL:
    #         return str(self.CELERY_RESULT_BACKEND_URL)
    #     if self.REDIS_HOST and self.REDIS_PORT is not None and self.CELERY_RESULT_BACKEND_DB_NUMBER is not None:
    #        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.CELERY_RESULT_BACKEND_DB_NUMBER}"
    #     return "Error: Celery Result Redis components missing"


    @property
    def computed_redis_dsn_id_mapping(self) -> str: # This one can remain as it's a specific utility
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
