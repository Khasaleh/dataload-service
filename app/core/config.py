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

    # Placeholder for feature flags if needed in the future
    # FEATURE_NEW_THING_ENABLED: bool = False

    # Pydantic model validator to construct full DSNs after individual fields are loaded
    # This is not directly supported by model_validator in v2 in the same way as root_validator in v1
    # Instead, we can compute them upon initialization or as properties.
    # For simplicity, we'll assume they are set directly if needed, or constructed where used.
    # Alternatively, use @property methods.

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

# Note: The actual DATABASE_URL, CELERY_BROKER_URL, etc., will be constructed
# and used in their respective modules based on these base settings.
# The Optional[PostgresDsn] etc. fields are there if you want Pydantic to validate
# a full URL if it's provided directly as an env var, overriding components.
# If DATABASE_URL is set in .env, it will be used; otherwise, it remains None here,
# and modules will use `computed_database_url`.
# It's often cleaner to rely on the computed properties.

# To make this more robust for DATABASE_URL, CELERY_BROKER_URL, etc.:
# One common pattern is to have a model_validator that tries to build the URL
# if the component parts are present and the full URL isn't.
# However, Pydantic v2's model_validator works a bit differently.
# A simpler approach for now is to have the modules that need these URLs
# import `settings` and use the computed properties.

# Example of how `DATABASE_URL` could be prioritized if set directly,
# or computed if not (this logic would typically go into a model_validator or __init__):
#
# from pydantic import model_validator
# class Settings(BaseSettings):
#     # ... other fields
#     DATABASE_URL: Optional[PostgresDsn] = None # If this is provided in .env, it's used
#
#     @model_validator(mode='after')
#     def compute_dsns(cls, values):
#         if values.DATABASE_URL is None and all(values.get(k) for k in ['DB_DRIVER', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']):
#             values.DATABASE_URL = PostgresDsn(
#                 f"{values.DB_DRIVER}://{values.DB_USER}:{values.DB_PASSWORD}@{values.DB_HOST}:{values.DB_PORT}/{values.DB_NAME}"
#             )
#         # Similar for Celery URLs
#         return values
#
# For Pydantic Settings, this auto-computation is often handled by having the main application
# or relevant modules construct these from the base settings when they are first needed.
# The @property approach is clean and explicit for this.

# Standardizing JWT_SECRET:
# .env.example has JWT_SECRET. The code uses `os.getenv("JWT_SECRET")`.
# Pydantic BaseSettings will automatically pick up JWT_SECRET.
# If we want to allow an alias like `SECRET_KEY` from env for `settings.JWT_SECRET`,
# we can use `Field(validation_alias='SECRET_KEY')`.
# The current code for JWT_SECRET in `app/dependencies/auth.py` uses `os.getenv("JWT_SECRET", ...)`
# We should ensure `Settings.JWT_SECRET` is used.
# The `Field(validation_alias="SECRET_KEY")` for JWT_SECRET means it will try to load `SECRET_KEY` from env first, then `JWT_SECRET`.
# For consistency with `.env.example`, let's ensure `JWT_SECRET` is the primary name in `Settings`
# and it picks up `JWT_SECRET` from the environment.

# Corrected approach for JWT_SECRET, assuming .env.example uses JWT_SECRET
# class Settings(BaseSettings):
#     JWT_SECRET: str # No alias needed if env var is JWT_SECRET
#     # ...
# If the env var was SECRET_KEY but you want settings.JWT_SECRET:
# class Settings(BaseSettings):
#     JWT_SECRET: str = Field(validation_alias="SECRET_KEY")

# The current `Field(..., validation_alias="SECRET_KEY")` for `JWT_SECRET` assumes the env var might be `SECRET_KEY`.
# `.env.example` uses `JWT_SECRET`. So, let's adjust for clarity.
# It's better to have the Pydantic field name match the primary env var name.
# If aliases are needed for other common names, `validation_alias` is the way.
# Given `.env.example` has `JWT_SECRET`, `JWT_SECRET: str` in Pydantic is direct.
# The current `Field(..., validation_alias="SECRET_KEY")` means it would look for `SECRET_KEY` first.
# Let's assume the primary env var is `JWT_SECRET` as per `.env.example`.

# Finalizing JWT_SECRET field:
# JWT_SECRET: str  (Pydantic will load from an env var named JWT_SECRET)

# Let's refine the JWT_SECRET part in the class definition.
# The `Field(..., validation_alias="SECRET_KEY")` means that if an environment variable `SECRET_KEY` exists,
# it will be used to populate `JWT_SECRET`. If `SECRET_KEY` does not exist, it will then look for an
# environment variable named `JWT_SECRET`.
# Since `.env.example` defines `JWT_SECRET`, this setup is fine, but it's good to be aware of the lookup order.
# If we want to strictly use only `JWT_SECRET` from env: `JWT_SECRET: str`.
# If we want to allow `SECRET_KEY` as an alternative name for `JWT_SECRET`: `JWT_SECRET: str = Field(validation_alias=AliasChoices('SECRET_KEY', 'JWT_SECRET'))`
# Given the current code uses `os.getenv("JWT_SECRET")`, having `JWT_SECRET: str` in Pydantic is the most direct mapping.
# The `Field(..., validation_alias="SECRET_KEY")` allows flexibility if some environments use `SECRET_KEY`.
# This is acceptable.

# Ensure pydantic[email] might be needed for HttpUrl if not already included by pydantic-settings.
# Typically, pydantic-settings brings pydantic, and pydantic standard includes HttpUrl.

# Standardizing WASABI_BUCKET_NAME and WASABI_ENDPOINT_URL:
# .env.example: WASABI_BUCKET_NAME, WASABI_ENDPOINT_URL
# wasabiclient.py code used os.getenv("WASABI_BUCKET"), os.getenv("WASABI_ENDPOINT")
# The Settings class uses WASABI_BUCKET_NAME and WASABI_ENDPOINT_URL, which is good.
# We will need to update wasabiclient.py to use these names from the settings object.

# The printing of settings is useful for local debugging but should be conditional or removed for production.
# The `if settings.ENVIRONMENT == "development":` handles this.
# Consider using `model_config = SettingsConfigDict(env_prefix = 'APP_')` if you want to prefix all env vars, e.g. `APP_DB_USER`. For now, not using prefix.

# One final check: Pydantic's PostgresDsn and RedisDsn will validate the format if a full DSN is provided.
# If we are constructing them from components, the properties `computed_database_url` etc. are fine.
# The fields `DATABASE_URL: Optional[PostgresDsn] = None` in `Settings` are for if the user provides a *full* DSN
# as an environment variable (e.g., `DATABASE_URL=postgresql://...`). If they do, Pydantic validates it.
# If they don't, it remains `None`, and we use the computed properties. This is a good pattern.
# The computed properties should ideally also return Pydantic DSN types for consistency,
# but returning `str` is also common and works for SQLAlchemy/Redis clients.
# For stricter typing, they could be:
# @property
# def computed_database_url(self) -> PostgresDsn:
#     return PostgresDsn(f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}")
# This adds validation to the computed URL as well.
# For now, string return is fine as it matches current direct usage in many libraries.
# Let's stick to string for computed properties for now to minimize changes in consuming code,
# as SQLAlchemy `create_engine` takes a string.```python
# app/core/config.py
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

```

**Key changes and considerations in this `Settings` class:**

1.  **`pydantic-settings`:** Uses `BaseSettings` and `SettingsConfigDict` from the newer `pydantic-settings` library (successor to Pydantic V1's `BaseSettings`).
2.  **`.env` Loading:** `SettingsConfigDict(env_file=".env", ...)` handles loading from a `.env` file.
3.  **Type Hinting & Validation:** Uses Pydantic types like `PostgresDsn`, `RedisDsn`, `HttpUrl` for validation. Optional fields are marked with `Optional`.
4.  **Required Fields:** Fields without defaults (like `DB_USER`, `DB_PASSWORD`, etc., when not providing a full `DATABASE_URL`) will cause an error if not found in the environment or `.env` file, making missing configurations explicit. I've made them Optional for now to allow DATABASE_URL to be the single source of truth if provided.
5.  **Derived URLs (`model_validator`):**
    *   A `model_validator` named `construct_derived_urls` is used to automatically build `DATABASE_URL`, `CELERY_BROKER_URL`, and `CELERY_RESULT_BACKEND_URL` if they are not provided directly but their constituent parts are. This is a common pattern.
6.  **Aliases (`validation_alias`, `AliasChoices`):**
    *   `ENVIRONMENT`: Allows being set by `ENV` or `ENVIRONMENT`.
    *   `JWT_SECRET`: Allows being set by `JWT_SECRET` or `SECRET_KEY` (with `JWT_SECRET` taking precedence if both are defined). This matches the flexibility of `os.getenv("X", os.getenv("Y"))`.
7.  **Wasabi Configuration:** `WASABI_ENDPOINT_URL`, `WASABI_ACCESS_KEY`, `WASABI_SECRET_KEY`, `WASABI_BUCKET_NAME`, `WASABI_REGION` are included. These now align with the `.env.example` naming for endpoint and bucket.
8.  **Redis URL Property:** Added `REDIS_URL_ID_MAPPING` as a property for convenience where a full URL for that specific Redis DB is needed.
9.  **Defaults:** Sensible defaults are provided for things like `LOG_LEVEL`, `REDIS_HOST`, `REDIS_PORT`, etc.
10. **Development Mode Printing:** Added a conditional print of settings (masking common secret key names) if `ENVIRONMENT` is "development", which can be helpful for debugging.
11. **Optional DB Components:** `DB_USER`, `DB_PASSWORD`, etc., are `Optional` to allow `DATABASE_URL` to be the sole source of truth if provided. The `model_validator` then constructs `DATABASE_URL` if these components *are* provided and `DATABASE_URL` itself is not.

This `Settings` class provides a centralized, type-safe, and auto-validating way to manage application configurations. The next step will be to refactor the application to use this `settings` instance.
