import os
from logging.config import fileConfig
import sys # For sys.path modification

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Setup for Target Metadata ---
# Add project root to sys.path to allow importing app.db.base_class and app.db.models
# This assumes the alembic directory is at the project root.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import Base and all ORM models to ensure they are registered with Base.metadata
# This is crucial for Alembic's autogenerate feature to detect model changes.
from app.db.base_class import Base
# Ensure all models are imported here after being defined in app.db.models
from app.db.models import (
    UploadSessionOrm,
    BrandOrm,
    AttributeOrm,
    ReturnPolicyOrm,
    ProductOrm,
    ProductItemOrm,

    ProductPriceOrm,
    MetaTagOrm,
    CategoryOrm,
    CategoryAttributeOrm,
    BusinessDetailsOrm,
    ProductImageOrm,
    ProductSpecificationOrm,
    # Make sure ShoppingCategoryOrm is also imported if it's defined in models.py and part of Base
    from app.models.shopping_category import ShoppingCategoryOrm # Ensure this path is correct

)

# target_metadata should point to your Base.metadata
target_metadata = Base.metadata

# --- Database URL Configuration ---
# Construct database URL from environment variables, similar to app/db/connection.py
# Provide fallbacks suitable for Alembic CLI execution if environment variables might not be fully set up.
DB_DRIVER = os.getenv("DB_DRIVER", "postgresql+psycopg2")
DB_USER = os.getenv("DB_USER", "defaultuser") # Fallback for Alembic CLI
DB_PASSWORD = os.getenv("DB_PASSWORD", "defaultpass") # Fallback for Alembic CLI
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "defaultdb") # Fallback for Alembic CLI

# Dynamically construct the database URL
# This overrides the sqlalchemy.url from alembic.ini
db_url_from_env = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
if db_url_from_env: # Ensure not None if critical env vars were missing and not defaulted
    config.set_main_option('sqlalchemy.url', db_url_from_env)
else:
    # Handle case where critical DB env vars are missing and no defaults made a usable URL
    # This might happen if you remove defaults for DB_USER, DB_PASSWORD, DB_NAME above
    # and they are not set in the environment where Alembic is run.
    raise ValueError("Database URL could not be constructed from environment variables. "
                     "Ensure DB_USER, DB_PASSWORD, DB_HOST, DB_NAME are set.")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.
    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # include_schemas=True, # Set to True if you are using multiple schemas and want Alembic to manage them
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.
    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}), # Uses sqlalchemy.url from config
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
            # include_schemas=True, # If managing multiple schemas
            # version_table_schema=target_metadata.schema, # If alembic_version table needs to be in a specific schema
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
