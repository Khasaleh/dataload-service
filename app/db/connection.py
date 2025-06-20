from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
import logging

logger = logging.getLogger(__name__)

# --- Database Configuration from Environment Variables ---
# These variables define the connection to the primary/default database (specified by DB_NAME).
# This database acts as the entry point and may contain multiple schemas for different tenants
# if using a schema-per-tenant strategy.
#
# - DB_DRIVER: SQLAlchemy driver string (e.g., "postgresql+psycopg2", "mysql+mysqlconnector").
# - DB_USER: Username for database connection.
# - DB_PASSWORD: Password for database connection.
# - DB_HOST: Hostname or IP address of the database server.
# - DB_PORT: Port number for the database server (e.g., "5432" for PostgreSQL).
# - DB_NAME: The name of the specific database to connect to on the server. This database
#            will contain the various schemas (public, tenant-specific, shared service schemas).

DB_DRIVER = os.getenv("DB_DRIVER", "postgresql+psycopg2")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME") # The main database name

# Schema configurations (can be used for table definitions or search_path modifications)
CATALOG_SCHEMA = os.getenv("CATALOG_SERVICE_SCHEMA", "catalog") # Example default schema name
BUSINESS_SCHEMA = os.getenv("BUSINESS_SERVICE_SCHEMA", "business") # Example default schema name

# Construct the primary DATABASE_URL from component environment variables
DATABASE_URL = None
if all([DB_DRIVER, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    DATABASE_URL = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    logger.error(
        "One or more core database connection environment variables are missing "
        "(DB_DRIVER, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME). "
        "Database functionality will be impaired."
    )
    # Depending on application strictness, could raise an exception here.
    # For now, DATABASE_URL will be None, and get_engine will fail if it's used.

# --- Multi-Tenancy Connection Management ---

# DATABASES dictionary can still be used for a database-per-tenant strategy,
# but the "default" should now rely on the constructed DATABASE_URL.
DATABASES = {
    # Example for specific tenant DB if it's different from the default construction:
    # "biz_1001": "postgresql://user_biz1:pass_biz1@custom_host:5432/db_for_biz_1001",
    "default": DATABASE_URL # Uses the URL constructed from individual env vars
}

def get_engine(business_id: str, strategy: str = "database_per_tenant"):
    """
    Returns a SQLAlchemy engine.
    - If 'database_per_tenant', it attempts to find a specific DB URL for the business_id.
    - If 'shared_database_with_schema_switching', it uses a default/shared database URL.
    """
    db_url = None
    if strategy == "database_per_tenant":
        db_url = DATABASES.get(f"biz_{business_id}")
        if not db_url:
            logger.warning(f"No specific database configuration for business_id={business_id}. Falling back to default.")
            db_url = DATABASES.get("default")
    else: # shared_database_with_schema_switching or other strategies
        db_url = DATABASES.get("default")

    if not db_url:
        logger.error(f"No database URL found for business_id={business_id} or default.")
        raise Exception(f"No database configuration usable for business_id={business_id}")

    logger.info(f"Creating engine for business_id {business_id} using URL (ending): ...{db_url[-20:]}")
    return create_engine(db_url, pool_pre_ping=True)


def get_session(business_id: str):
    """
    Provides a SQLAlchemy session, potentially configured for a specific tenant.

    This function demonstrates how different multi-tenancy strategies could be initiated.
    The actual implementation details would depend heavily on the chosen ORM, database,
    and overall architecture.

    Note on Database Migrations (Alembic) for Multi-Tenancy:
    - If using a schema-per-tenant strategy (like 'shared_database_with_schema_switching' below),
      Alembic migrations (managed in the `alembic/` directory) need to be applied to EACH
      tenant-specific schema to create and maintain its table structures. This typically involves
      scripting Alembic runs or using advanced Alembic features for multi-tenancy, possibly
      by adapting `alembic/env.py` to iterate over known tenant schemas or by running
      `alembic upgrade head --x-arg tenant_schema=business_xyz` and using that arg in `env.py`.
    - Shared tables (e.g., in 'public' or `CATALOG_SCHEMA`, `BUSINESS_SCHEMA`) are typically
      migrated once per database.
    """

    # For this example, we'll assume a 'shared_database_with_schema_switching' strategy
    # to demonstrate the schema path setting as requested by the subtask.
    # In a real app, the strategy might be chosen based on configuration or business_id.
    tenant_strategy = "shared_database_with_schema_switching" # Example strategy

    engine = get_engine(business_id, strategy=tenant_strategy)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = SessionLocal()

    logger.info(f"Session created for business_id: {business_id}. Strategy: {tenant_strategy}")

    # --- Placeholder for Schema Switching Logic ---
    # In a multi-tenant system where tenants share a database but are isolated by schemas:
    # - Each tenant's data resides in a dedicated schema (e.g., "business_1001", "tenant_abc").
    # - When a session is established for a specific tenant, the database session's
    #   search_path (for PostgreSQL) or current schema/database must be set.
    # This ensures that SQL queries operate on the correct tenant's data.

    if tenant_strategy == "shared_database_with_schema_switching":
        # This is a common approach for PostgreSQL for isolating tenant data within a shared database.
        # Other databases might use different commands (e.g., `USE database_name; SET schema = schema_name;` or similar).

        # Defines the schema name for this tenant. For example, if business_id is "acme",
        # this will attempt to use a schema named "business_acme".
        # IMPORTANT: These tenant-specific schemas (e.g., "business_acme") MUST be manually
        # created in your PostgreSQL database (the one defined by DB_NAME) before they can be used.
        # Example SQL: CREATE SCHEMA IF NOT EXISTS business_acme;
        schema_name = f"business_{business_id}" # Or derive from a mapping, lookup service, etc.

        try:
            # Set the session's search_path for PostgreSQL. This command dictates the default schema
            # where tables will be looked for (and created if not schema-qualified) for the
            # duration of the current session/transaction.
            # The tenant's specific schema (`schema_name`) is placed first in the search path.
            # `public` is often included for access to standard PostgreSQL functions or extensions.
            # Shared schemas like `CATALOG_SCHEMA` or `BUSINESS_SCHEMA` (loaded from env vars)
            # could also be included in the search_path if they contain shared tables or functions
            # that need to be accessible without schema qualification:
            # e.g., session.execute(text(f"SET search_path TO {schema_name}, {CATALOG_SCHEMA}, {BUSINESS_SCHEMA}, public;"))
            # For this example, we'll stick to the tenant-specific schema and public.

            session.execute(text(f"SET search_path TO {schema_name}, public;"))
            logger.info(f"Successfully set search_path to '{schema_name}, public' for session of business_id {business_id}.")

            # Note on ORM table definitions:
            # If ORM models (e.g., in app/db/models.py) explicitly define their schema
            # (e.g., `__table_args__ = {"schema": "some_fixed_schema"}`), that explicit schema
            # takes precedence over the search_path for those specific tables.
            # The search_path is primarily for unqualified table names in queries or for default
            # placement of new tables if schemas are not specified in ORM/SQL.
        except Exception as e:
            logger.error(f"Failed to set search_path to '{schema_name}' for business_id {business_id}: {e}", exc_info=True)
            session.rollback()
            raise

    # Other strategies for multi-tenancy include:
    # 1. Row-Level Security (RLS):
    #    - Supported by some databases (e.g., PostgreSQL, SQL Server).
    #    - Data for all tenants is in shared tables, but policies filter rows based on the
    #      current user/session context (e.g., business_id set as a session variable).
    #    - Comment: `session.execute(text(f"SET app.current_business_id = '{business_id}';"))`
    #
    # 2. Using a dedicated multi-tenancy library:
    #    - Libraries like `sqlalchemy-multitenant` can abstract some of these details.
    #    - These libraries often provide mechanisms to manage sessions and query routing
    #      based on the current tenant.
    #
    # 3. Application-Level Filtering:
    #    - Manually adding `WHERE business_id = :current_business_id` to all queries.
    #    - Prone to errors if a filter is missed. ORM query helpers can mitigate this.

    return session
