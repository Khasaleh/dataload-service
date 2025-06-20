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
    # The DATABASES dictionary is largely simplified as we move to a single DB with shared schemas.
    # It's kept here if a very specific tenant needs a completely separate DB instance,
    # but get_engine will now primarily use the "default" constructed DATABASE_URL.
}

def get_engine(): # Parameters business_id and strategy removed
    """
    Returns a SQLAlchemy engine using the default database configuration.
    The application now assumes a single primary database containing shared schemas.
    """
    db_url = DATABASES.get("default")
    if not db_url:
        # This case should ideally be prevented by the check after DATABASE_URL construction.
        logger.error("Default database URL not configured or missing.")
        raise Exception("Default database URL is not configured.")

    logger.info(f"Creating engine using default database URL (ending): ...{db_url[-20:]}")
    return create_engine(db_url, pool_pre_ping=True)


def get_session(business_id: int): # Type hint for business_id changed to int
    """
    Provides a SQLAlchemy session configured with a fixed search_path
    for shared schemas (CATALOG_SCHEMA, BUSINESS_SCHEMA, public).

    The `business_id` parameter is now primarily for context (e.g., logging, or if RLS
    or other per-session, business-specific settings were needed beyond schema path).
    Data isolation between businesses is expected to be handled by `business_id`
    columns in tables within the shared schemas.

    Note on Database Migrations (Alembic):
    - Alembic migrations (managed in `alembic/` directory) should be applied to the shared
      schemas (`CATALOG_SCHEMA`, `BUSINESS_SCHEMA`, `public`) in the database defined by `DB_NAME`.
    - Ensure these schemas are created in your database. Alembic typically creates tables
      within the default search_path of the migration connection or in explicitly specified schemas
      if models have `__table_args__ = {"schema": "schema_name"}`.
    """

    engine = get_engine() # Simplified call
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = SessionLocal()

    # Log the business_id for context, even if not used for dynamic schema path.
    logger.info(f"Session created. Intended business_id context: {business_id}.")

    # --- Set Fixed Schema Search Path ---
    # All tenants/businesses will use the same set of schemas defined by environment variables.
    # Data isolation is achieved via `business_id` columns in tables within these schemas.

    # Schema names are loaded from environment variables at the module level.
    # CATALOG_SCHEMA (e.g., "catalog_management")
    # BUSINESS_SCHEMA (e.g., "fazeal_business")

    search_path_schemas = [
        CATALOG_SCHEMA,    # For shared catalog-related tables
        BUSINESS_SCHEMA, # For shared business-operation related tables
        "public"           # Standard public schema, always include
    ]
    # Filter out any None or empty schema names that might result if env vars are missing and defaults are None.
    # However, CATALOG_SCHEMA and BUSINESS_SCHEMA have defaults, so they should always be strings.
    valid_schemas = [s for s in search_path_schemas if s and s.strip()]

    if not valid_schemas:
        logger.error("No valid schemas found for search_path. Check CATALOG_SERVICE_SCHEMA and BUSINESS_SERVICE_SCHEMA environment variables.")
        # Depending on strictness, could raise an error or default to just "public".
        # For now, we'll proceed, and it might default to a minimal search_path if execute fails or is skipped.
        # It's better to ensure valid_schemas is never empty by having robust defaults or checks.
        # Let's ensure public is always there at least.
        valid_schemas = ["public"] if not valid_schemas else valid_schemas


    search_path_sql = f"SET search_path TO {', '.join(valid_schemas)};"

    try:
        session.execute(text(search_path_sql))
        logger.info(f"Successfully set search_path to '{', '.join(valid_schemas)}' for session. Business_id context: {business_id}.")
    except Exception as e:
        logger.error(f"Failed to set search_path to '{', '.join(valid_schemas)}' for business_id {business_id}: {e}", exc_info=True)
        session.rollback()
        raise # Re-raise to indicate session setup failure

    # Note: Data isolation is now primarily the responsibility of queries using
    # `WHERE business_id = :current_business_id` on tables within these shared schemas.
    # The ORM models should all include a `business_id` column for this purpose.
    # Row-Level Security (RLS) in PostgreSQL could also be used in conjunction with this
    # to enforce this filtering at the database level automatically.

    return session
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
