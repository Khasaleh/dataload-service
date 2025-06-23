from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging
from app.core.config import settings # Import the centralized settings

logger = logging.getLogger(__name__)

# --- Database Configuration from Centralized Settings ---
# All database configuration is now sourced from the `settings` object.

# The DATABASE_URL is either provided directly or constructed in the Settings class.
DATABASE_URL = settings.DATABASE_URL

if not DATABASE_URL:
    logger.error(
        "DATABASE_URL is not configured in settings. "
        "Ensure database environment variables (DB_USER, DB_PASSWORD, etc.) "
        "or a full DATABASE_URL are provided. Database functionality will be impaired."
    )
    # Depending on application strictness, could raise an exception here.
    # For now, get_engine will fail if DATABASE_URL is None.

# Schema configurations are also sourced from settings
CATALOG_SCHEMA = settings.CATALOG_SERVICE_SCHEMA
BUSINESS_SCHEMA = settings.BUSINESS_SERVICE_SCHEMA


# --- Engine Creation ---
# The engine is created once and can be reused.
_engine = None

def get_engine():
    """
    Returns a SQLAlchemy engine using the database configuration from settings.
    The application assumes a single primary database.
    """
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            logger.critical("DATABASE_URL is not configured. Cannot create database engine.")
            raise Exception("DATABASE_URL is not configured. Cannot create database engine.")

        # Convert Pydantic DSN type to string for create_engine
        db_url_str = str(DATABASE_URL)
        logger.info(f"Creating engine using database URL (ending): ...{db_url_str[-20:]}")
        _engine = create_engine(db_url_str, pool_pre_ping=True)
    return _engine


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
