from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
import logging

logger = logging.getLogger(__name__)

# --- Database Connection Strategies for Multi-Tenancy ---
# The functions below illustrate how database connections and sessions might be managed
# in a multi-tenant application. The specific strategy depends on the chosen isolation level.

# Strategy 1: Database per Tenant (as suggested by the DATABASES dictionary)
#   - Each tenant has a completely separate database.
#   - Provides strong data isolation.
#   - Can be complex to manage many databases.
#   - The DATABASES dictionary maps a business_id to a specific database URL.

DATABASES = {
    # Example: "biz_1001": "postgresql://user:pass@host_for_biz_1001:5432/db_for_biz_1001",
    # For a generic local setup, you might have a single DB URL from env vars:
    "default": os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/default_catalog_db")
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
    """

    # For this example, we'll assume a 'shared_database_with_schema_switching' strategy
    # to demonstrate the schema path setting as requested by the subtask.
    # In a real app, the strategy might be chosen based on configuration or business_id.
    tenant_strategy = "shared_database_with_schema_switching"

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
        # This is a common approach for PostgreSQL.
        # Other databases might have different commands or mechanisms.
        schema_name = f"business_{business_id}" # Or derive from a mapping, lookup service, etc.

        try:
            # Example for PostgreSQL: Set the session's search_path.
            # This tells PostgreSQL which schemas to look in, in order.
            # The tenant's schema is first, followed by 'public' (for shared tables/extensions).
            session.execute(text(f"SET search_path TO {schema_name}, public;"))
            logger.info(f"Successfully set search_path to '{schema_name}, public' for session of business_id {business_id}.")
        except Exception as e:
            logger.error(f"Failed to set search_path to '{schema_name}' for business_id {business_id}: {e}", exc_info=True)
            # Depending on the application's requirements, you might:
            # - Raise the exception to prevent operations on an incorrectly configured session.
            # - Fallback to a default behavior if applicable.
            # - Mark the session as invalid.
            session.rollback() # Rollback any transaction started by session.execute() if it failed mid-way
            raise # Re-raise the exception to make the caller aware of the failure.

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
