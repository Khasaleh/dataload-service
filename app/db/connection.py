import logging
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from app.core.config import settings  # centralized settings

logger = logging.getLogger(__name__)

# Schema names from settings
CATALOG_SCHEMA = settings.CATALOG_SERVICE_SCHEMA
BUSINESS_SCHEMA = settings.BUSINESS_SERVICE_SCHEMA

# --- Engine cache ---
_engine_default: Optional = None
_engine_db2: Optional    = None

def get_engine(db_key: Optional[str] = None):
    """
    Return a SQLAlchemy engine for the given db_key:
      - None or "default" → settings.DATABASE_URL
      - "DB2"           → settings.DATABASE_URL_DB2
    """
    global _engine_default, _engine_db2

    if db_key == "DB2":
        url = settings.DATABASE_URL_DB2
        if url is None:
            logger.critical("DATABASE_URL_DB2 is not configured but 'DB2' was requested.")
            raise RuntimeError("Missing DATABASE_URL_DB2 for db_key='DB2'")
        if _engine_db2 is None:
            logger.info("Creating DB2 engine (ending): ...%s", str(url)[-20:])
            _engine_db2 = create_engine(str(url), pool_pre_ping=True)
        return _engine_db2

    # default
    url = settings.DATABASE_URL
    if url is None:
        logger.critical("DATABASE_URL is not configured. Cannot create default engine.")
        raise RuntimeError("Missing DATABASE_URL for default database")
    if _engine_default is None:
        logger.info("Creating default engine (ending): ...%s", str(url)[-20:])
        _engine_default = create_engine(str(url), pool_pre_ping=True)
    return _engine_default


def get_session(
    business_id: int,
    db_key: Optional[str] = None
) -> Session:
    """
    Return a SQLAlchemy Session bound to the engine selected by db_key.
    - business_id is logged for context.
    - We set a fixed search_path to include CATALOG_SCHEMA, BUSINESS_SCHEMA, and public.
    """
    engine = get_engine(db_key)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = SessionLocal()

    logger.info(
        "Session created (db_key=%s). Intended business_id: %s",
        db_key or "default",
        business_id,
    )

    # Build and apply search_path
    schemas = [CATALOG_SCHEMA, BUSINESS_SCHEMA, "public"]
    valid = [s.strip() for s in schemas if s and s.strip()]
    search_path_sql = f"SET search_path TO {', '.join(valid)};"
    try:
        session.execute(text(search_path_sql))
        logger.info("search_path set to '%s'", ", ".join(valid))
    except Exception as e:
        logger.error("Failed to set search_path %s: %s", valid, e, exc_info=True)
        session.rollback()
        raise

    return session
