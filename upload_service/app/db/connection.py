from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session # Added Session for type hinting
from typing import Dict # For type hinting
import os

# Example connection registry (could come from DB or config service)
DATABASES = {
    "biz_1001": os.getenv("DB_URL_BIZ_1001", "postgresql://user:pass@host1:5432/catalog_1001"),
    "biz_1002": os.getenv("DB_URL_BIZ_1002", "postgresql://user:pass@host2:5432/catalog_1002"),
    "biz_1003": os.getenv("DB_URL_BIZ_1003", "postgresql://user:pass@host3:5432/catalog_1003"),
    # It's good practice to load these from environment variables or a config service
    # For example, using os.getenv("DB_URL_BIZ_1001", "default_fallback_if_not_set")
}

# Cache for SQLAlchemy engines
_ENGINES: Dict[str, create_engine] = {} # Using create_engine for type hint of value

def get_engine(business_id: str) -> create_engine: # Return type hint
    # Construct the key for the DATABASES dictionary, e.g., "biz_demo123"
    # Assuming business_id from token might be "demo123", adjust if format is different
    db_key = f"biz_{business_id}"
    db_url = DATABASES.get(db_key)

    if not db_url:
        # More specific error for configuration missing
        raise ValueError(f"No database configuration found for business_id='{business_id}' (key='{db_key}')")

    # Check if engine is already cached
    if db_url not in _ENGINES:
        print(f"Creating new SQLAlchemy engine for {db_url}") # Logging for visibility
        try:
            # Create and cache the engine
            _ENGINES[db_url] = create_engine(db_url, pool_pre_ping=True, echo=False) # echo=False is common for prod
        except Exception as e:
            # Handle potential errors during engine creation (e.g., invalid URL, DB not reachable)
            # Log the error e
            raise ConnectionError(f"Failed to create database engine for {db_url}: {e}") from e
    else:
        print(f"Using cached SQLAlchemy engine for {db_url}") # Logging for visibility

    return _ENGINES[db_url]

def get_session(business_id: str) -> Session: # Return type hint
    engine = get_engine(business_id)
    # autocommit=False, autoflush=False are defaults for sessionmaker but good to be explicit
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return SessionLocal()

# Example of how to ensure environment variables are set if you run this file directly for testing
if __name__ == "__main__":
    # This block is for demonstration/testing if you run this file directly.
    # In a real app, env vars would be set in your deployment environment or .env file.

    # Mock environment variables for testing if not set
    os.environ.setdefault("DB_URL_BIZ_1001", "sqlite:///./test_biz_1001.db") # Use SQLite for local test
    os.environ.setdefault("DB_URL_BIZ_1002", "sqlite:///./test_biz_1002.db")

    # Update DATABASES to use these env vars for the test run
    DATABASES["biz_1001"] = os.getenv("DB_URL_BIZ_1001")
    DATABASES["biz_1002"] = os.getenv("DB_URL_BIZ_1002")

    print("--- Testing get_session with engine caching ---")

    # Test biz_1001
    print("\nTesting for business_id '1001'")
    session_1a = get_session(business_id="1001")
    print(f"Session 1a engine: {session_1a.bind.url}")
    session_1a.close()

    session_1b = get_session(business_id="1001") # Should use cached engine
    print(f"Session 1b engine: {session_1b.bind.url}")
    session_1b.close()

    # Test biz_1002
    print("\nTesting for business_id '1002'")
    session_2 = get_session(business_id="1002")
    print(f"Session 2 engine: {session_2.bind.url}")
    session_2.close()

    # Test biz_1001 again
    print("\nTesting for business_id '1001' again")
    session_1c = get_session(business_id="1001") # Should definitely use cached engine
    print(f"Session 1c engine: {session_1c.bind.url}")
    session_1c.close()

    print(f"\nEngines in cache: {list(_ENGINES.keys())}")

    # Test missing business_id
    print("\nTesting for missing business_id '9999'")
    try:
        get_session(business_id="9999")
    except ValueError as e:
        print(f"Caught expected error: {e}")
    except ConnectionError as e:
        print(f"Caught expected connection error: {e}")
