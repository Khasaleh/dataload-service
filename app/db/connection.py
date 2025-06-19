from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# Example connection registry (could come from DB or config service)
DATABASES = {
    "biz_1001": "postgresql://user:pass@host1:5432/catalog_1001",
    "biz_1002": "postgresql://user:pass@host2:5432/catalog_1002",
    "biz_1003": "postgresql://user:pass@host3:5432/catalog_1003",
}

def get_engine(business_id: str):
    db_url = DATABASES.get(f"biz_{business_id}")
    if not db_url:
        raise Exception(f"No database configuration found for business_id={business_id}")
    return create_engine(db_url, pool_pre_ping=True)

def get_session(business_id: str):
    engine = get_engine(business_id)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return SessionLocal()
