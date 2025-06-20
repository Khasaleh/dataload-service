from sqlalchemy import text
from app.db.connection import get_session
import redis
import os

def get_redis():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=0,
        decode_responses=True
    )

def get_or_create_brand(session, business_id, brand_name):
    stmt = text("SELECT id FROM brands WHERE brand_name=:brand_name AND business_id=:business_id")
    result = session.execute(stmt, {"brand_name": brand_name, "business_id": business_id}).fetchone()
    if result:
        return result[0]
    stmt = text("""
        INSERT INTO brands (business_id, brand_name) 
        VALUES (:business_id, :brand_name) 
        RETURNING id
    """)
    result = session.execute(stmt, {"business_id": business_id, "brand_name": brand_name}).fetchone()
    return result[0]

def get_or_create_attribute(session, business_id, attribute_name, allowed_values):
    stmt = text("""
        SELECT id FROM attributes 
        WHERE attribute_name=:attribute_name AND business_id=:business_id
    """)
    result = session.execute(stmt, {"attribute_name": attribute_name, "business_id": business_id}).fetchone()
    if result:
        return result[0]
    stmt = text("""
        INSERT INTO attributes (business_id, attribute_name, allowed_values)
        VALUES (:business_id, :attribute_name, :allowed_values)
        RETURNING id
    """)
    result = session.execute(stmt, {
        "business_id": business_id,
        "attribute_name": attribute_name,
        "allowed_values": allowed_values
    }).fetchone()
    return result[0]

def get_or_create_return_policy(session, business_id, return_policy_code, name):
    stmt = text("""
        SELECT id FROM return_policies 
        WHERE return_policy_code=:return_policy_code AND business_id=:business_id
    """)
    result = session.execute(stmt, {"return_policy_code": return_policy_code, "business_id": business_id}).fetchone()
    if result:
        return result[0]
    stmt = text("""
        INSERT INTO return_policies (business_id, return_policy_code, name)
        VALUES (:business_id, :return_policy_code, :name)
        RETURNING id
    """)
    result = session.execute(stmt, {
        "business_id": business_id,
        "return_policy_code": return_policy_code,
        "name": name
    }).fetchone()
    return result[0]
