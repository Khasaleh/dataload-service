from sqlalchemy import (
    Column, Integer, String, DateTime, Float, Boolean, ForeignKey,
    UniqueConstraint, Index, Text, BigInteger # Added BigInteger
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func # For server-side default timestamps

from .base_class import Base
import os # Added os import

# Schema constants (can be imported from a config file or defined here)
# These should align with what's set in app/db/connection.py via environment variables
CATALOG_SCHEMA = os.getenv("CATALOG_SERVICE_SCHEMA", "catalog_management")
BUSINESS_SCHEMA = os.getenv("BUSINESS_SERVICE_SCHEMA", "fazeal_business")
PUBLIC_SCHEMA = "public"

# --- Upload Session Model ---
class UploadSessionOrm(Base):
    __tablename__ = "upload_sessions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    session_id = Column(String, unique=True, index=True, nullable=False)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed from business_id, type BigInteger
    load_type = Column(String, nullable=False)
    original_filename = Column(String, nullable=True)
    wasabi_path = Column(String, nullable=True)
    status = Column(String, default="pending", nullable=False, index=True)
    details = Column(Text, nullable=True)
    record_count = Column(Integer, nullable=True)
    error_count = Column(Integer, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False) # Changed to server_default
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False) # Changed to server_default

    __table_args__ = (
        Index('idx_upload_session_business_status', "business_details_id", "status"),
        Index('idx_upload_session_business_created_at', "business_details_id", "created_at"),
        {"schema": PUBLIC_SCHEMA} # Assuming operational data like this goes to public or a dedicated ops schema
    )

# --- Category Models ---
class CategoryOrm(Base):
    __tablename__ = "categories"

    # DDL provided fields
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    created_by = Column(BigInteger, nullable=True)
    created_date = Column(BigInteger, nullable=True) # DDL: bigint, could be epoch or specific format
    updated_by = Column(BigInteger, nullable=True)
    updated_date = Column(BigInteger, nullable=True) # DDL: bigint

    business_details_id = Column(BigInteger, index=True, nullable=False) # Standardized tenant ID

    # DDL 'created_at' is timestamp, aliasing to avoid conflict if 'created_date' is different
    created_at_ts = Column("created_at", DateTime, server_default=func.now(), nullable=True)

    description = Column(Text, nullable=False) # DDL: text, NOT NULL
    enabled = Column(Boolean, nullable=True, default=True) # Assuming default True is sensible
    image_name = Column(String(500), nullable=True)
    name = Column(String(150), nullable=False) # DDL: varchar(150), NOT NULL

    parent_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.categories.id"), nullable=True) # Self-referencing FK
    parent = relationship("CategoryOrm", remote_side=[id], backref="children", lazy="selectin")

    long_description = Column(Text, nullable=True)
    order_type = Column(String(255), nullable=True)
    shipping_type = Column(String(255), nullable=True)
    active = Column(String(255), nullable=True) # DDL: varchar(255). Consider Boolean if it's 'true'/'false' strings.
    seo_description = Column(String(255), nullable=True)
    seo_keywords = Column(String(255), nullable=True)
    seo_title = Column(String(255), nullable=True)
    url = Column(String(255), nullable=True, unique=True) # DDL implies unique url
    position_on_site = Column(BigInteger, nullable=True)

    category_attributes = relationship("CategoryAttributeOrm", back_populates="category", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('business_details_id', 'name', 'parent_id', name='uq_category_business_name_parent'),
        Index('idx_category_business_name', "business_details_id", "name"),
        {"schema": PUBLIC_SCHEMA}
    )

class CategoryAttributeOrm(Base):
    __tablename__ = "categories_attributes"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    active = Column(String(255), nullable=True) # DDL: varchar(255)
    created_by = Column(BigInteger, nullable=True)
    created_date = Column(BigInteger, nullable=True) # DDL: bigint
    updated_by = Column(BigInteger, nullable=True)
    updated_date = Column(BigInteger, nullable=True) # DDL: bigint
    name = Column(String(255), nullable=True) # Attribute name (e.g., "Color", "Size") for this category

    category_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.categories.id"), nullable=False)
    category = relationship("CategoryOrm", back_populates="category_attributes")

    # If this 'name' should link to a predefined attribute in the 'catalog_management.attributes' table:
    # attribute_definition_id = Column(Integer, ForeignKey(f"{CATALOG_SCHEMA}.attributes.id"), nullable=True) # Optional link
    # attribute_definition = relationship("AttributeOrm")

    __table_args__ = (
        UniqueConstraint('category_id', 'name', name='uq_categoryattribute_category_name'),
        {"schema": PUBLIC_SCHEMA}
    )


# --- CSV Data Entity Models (Updated) ---

class BrandOrm(Base):
    __tablename__ = "brands"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True) # Changed to BigInteger
    business_details_id = Column(BigInteger, index=True, nullable=False) # Confirmed BigInteger
    name = Column(String(150), index=True, nullable=False) # Changed from brand_name, added length

    logo = Column(String(500), nullable=False) # New field
    supplier_id = Column(BigInteger, nullable=True) # New field
    active = Column(String(255), nullable=True) # New field, consider Boolean if values are 'true'/'false'

    # Audit fields as per DDL (BigInteger type)
    created_by = Column(BigInteger, nullable=True) # New field
    created_date = Column(BigInteger, nullable=True) # New field
    updated_by = Column(BigInteger, nullable=True) # New field
    updated_date = Column(BigInteger, nullable=True) # New field

    # Removing old created_at, updated_at if DDL's created_date/updated_date are the source
    # created_at = Column(DateTime, server_default=func.now(), nullable=False)
    # updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    products = relationship("ProductOrm", back_populates="brand")

    __table_args__ = (
        UniqueConstraint('business_details_id', 'name', name='uq_brand_business_name'), # Updated to 'name'
        {"schema": CATALOG_SCHEMA}
    )

class AttributeOrm(Base):
    __tablename__ = "attribute" # Changed from "attributes" to match DDL

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True) # Changed to BigInteger
    business_details_id = Column(BigInteger, index=True, nullable=False) # Confirmed
    name = Column(String(150), index=True, nullable=False) # Renamed from attribute_name, added length

    is_color = Column(Boolean, nullable=False, server_default=sa.false()) # New field
    active = Column(String(255), nullable=True) # New field

    # Audit fields as per DDL
    created_by = Column(BigInteger, nullable=True)
    created_date = Column(BigInteger, nullable=True)
    updated_by = Column(BigInteger, nullable=True)
    updated_date = Column(BigInteger, nullable=True)

    # Removed old created_at, updated_at DateTime fields
    # created_at = Column(DateTime, server_default=func.now(), nullable=False)
    # updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    attribute_values = relationship("AttributeValueOrm", back_populates="attribute", cascade="all, delete-orphan", lazy="selectin")

    __table_args__ = (
        UniqueConstraint('business_details_id', 'name', name='uq_attribute_business_name'), # Updated to 'name'
        {"schema": CATALOG_SCHEMA}
    )

class AttributeValueOrm(Base):
    __tablename__ = "attribute_value"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    value = Column(String(150), nullable=False) # The actual underlying value

    attribute_id = Column(BigInteger, ForeignKey(f'{CATALOG_SCHEMA}.attribute.id'), nullable=False)
    attribute = relationship("AttributeOrm", back_populates="attribute_values")

    name = Column(String(150), nullable=True) # Display name for the value

    attribute_image_url = Column(String(255), nullable=True)
    active = Column(String(255), nullable=True, server_default="INACTIVE")

    created_by = Column(BigInteger, nullable=True)
    created_date = Column(BigInteger, nullable=True)
    updated_by = Column(BigInteger, nullable=True)
    updated_date = Column(BigInteger, nullable=True)
    logo_name = Column(String(500), nullable=True)

    __table_args__ = (
        UniqueConstraint('attribute_id', 'name', name='uq_attribute_value_attribute_id_name'),
        UniqueConstraint('attribute_id', 'value', name='uq_attribute_value_attribute_id_value'),
        {"schema": CATALOG_SCHEMA}
    )

class ReturnPolicyOrm(Base):
    __tablename__ = "return_policies"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed
    return_policy_code = Column(String, index=True, nullable=False)
    name = Column(String, nullable=False)
    return_window_days = Column(Integer, nullable=False)
    grace_period_days = Column(Integer, nullable=False, default=0)
    description = Column(Text, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    products = relationship("ProductOrm", back_populates="return_policy")

    __table_args__ = (
        UniqueConstraint('business_details_id', 'return_policy_code', name='uq_returnpolicy_business_code'),
        {"schema": BUSINESS_SCHEMA} # Assigned schema
    )

class ProductOrm(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed
    product_name = Column(String, index=True, nullable=False)
    product_url = Column(String, nullable=True)

    brand_id = Column(Integer, ForeignKey(f'{CATALOG_SCHEMA}.brands.id'), nullable=False) # Schema-qualified FK
    brand = relationship("BrandOrm", back_populates="products")

    category_path = Column(String, nullable=True, index=True)

    return_policy_id = Column(Integer, ForeignKey(f'{BUSINESS_SCHEMA}.return_policies.id'), nullable=False) # Schema-qualified FK
    return_policy = relationship("ReturnPolicyOrm", back_populates="products")

    package_length = Column(Float, nullable=True)
    package_width = Column(Float, nullable=True)
    package_height = Column(Float, nullable=True)
    package_weight = Column(Float, nullable=True)
    status = Column(String, nullable=True, index=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    items = relationship("ProductItemOrm", back_populates="product", cascade="all, delete-orphan")
    prices = relationship("ProductPriceOrm", back_populates="product", cascade="all, delete-orphan") # Should be one-to-one based on ProductPriceOrm.product_id unique
    meta_tag = relationship("MetaTagOrm", uselist=False, back_populates="product", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('business_details_id', 'product_name', name='uq_product_business_name'),
        Index('idx_product_business_status', "business_details_id", "status"),
        {"schema": CATALOG_SCHEMA} # Assigned schema
    )

class ProductItemOrm(Base):
    __tablename__ = "product_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed

    product_id = Column(Integer, ForeignKey(f'{CATALOG_SCHEMA}.products.id'), nullable=False) # Schema-qualified FK
    product = relationship("ProductOrm", back_populates="items")

    variant_sku = Column(String, index=True, nullable=False)
    attribute_combination = Column(Text, nullable=True)
    status = Column(String, nullable=True, index=True)
    published = Column(Boolean, default=False, nullable=False)
    default_sku = Column(Boolean, default=False, nullable=False)
    quantity = Column(Integer, default=0, nullable=False)
    image_urls = Column(Text, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('business_details_id', 'variant_sku', name='uq_item_business_sku'),
        Index('idx_item_product_id', "product_id"), # Kept manual name for index
        {"schema": CATALOG_SCHEMA} # Assigned schema
    )

class ProductPriceOrm(Base):
    __tablename__ = "product_prices"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed

    product_id = Column(Integer, ForeignKey(f'{CATALOG_SCHEMA}.products.id'), nullable=False, unique=True)
    product = relationship("ProductOrm", back_populates="prices")

    price = Column(Float, nullable=False)
    cost_per_item = Column(Float, nullable=True)
    offer_price = Column(Float, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        # UniqueConstraint('business_details_id', 'product_id', name='uq_price_business_product'), # product_id is already unique
        {"schema": CATALOG_SCHEMA} # Assigned schema
    )

class MetaTagOrm(Base):
    __tablename__ = "meta_tags"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed

    product_id = Column(Integer, ForeignKey(f'{CATALOG_SCHEMA}.products.id'), nullable=False, unique=True)
    product = relationship("ProductOrm", back_populates="meta_tag")

    meta_title = Column(String(255), nullable=True) # Specify length for String if appropriate
    meta_keywords = Column(Text, nullable=True)
    meta_description = Column(Text, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        # UniqueConstraint('business_details_id', 'product_id', name='uq_metatag_business_product'), # product_id is already unique
        {"schema": CATALOG_SCHEMA} # Assigned schema
    )
```
