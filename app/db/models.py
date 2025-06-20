from sqlalchemy import (
    Column, Integer, String, DateTime, Float, Boolean, ForeignKey,
    UniqueConstraint, Index, Text
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func # For server-side default timestamps if preferred over Python's datetime.utcnow

from .base_class import Base
import datetime # For default client-side timestamps

# --- Upload Session Model ---
class UploadSessionOrm(Base):
    __tablename__ = "upload_sessions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True) # Auto P_K for the session record itself
    session_id = Column(String, unique=True, index=True, nullable=False) # The UUID session_id (from Python)
    business_id = Column(String, index=True, nullable=False)
    load_type = Column(String, nullable=False)
    original_filename = Column(String, nullable=True)
    wasabi_path = Column(String, nullable=True)
    status = Column(String, default="pending", nullable=False, index=True)
    details = Column(Text, nullable=True) # Using Text for potentially longer error messages
    record_count = Column(Integer, nullable=True)
    error_count = Column(Integer, nullable=True)

    # Timestamps:
    # default=datetime.datetime.utcnow for client-side default
    # default=func.now() for server-side default (requires importing func from sqlalchemy.sql)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)

    __table_args__ = (
        Index('idx_upload_session_business_status', "business_id", "status"),
        # Consider an index on business_id and created_at for querying recent sessions
        Index('idx_upload_session_business_created_at', "business_id", "created_at"),
    )

# --- CSV Data Entity Models ---

class BrandOrm(Base):
    __tablename__ = "brands"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_id = Column(String, index=True, nullable=False)
    brand_name = Column(String, index=True, nullable=False)
    # Add other fields from Pydantic BrandModel if any

    # Relationships
    products = relationship("ProductOrm", back_populates="brand")

    __table_args__ = (
        UniqueConstraint('business_id', 'brand_name', name='uq_brand_business_name'),
    )

class AttributeOrm(Base):
    __tablename__ = "attributes"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_id = Column(String, index=True, nullable=False)
    attribute_name = Column(String, index=True, nullable=False)
    allowed_values = Column(Text, nullable=True) # Storing as Text; could be JSON if DB supports and it's complex

    __table_args__ = (
        UniqueConstraint('business_id', 'attribute_name', name='uq_attribute_business_name'),
    )

class ReturnPolicyOrm(Base):
    __tablename__ = "return_policies"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_id = Column(String, index=True, nullable=False)
    return_policy_code = Column(String, index=True, nullable=False)
    name = Column(String, nullable=False)
    return_window_days = Column(Integer, nullable=False)
    grace_period_days = Column(Integer, nullable=False, default=0)
    description = Column(Text, nullable=True)
    # Add other fields from Pydantic ReturnPolicyModel

    # Relationships
    products = relationship("ProductOrm", back_populates="return_policy")

    __table_args__ = (
        UniqueConstraint('business_id', 'return_policy_code', name='uq_returnpolicy_business_code'),
    )

class ProductOrm(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_id = Column(String, index=True, nullable=False)
    product_name = Column(String, index=True, nullable=False) # Unique key for product within a business
    product_url = Column(String, nullable=True)

    brand_id = Column(Integer, ForeignKey('brands.id'), nullable=False)
    brand = relationship("BrandOrm", back_populates="products")

    category_path = Column(String, nullable=True, index=True) # Often used for filtering/grouping

    return_policy_id = Column(Integer, ForeignKey('return_policies.id'), nullable=False)
    return_policy = relationship("ReturnPolicyOrm", back_populates="products")

    package_length = Column(Float, nullable=True)
    package_width = Column(Float, nullable=True)
    package_height = Column(Float, nullable=True)
    package_weight = Column(Float, nullable=True)
    status = Column(String, nullable=True, index=True) # e.g., "active", "draft"

    # Relationships to child tables
    items = relationship("ProductItemOrm", back_populates="product", cascade="all, delete-orphan")
    prices = relationship("ProductPriceOrm", back_populates="product", cascade="all, delete-orphan")
    meta_tag = relationship("MetaTagOrm", uselist=False, back_populates="product", cascade="all, delete-orphan") # One-to-one

    __table_args__ = (
        UniqueConstraint('business_id', 'product_name', name='uq_product_business_name'),
        Index('idx_product_business_status', "business_id", "status"),
    )

class ProductItemOrm(Base):
    __tablename__ = "product_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_id = Column(String, index=True, nullable=False) # For direct querying by business_id & denormalization

    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    product = relationship("ProductOrm", back_populates="items")

    variant_sku = Column(String, index=True, nullable=False)
    attribute_combination = Column(Text, nullable=True) # e.g., "Color=Red,Size=M"
    status = Column(String, nullable=True, index=True)
    published = Column(Boolean, default=False, nullable=False)
    default_sku = Column(Boolean, default=False, nullable=False)
    quantity = Column(Integer, default=0, nullable=False)
    image_urls = Column(Text, nullable=True) # Could be JSON or comma-separated string

    __table_args__ = (
        UniqueConstraint('business_id', 'variant_sku', name='uq_item_business_sku'),
        # product_id is already indexed by ForeignKey, but explicit index can be added if heavily queried.
        # Index('idx_item_product_id', "product_id"),
    )

class ProductPriceOrm(Base):
    __tablename__ = "product_prices"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_id = Column(String, index=True, nullable=False) # Denormalized for easier querying

    product_id = Column(Integer, ForeignKey('products.id'), nullable=False, unique=True) # Assuming one price entry per product for simplicity
                                                                                        # If multiple price types/versions, remove unique=True
                                                                                        # and add a price_type field.
    product = relationship("ProductOrm", back_populates="prices") # Should be one-to-one if product_id is unique

    # product_name is from ProductOrm via relationship if needed, not duplicated here typically.
    # If this table represents price for a ProductItem (variant), then FK should be to product_items.id

    price = Column(Float, nullable=False)
    cost_per_item = Column(Float, nullable=True) # Cost might not always be available
    offer_price = Column(Float, nullable=True)

    # __table_args__ might include UniqueConstraint on (business_id, product_id) if not covered by unique on product_id.
    # If product_id is unique, that suffices.

class MetaTagOrm(Base):
    __tablename__ = "meta_tags"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_id = Column(String, index=True, nullable=False) # Denormalized for easier querying

    product_id = Column(Integer, ForeignKey('products.id'), nullable=False, unique=True) # One-to-one with Product
    product = relationship("ProductOrm", back_populates="meta_tag")

    # product_name is from ProductOrm via relationship.
    meta_title = Column(String, nullable=True)
    meta_keywords = Column(Text, nullable=True)
    meta_description = Column(Text, nullable=True)

    # __table_args__ if needed, e.g. UniqueConstraint on (business_id, product_id) if product_id wasn't unique by itself.
```
