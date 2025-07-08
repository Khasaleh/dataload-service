import sqlalchemy as sa # Added for sa.false()
from sqlalchemy import (
    Column, Integer, String, DateTime, Float, Boolean, ForeignKey,
    UniqueConstraint, Index, Text, BigInteger # Added BigInteger
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func # For server-side default timestamps
# from sqlalchemy.sql.expression import nextval # Removed incorrect import
from app.db.schema_names import PUBLIC_SCHEMA
from .base_class import Base
import os # Added os import
from app.models.shopping_category import ShoppingCategoryOrm

# Schema constants (can be imported from a config file or defined here)
# These should align with what's set in app/db/connection.py via environment variables
CATALOG_SCHEMA = os.getenv("CATALOG_SERVICE_SCHEMA", "catalog_management")
BUSINESS_SCHEMA = os.getenv("BUSINESS_SERVICE_SCHEMA", "fazeal_business")


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


# --- Business Details Model ---
class BusinessDetailsOrm(Base):
    __tablename__ = "business_details"
    __table_args__ = (
        {"schema": PUBLIC_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)

    # Existing relationships
    return_policies = relationship(
        "ReturnPolicyOrm",
        back_populates="business_detail"
    )

    # Add this relationship to match ShoppingCategoryOrm
    shopping_categories = relationship(
        "ShoppingCategoryOrm",
        back_populates="business_detail",
        cascade="all, delete-orphan"
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
    seo_description = Column(String(255), nullable=True) # Max 255 chars
    seo_keywords = Column(String(255), nullable=True) # Max 255 chars
    seo_title = Column(String(255), nullable=True) # Max 255 chars
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

    # products = relationship("ProductOrm", back_populates="brand") # ProductOrm DDL does not have brand_id

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
    __tablename__ = "return_policy"
    __table_args__ = (
        UniqueConstraint(
            'business_details_id',
            'policy_name',
            name='uq_return_policy_business_name'
        ),
        Index(
            'idx_return_policy_lookup',
            "business_details_id",
            "return_policy_type"
        ),
        {"schema": PUBLIC_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)

    # Audit timestamps
    created_date = Column(DateTime, server_default=func.now(), nullable=False)
    updated_date = Column(DateTime, onupdate=func.now(), nullable=True)

    # Core business fields
    policy_name         = Column(Text, nullable=False, index=True)
    return_policy_type  = Column(
        String(255),
        nullable=False,
        index=True,
        doc="e.g. SALES_RETURN_ALLOWED or SALES_ARE_FINAL"
    )
    grace_period_return = Column(
        Integer,
        nullable=True,
        doc="Number of days (NULL if blank in CSV)"
    )
    time_period_return  = Column(
        Integer,
        nullable=True,
        doc="Number of days (NULL if blank in CSV)"
    )

    business_details_id = Column(
        BigInteger,
        ForeignKey(f"{PUBLIC_SCHEMA}.business_details.id"),
        nullable=False,
        index=True
    )

    # Relationships
    business_detail = relationship(
        "BusinessDetailsOrm",
        back_populates="return_policies"
    )
    products = relationship(
        "ProductOrm",
        back_populates="return_policy"
    )
# --- Shopping Category Model (Basic for FK reference) ---
# This will be defined in a separate file app/models/shopping_category.py
from app.models.shopping_category import ShoppingCategoryOrm # Import for relationship

# class ShoppingCategoryOrm(Base):
#     __tablename__ = "shopping_categories"
#     id = Column(BigInteger, primary_key=True)
#     name = Column(String(150), nullable=False, unique=True) # Assuming name is unique for lookup
#     # Other fields from DDL: created_at, updated_at, parent_id, business_type
#     __table_args__ = ({"schema": PUBLIC_SCHEMA})


# --- Product Model (Revised based on DDL) ---
class ProductOrm(Base):
    __tablename__ = "products"
    __table_args__ = ({"schema": PUBLIC_SCHEMA}) # DDL implies public schema

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True) # DDL: bigint, nextval handled by DB
    created_by = Column(BigInteger, nullable=True)
    created_date = Column(BigInteger, nullable=True) # DDL: bigint (epoch or similar)
    updated_by = Column(BigInteger, nullable=True)
    updated_date = Column(BigInteger, nullable=True) # DDL: bigint

    barcode = Column(Text, nullable=False) # DDL: text NOT NULL
    brand_name = Column(String(256), nullable=True) # DDL: character varying(256) - Storing name, linking via brand_id
    business_details_id = Column(BigInteger, nullable=False, index=True) # DDL: bigint NOT NULL
    description = Column(Text, nullable=False) # DDL: text NOT NULL
    is_child_item = Column(Integer, nullable=True) # DDL: integer
    name = Column(String(256), nullable=False, index=True) # DDL: character varying(256) NOT NULL
    product_type_status = Column(Integer, nullable=True) # DDL: integer
    self_gen_product_id = Column(String(256), nullable=False, index=True) # DDL: character varying(256) NOT NULL

    category_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.categories.id"), nullable=False, index=True) # DDL: bigint NOT NULL
    category = relationship("CategoryOrm") # Relationship to CategoryOrm

    discount = Column(Float, nullable=True) # DDL: real
    price = Column(Float, nullable=True) # DDL: real
    sale_price = Column(Float, nullable=True) # DDL: real
    quantity = Column(BigInteger, nullable=True) # DDL: bigint
    size_chart_image = Column(String(255), nullable=True)
    product_dimentions = Column(Text, nullable=True) # Note: DDL typo "dimentions"
    product_weight = Column(String(255), nullable=True)
    cost_price = Column(Float, nullable=True) # DDL: real
    active = Column(String(255), nullable=True, index=True) # DDL: character varying(255)
    ean = Column(String(256), nullable=True)
    isbn = Column(String(256), nullable=True)
    keywords = Column(String(512), nullable=True) # Max 512 chars
    mpn = Column(String(256), nullable=True)
    seo_description = Column(String(512), nullable=True) # Max 512 chars
    seo_title = Column(String(256), nullable=True) # Max 256 chars
    upc = Column(String(256), nullable=True)
    url = Column(String(256), nullable=True, index=True) # DDL: character varying(256)

    shopping_category_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.shopping_categories.id"), nullable=True, index=True) # DDL: bigint
    shopping_category = relationship("ShoppingCategoryOrm") # Relationship to ShoppingCategoryOrm

    package_size_height = Column(Float, nullable=True) # DDL: real
    package_size_length = Column(Float, nullable=True) # DDL: real
    package_size_width = Column(Float, nullable=True) # DDL: real
    product_weights = Column(Float, nullable=True) # DDL: real (Note: CSV has 'product_weights', DDL 'product_weights')
    size_unit = Column(String(255), nullable=True)
    weight_unit = Column(String(255), nullable=True)
    average_review = Column(Float, default=0) # DDL: double precision DEFAULT 0
    review_count = Column(BigInteger, default=0) # DDL: bigint DEFAULT 0
    is_item_package_dimentions = Column(Boolean, default=False) # DDL: boolean DEFAULT false (Note: DDL typo "dimentions")
    is_item_level_weight = Column(Boolean, default=False) # DDL: boolean DEFAULT false
    order_limit = Column(BigInteger, nullable=True)

    # Return Policy Fields from Product DDL
    return_fee = Column(Float, nullable=True) # DDL: real
    return_fee_type = Column(String(255), nullable=True) # DDL: character varying(255)
    return_policy_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.return_policy.id"), nullable=True, index=True) # DDL: bigint
    return_type = Column(String(255), nullable=True) # DDL: character varying(255)

    video_url = Column(String(255), nullable=True)
    main_image_url = Column(String(255), nullable=True) # Main image for the product itself
    mobile_barcode = Column(Text, nullable=True)
    highest_price = Column(Float, default=0) # DDL: double precision DEFAULT 0
    thumbnail_url = Column(String(255), nullable=True)

    # Relationships
    # Brand relationship: CSV provides brand_name, we'll look up brand_id.
    # The products table DDL does not have a direct brand_id FK.
    # If linking to brands is required, it might be conceptual or via brand_name.
    # For now, removing direct brand_id FK from ProductOrm as per its DDL.
    # brand_id = Column(BigInteger, ForeignKey(f'{CATALOG_SCHEMA}.brands.id'), nullable=True)
    # brand = relationship("BrandOrm", back_populates="products")

    # ReturnPolicyOrm relationship (if we link via return_policy_id)
    return_policy = relationship("ReturnPolicyOrm", back_populates="products")

    # Product Images (for is_child_item = 0)
    images = relationship("ProductImageOrm", back_populates="product", cascade="all, delete-orphan")

    # Product Specifications
    specifications = relationship("ProductSpecificationOrm", back_populates="product", cascade="all, delete-orphan")

    # Relationship to items (SKUs) - Assuming ProductItemOrm will be defined/updated later for is_child_item = 1
    items = relationship("ProductItemOrm", back_populates="product", cascade="all, delete-orphan")

    # Removing old relationships that are not in the new DDL context
    legacy_prices = relationship("ProductPriceOrm", back_populates="product", cascade="all, delete-orphan")
    meta_tag = relationship("MetaTagOrm", uselist=False, back_populates="product", cascade="all, delete-orphan")

    # Constraints from DDL: products_pkey PRIMARY KEY (id)
    # fkc6f144ia6250x7b32f06ofd6o FOREIGN KEY (shopping_category_id) REFERENCES public.shopping_categories (id)
    # fkog2rp4qthbtt2lfyhfo32lsw9 FOREIGN KEY (category_id) REFERENCES public.categories (id)
    # (Return policy FK is not explicitly named in DDL but implied by return_policy_id column)

    # Unique constraints based on common practice or previous definitions if applicable
    # For example: UniqueConstraint('business_details_id', 'self_gen_product_id', name='uq_product_business_self_gen_id')
    # UniqueConstraint('business_details_id', 'name', name='uq_product_business_name') - if name is unique per business

    prices = relationship("PriceOrm", back_populates="product", cascade="all, delete-orphan", foreign_keys="[PriceOrm.product_id]")


# --- Product Image Model ---
class ProductImageOrm(Base):
    __tablename__ = "product_images"
    __table_args__ = ({"schema": PUBLIC_SCHEMA}) # DDL implies public schema

    # DDL: id integer NOT NULL DEFAULT nextval('product_images_id_seq'::regclass)
    id = Column(Integer, primary_key=True, index=True, autoincrement=True) # nextval handled by DB

    name = Column(String(255), nullable=False) # URL or path to the image
    product_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.products.id"), nullable=True, index=True) # Nullable if linked to SKU
    main_image = Column(Boolean, default=False, nullable=True)

    # DDL: main_sku_id bigint (references public.main_skus.id) - Will handle when main_skus/items are implemented
    # main_sku_id = Column(BigInteger, nullable=True) # ForeignKey to main_skus.id will be added later
    # For Price MVP, linking ProductImageOrm directly to main_skus might be out of scope if main_skus is not yet fully defined/used.
    # Temporarily commenting out main_sku_id if it's not immediately used or causes FK issues without a main_skus table.
    main_sku_id = Column(BigInteger, ForeignKey(f'{CATALOG_SCHEMA}.product_items.id', name='fk_product_images_main_sku_id', use_alter=True), nullable=True, index=True)


    active = Column(String(255), nullable=True) # DDL: character varying(255)
    created_by = Column(BigInteger, nullable=True)
    created_date = Column(BigInteger, nullable=True)
    updated_by = Column(BigInteger, nullable=True)
    updated_date = Column(BigInteger, nullable=True)

    product = relationship("ProductOrm", back_populates="images")

    #CONSTRAINT fk3jss75rmr7hpgc0m8iptl84tw FOREIGN KEY (main_sku_id) REFERENCES public.main_skus (id)
    #CONSTRAINT fkqnq71xsohugpqwf3c9gxmsuy FOREIGN KEY (product_id) REFERENCES public.products (id)

# --- Product Specification Model ---
class ProductSpecificationOrm(Base):
    __tablename__ = "product_specification"
    __table_args__ = ({"schema": PUBLIC_SCHEMA}) # DDL implies public schema

    # DDL: id bigint NOT NULL DEFAULT nextval('product_specification_id_seq'::regclass)
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True) # nextval handled by DB

    name = Column(String(255), nullable=True) # Specification name (e.g., "Color", "Material")
    value = Column(Text, nullable=True) # Specification value (e.g., "Red", "Cotton")

    product_id = Column(BigInteger, ForeignKey(f"{PUBLIC_SCHEMA}.products.id"), nullable=True, index=True)

    active = Column(String(255), nullable=True) # DDL: character varying(255)
    created_by = Column(BigInteger, nullable=True)
    created_date = Column(BigInteger, nullable=True)
    updated_by = Column(BigInteger, nullable=True)
    updated_date = Column(BigInteger, nullable=True)

    product = relationship("ProductOrm", back_populates="specifications")
    # CONSTRAINT fkcshw23fru6i7sk0p9qq8fu4gt FOREIGN KEY (product_id) REFERENCES public.products (id)


class ProductItemOrm(Base):
    __tablename__ = "product_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed

    product_id = Column(BigInteger, ForeignKey(f'{PUBLIC_SCHEMA}.products.id'), nullable=False) # Corrected schema to PUBLIC and type to BigInteger
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

    prices = relationship("PriceOrm", back_populates="sku", cascade="all, delete-orphan", foreign_keys="[PriceOrm.sku_id]")


class PriceOrm(Base):
    __tablename__ = "prices"
    __table_args__ = ({"schema": CATALOG_SCHEMA})

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False)

    product_id = Column(BigInteger, ForeignKey(f'{PUBLIC_SCHEMA}.products.id'), nullable=True, index=True)
    product = relationship("ProductOrm", back_populates="prices", foreign_keys=[product_id])

    sku_id = Column(Integer, ForeignKey(f'{CATALOG_SCHEMA}.product_items.id'), nullable=True, index=True)
    sku = relationship("ProductItemOrm", back_populates="prices", foreign_keys=[sku_id])

    price = Column(Float, nullable=False)
    discount_price = Column(Float, nullable=True)
    cost_price = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True, default="USD") # Default currency

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('product_id', name='uq_price_product_id'), # A product can only have one direct price entry
        UniqueConstraint('sku_id', name='uq_price_sku_id'),         # An SKU can only have one price entry
        # Ensure that either product_id or sku_id is set, but not both (handled by check constraint or application logic)
        # CheckConstraint('(product_id IS NOT NULL AND sku_id IS NULL) OR (product_id IS NULL AND sku_id IS NOT NULL)', name='cc_price_target_exclusive'),
        # The above CheckConstraint might be too restrictive if you want to allow prices not linked to product/SKU initially.
        # Application logic should enforce this based on price_type.
        Index('idx_price_business_product', "business_details_id", "product_id", unique=True, postgresql_where=sa.text("product_id IS NOT NULL")),
        Index('idx_price_business_sku', "business_details_id", "sku_id", unique=True, postgresql_where=sa.text("sku_id IS NOT NULL")),
        {"schema": CATALOG_SCHEMA}
    )


class ProductPriceOrm(Base):
    __tablename__ = "product_prices"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    business_details_id = Column(BigInteger, index=True, nullable=False) # Changed

    product_id = Column(BigInteger, ForeignKey(f'{PUBLIC_SCHEMA}.products.id'), nullable=False, unique=True) # Corrected schema and type
    product = relationship("ProductOrm", back_populates="legacy_prices")

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

    product_id = Column(BigInteger, ForeignKey(f'{PUBLIC_SCHEMA}.products.id'), nullable=False, unique=True) # Corrected schema and type
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
