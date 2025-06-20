
from pydantic import BaseModel, Field, validator
from typing import Optional

class BrandModel(BaseModel):
    brand_name: str

    @validator('brand_name')
    def brand_name_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('brand_name must not be empty')
        return value

class AttributeModel(BaseModel):
    attribute_name: str
    allowed_values: str

    @validator('attribute_name', 'allowed_values')
    def attribute_fields_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('field must not be empty')
        return value

class ReturnPolicyModel(BaseModel):
    return_policy_code: str
    name: str
    return_window_days: int
    grace_period_days: int
    description: str

    @validator('return_policy_code', 'name')
    def policy_fields_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('field must not be empty')
        return value

    @validator('return_window_days')
    def return_window_days_must_be_positive(cls, value):
        if value <= 0:
            raise ValueError('return_window_days must be positive')
        return value

    @validator('grace_period_days')
    def grace_period_days_must_be_non_negative(cls, value):
        if value < 0:
            raise ValueError('grace_period_days must be non-negative')
        return value

class ProductModel(BaseModel):
    product_name: str
    product_url: str
    brand_name: str
    category_path: str
    return_policy_code: str
    package_length: float
    package_width: float
    package_height: float
    package_weight: float
    status: str

    @validator('product_name', 'product_url', 'brand_name', 'category_path', 'return_policy_code', 'status')
    def product_text_fields_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('field must not be empty')
        return value

    @validator('package_length', 'package_width', 'package_height', 'package_weight')
    def package_dimensions_must_be_positive(cls, value):
        if value <= 0:
            raise ValueError('package dimension/weight must be positive')
        return value

class ProductItemModel(BaseModel):
    product_name: str
    variant_sku: str
    attribute_combination: str
    status: str
    published: str
    default_sku: str
    quantity: int
    image_urls: Optional[str]

    @validator('product_name', 'variant_sku', 'attribute_combination', 'status', 'published', 'default_sku')
    def item_text_fields_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('field must not be empty')
        return value

    @validator('quantity')
    def quantity_must_be_non_negative(cls, value):
        if value < 0:
            raise ValueError('quantity must be non-negative')
        return value

class ProductPriceModel(BaseModel):
    product_name: str
    price: float
    cost_per_item: float
    offer_price: Optional[float]

    @validator('product_name')
    def price_product_name_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('product_name must not be empty')
        return value

from datetime import datetime
import uuid

class UploadSessionModel(BaseModel):
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    business_id: str
    load_type: str  # e.g., "products", "brands", "attributes", "return_policies", "product_items", "product_prices", "meta_tags"
    original_filename: str
    wasabi_path: str
    status: str = "pending"  # e.g., "pending", "processing", "validation_failed", "completed", "failed"
    details: Optional[str] = None
    record_count: Optional[int] = None
    error_count: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @validator('load_type')
    def load_type_must_be_valid(cls, value):
        valid_load_types = {"brands", "attributes", "return_policies", "products", "product_items", "product_prices", "meta_tags"}
        if value not in valid_load_types:
            raise ValueError(f"Invalid load_type: {value}. Must be one of {valid_load_types}")
        return value

    # Optional: A validator to auto-update updated_at, though this is often handled by ORM events or DB defaults
    # For Pydantic, if you re-validate or create a new model from an old one, this could be useful.
    # However, for simple status updates, the route handler might explicitly set updated_at.
    # For now, default_factory on field definition is okay for creation time.
    # If the model is loaded from DB and then saved, `updated_at` should be set by the application logic.

    class Config:
        orm_mode = True # if you ever use this with an ORM like SQLAlchemy

    @validator('offer_price')
    def offer_price_must_be_positive_if_present(cls, value):
        if value is not None and value <= 0:
            raise ValueError('offer_price must be positive if present')
        return value

class MetaTagModel(BaseModel):
    product_name: str
    meta_title: Optional[str]
    meta_keywords: Optional[str]
    meta_description: Optional[str]

    @validator('product_name')
    def meta_product_name_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('product_name must not be empty')
        return value
