
from pydantic import BaseModel, Field, validator, constr # Added constr
from typing import Optional, List # Added List

class BrandCsvModel(BaseModel): # Renamed from BrandModel
    """Pydantic model for validating a row from a Brand CSV file."""
    name: constr(strip_whitespace=True, min_length=1) # Was brand_name; this is the key identifier from CSV
    logo: constr(strip_whitespace=True, min_length=1) # Path or URL to the brand logo; mandatory

    supplier_id: Optional[int] = None # Pydantic int for DB BigInteger
    active: Optional[str] = None # e.g., "TRUE", "FALSE", or other status strings, matches DB String(255)

    # Optional audit fields if provided in CSV (DB type is BigInteger)
    # These are often system-generated, but can be accepted from CSV if needed.
    created_by: Optional[int] = None
    created_date: Optional[int] = None # If CSV provides epoch timestamp
    updated_by: Optional[int] = None
    updated_date: Optional[int] = None # If CSV provides epoch timestamp

    # Custom validator for 'name' is no longer needed due to constr.
    # If other specific validations were needed for other fields, they could be added here.

    class Config:
        anystr_strip_whitespace = True
        # extra = "forbid" # If no other columns are allowed from CSV

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


# --- Category CSV Model ---
class CategoryCsvModel(BaseModel):
    """
    Pydantic model representing the structure of a row in a Category CSV file.
    This model is used for validation when processing category CSV uploads.
    """
    category_path: constr(strip_whitespace=True, min_length=1) # e.g., "L1/L2/L3" or "L1"
    name: Optional[str] = None # Name of the current category level, can be derived if not provided
    description: str # Per DDL, description is NOT NULL

    enabled: Optional[bool] = True
    image_name: Optional[str] = None
    long_description: Optional[str] = None
    order_type: Optional[str] = None # Consider Enum if values are fixed
    shipping_type: Optional[str] = None # Consider Enum if values are fixed
    active: Optional[str] = None # DDL has varchar(255). If boolean, CSV needs "true"/"false" or "1"/"0".

    seo_description: Optional[str] = None
    seo_keywords: Optional[str] = None # Example: "keyword1, keyword2, keyword3"
    seo_title: Optional[str] = None
    url: Optional[str] = None # If not auto-generated based on name/path
    position_on_site: Optional[int] = None # DDL has bigint, Pydantic int handles large integers.

    # Audit-like fields from DDL (created_by, created_date, updated_by, updated_date)
    # are typically system-generated by the database or application logic upon save.
    # If these are expected to be *provided* in the CSV, they can be uncommented.
    # Note: DDL has these as bigint; if they represent epoch timestamps, Pydantic 'int' is fine.
    # If they are string dates from CSV, use Optional[str] and parse in service layer.
    # created_by: Optional[int] = None
    # created_date: Optional[int] = None # Or Optional[datetime] if CSV format is parsable
    # updated_by: Optional[int] = None
    # updated_date: Optional[int] = None # Or Optional[datetime]

    class Config:
        anystr_strip_whitespace = True
        # extra = "forbid" # Uncomment if no extra fields are allowed from CSV

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

    # The misplaced 'offer_price' validator that belonged to ProductPriceModel was here.
    # It has been removed from UploadSessionModel.

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
