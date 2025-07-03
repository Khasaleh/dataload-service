from pydantic import BaseModel, Field, validator, constr, root_validator
from typing import Optional, List, Any
from datetime import datetime
from enum import Enum
class UploadSessionModel(BaseModel):
    session_id: str
    business_details_id: int
    load_type: str
    original_filename: str
    wasabi_path: str
    status: str
    details: Optional[str] = None
    record_count: Optional[int] = None
    error_count: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    
class CategoryCsvModel(BaseModel):
    category_path: constr(strip_whitespace=True, min_length=1)
    name: constr(strip_whitespace=True, min_length=1)
    description: Optional[str] = None
    enabled: bool = True
    image_name: Optional[str] = None
    long_description: Optional[str] = None
    order_type: Optional[str] = None
    shipping_type: Optional[str] = None
    active: Optional[bool] = True
    seo_description: Optional[str] = None
    seo_keywords: Optional[str] = None
    seo_title: Optional[str] = None
    url: Optional[str] = None
    position_on_site: Optional[int] = None

    class Config:
        anystr_strip_whitespace = True

    @validator("category_path")
    def must_be_slash_separated(cls, v):
        segments = [seg for seg in v.split("/") if seg]
        if not segments:
            raise ValueError("category_path must contain at least one segment")
        return v

    @validator("name")
    def name_must_match_last_path_segment(cls, v, values):
        path = values.get("category_path")
        if path:
            last = path.split("/")[-1]
            if v.strip().lower() != last.strip().lower():
                raise ValueError(f"name '{v}' must equal last segment of category_path '{last}'")
        return v
class BrandCsvModel(BaseModel):
    """Pydantic model for validating a row from a Brand CSV file."""
    name: constr(strip_whitespace=True, min_length=1)
    logo: constr(strip_whitespace=True, min_length=1)
    supplier_id: Optional[int] = None
    active: Optional[str] = None
    created_by: Optional[int] = None
    created_date: Optional[int] = None
    updated_by: Optional[int] = None
    updated_date: Optional[int] = None

    class Config:
        anystr_strip_whitespace = True

class AttributeCsvModel(BaseModel):
    """Pydantic model for validating a row from an Attributes CSV file."""
    attribute_name: constr(strip_whitespace=True, min_length=1)
    is_color: bool = False
    attribute_active: Optional[str] = None
    values_name: Optional[str] = None
    value_value: Optional[str] = None
    img_url: Optional[str] = None
    values_active: Optional[str] = None

    class Config:
        anystr_strip_whitespace = True

    @validator('values_name', 'value_value', 'img_url', 'values_active', pre=True, always=True)
    def ensure_optional_fields_are_not_empty_strings(cls, v):
        if v == "":
            return None
        return v

class ReturnPolicyCsvModel(BaseModel):
    id: Optional[int] = None
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    grace_period_return: Optional[int] = None
    policy_name: Optional[str] = None
    return_policy_type: str
    time_period_return: Optional[int] = None
    business_details_id: Optional[int] = None

    class Config:
        anystr_strip_whitespace = True
        extra = "forbid"

    @root_validator(pre=False, skip_on_failure=True)
    def check_conditional_fields(cls, values):
        policy_type = values.get('return_policy_type')
        time_period = values.get('time_period_return')
        if policy_type == "SALES_RETURN_ALLOWED" and time_period is None:
            raise ValueError("'time_period_return' is required when 'return_policy_type' is 'SALES_RETURN_ALLOWED'.")
        return values

class ProductItemModel(BaseModel):
    product_name: str
    variant_sku: str
    attribute_combination: str
    status: str
    published: str
    default_sku: str
    quantity: int
    image_urls: Optional[str] = None

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
    offer_price: Optional[float] = None

    @validator('product_name')
    def price_product_name_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('product_name must not be empty')
        return value

    @validator('offer_price')
    def offer_price_must_be_positive(cls, v):
        if v is not None and v <= 0:
            raise ValueError('offer_price must be positive if provided')
        return v

class ProductCsvModel(BaseModel):
    # Add the necessary fields for ProductCsvModel
    product_name: str
    category_path: str
    price: float
    quantity: int

    class Config:
        anystr_strip_whitespace = True

class MetaTagModel(BaseModel):
    product_name: str
    meta_title: Optional[str] = None
    meta_keywords: Optional[str] = None
    meta_description: Optional[str] = None

    @validator('product_name')
    def meta_product_name_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('product_name must not be empty')
        return value

class ErrorType(str, Enum):
    VALIDATION = "VALIDATION"
    DATABASE = "DATABASE"
    LOOKUP = "LOOKUP"
    FILE_FORMAT = "FILE_FORMAT"
    UNEXPECTED_ROW_ERROR = "UNEXPECTED_ROW_ERROR"
    TASK_EXCEPTION = "TASK_EXCEPTION"
    CONFIGURATION = "CONFIGURATION"
    UNKNOWN = "UNKNOWN"

class ErrorDetailModel(BaseModel):
    row_number: Optional[int] = None
    field_name: Optional[str] = None
    error_message: str
    error_type: ErrorType = ErrorType.UNKNOWN
    offending_value: Optional[str] = None

    class Config:
        use_enum_values = True
        from_attributes = True # Used for ORM mode (SQLAlchemy to Pydantic)

# --- API Response Schemas ---

class UserResponseSchema(BaseModel):
    """Pydantic model for user information response."""
    user_id: Any # Matches 'userId' from token context, can be int or str
    username: str
    business_id: int # Parsed integer business_id
    roles: List[str]
    company_id_str: Optional[str] = None # The original companyId string from token

    class Config:
        from_attributes = True

class SessionResponseSchema(BaseModel):
    """Pydantic model for individual upload session response."""
    session_id: str # UUID as string
    business_details_id: int # Renamed from business_id for clarity if it maps to ORM's business_details_id
    load_type: str
    original_filename: Optional[str] = None
    wasabi_path: Optional[str] = None
    status: str
    details: Optional[str] = None
    record_count: Optional[int] = None
    error_count: Optional[int] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class SessionListResponseSchema(BaseModel):
    """Pydantic model for a list of upload sessions with total count for pagination."""
    items: List[SessionResponseSchema]
    total: int

    class Config:
        from_attributes = True
