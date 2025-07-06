from pydantic import BaseModel, Field, validator, constr, root_validator
from typing import Optional, List, Any
from datetime import datetime
from enum import Enum
import re
from app.utils.slug import generate_slug
def generate_slug(input_string: str) -> str:
    slug = input_string.lower().strip()
    slug = slug.replace(' ', '-')
    slug = re.sub(r'[^a-z0-9\-]', '', slug)
    return slug.strip('-')

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
    enabled: bool = Field(..., description="Must be true or false")
    image_name: Optional[str] = None
    long_description: Optional[str] = None
    order_type: Optional[str] = None
    shipping_type: Optional[str] = None
    active: Optional[str] = Field(
        None,
        description="Either 'ACTIVE' or 'INACTIVE'; any other/missing ⇒ ACTIVE"
    )
    seo_description: Optional[str] = None
    seo_keywords: Optional[str] = None
    seo_title: Optional[str] = None
    url: Optional[str] = None
    position_on_site: Optional[int] = None

    class Config:
        str_strip_whitespace = True
        extra = "forbid"

    @validator("category_path", pre=True)
    def strip_path_whitespace(cls, v):
        # collapse and strip each segment
        return "/".join(seg.strip() for seg in v.split("/") if seg.strip())

    @validator("name", pre=True)
    def strip_name_whitespace(cls, v):
        return v.strip()

    @validator("name")
    def name_matches_last_path_segment(cls, v, values):
        path = values.get("category_path", "")
        last = path.rsplit("/", 1)[-1]
        if v.strip().lower() != last.strip().lower():
            raise ValueError(
                f"name '{v}' must equal last segment of category_path '{last}'"
            )
        return v

    @root_validator(pre=True)
    def fill_and_normalize_defaults(cls, values):
        # enabled must be present
        if "enabled" not in values or values["enabled"] is None:
            values["enabled"] = False

        # normalize active
        raw = values.get("active")
        if isinstance(raw, str) and raw.strip().lower() == "inactive":
            values["active"] = "INACTIVE"
        else:
            # everything else ⇒ ACTIVE
            values["active"] = "ACTIVE"

        # auto‐slug url if missing
        if not values.get("url") and values.get("name"):
            values["url"] = generate_slug(values["name"])

        return values

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

    @validator('supplier_id', 'created_by', 'created_date', 'updated_by', 'updated_date', pre=True)
    def empty_str_to_none(cls, v):
        # convert blanks or pure whitespace to None so Pydantic won’t try int("") 
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        return v

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
    return_policy_type: constr(strip_whitespace=True, min_length=1)
    time_period_return: Optional[int] = None

    # we don't expect business_details_id in the CSV
    business_details_id: Optional[int] = None

    class Config:
        anystr_strip_whitespace = True
        extra = "forbid"

    @validator("grace_period_return", "time_period_return", pre=True)
    def parse_ints_or_null(cls, v):
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        try:
            return int(v)
        except ValueError:
            raise ValueError("must be a valid integer")

    @root_validator(skip_on_failure=True)
    def enforce_final_policy_blankness(cls, values):
        typ = values.get("return_policy_type", "").strip().upper()
        if typ == "SALES_ARE_FINAL":
            if values.get("policy_name"):
                raise ValueError(
                    "policy_name must be empty when return_policy_type is SALES_ARE_FINAL"
                )
            if values.get("time_period_return") is not None:
                raise ValueError(
                    "time_period_return must be empty when return_policy_type is SALES_ARE_FINAL"
                )
        return values

    @root_validator(pre=False, skip_on_failure=True)
    def check_conditional_fields(cls, values):
        policy_type = values.get('return_policy_type', '').strip().upper()
        time_period = values.get('time_period_return')
        if policy_type == "SALES_RETURN_ALLOWED" and time_period is None:
            raise ValueError(
                "'time_period_return' is required when 'return_policy_type' is 'SALES_RETURN_ALLOWED'."
            )
        return values
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
