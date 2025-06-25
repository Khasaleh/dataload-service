from pydantic import BaseModel, Field, validator, constr, root_validator  # Added constr and root_validator
from typing import Optional, List  # Added List
from datetime import datetime  # Added datetime
from enum import Enum  # Added this import

class BrandCsvModel(BaseModel):  # Renamed from BrandModel
    """Pydantic model for validating a row from a Brand CSV file."""
    name: constr(strip_whitespace=True, min_length=1)  # Was brand_name; this is the key identifier from CSV
    logo: constr(strip_whitespace=True, min_length=1)  # Path or URL to the brand logo; mandatory

    supplier_id: Optional[int] = None  # Pydantic int for DB BigInteger
    active: Optional[str] = None  # e.g., "TRUE", "FALSE", or other status strings, matches DB String(255)

    # Optional audit fields if provided in CSV (DB type is BigInteger)
    created_by: Optional[int] = None
    created_date: Optional[int] = None  # If CSV provides epoch timestamp
    updated_by: Optional[int] = None
    updated_date: Optional[int] = None  # If CSV provides epoch timestamp

    class Config:
        anystr_strip_whitespace = True
        # extra = "forbid" # If no other columns are allowed from CSV

class ProductItemModel(BaseModel):
    """Pydantic model for validating a row from a Product Item CSV file."""
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

class AttributeCsvModel(BaseModel):
    """Pydantic model for validating a row from an Attributes CSV file."""
    attribute_name: constr(strip_whitespace=True, min_length=1)  # Name of the parent attribute, e.g., "Color", "Size"
    is_color: bool = False  # Mandatory, True if this attribute represents color swatches
    attribute_active: Optional[str] = None  # Active status of the attribute itself (e.g., "ACTIVE", "INACTIVE")

    values_name: Optional[str] = None  # e.g., "Red|Blue|Green" or "Small|Medium|Large"
    value_value: Optional[str] = None  # e.g., "FF0000|0000FF|00FF00" or "S|M|L"
    img_url: Optional[str] = None  # e.g., "url1|url2|url3"
    values_active: Optional[str] = None  # e.g., "ACTIVE|INACTIVE|ACTIVE"

    class Config:
        anystr_strip_whitespace = True
        # extra = "forbid"

    @validator('values_name', 'value_value', 'img_url', 'values_active', pre=True, always=True)
    def ensure_optional_fields_are_not_empty_strings(cls, v):
        if v == "":
            return None
        return v

    @validator('values_name', always=True)
    def check_values_name_provided_if_any_value_list_is_provided(cls, v, values):
        if (values.get('value_value') is not None or \
            values.get('img_url') is not None or \
            values.get('values_active') is not None) and not v:
            raise ValueError("'values_name' must be provided if 'value_value', 'img_url', or 'values_active' are specified.")
        return v

    @validator('values_active', always=True)
    def check_list_lengths_consistency(cls, v, values):
        names_str = values.get('values_name')
        if names_str is None:
            if values.get('value_value') is not None or \
               values.get('img_url') is not None or \
               v is not None:
                pass
            return v

        num_names = len(names_str.split('|'))
        lists_to_check = {
            "value_value": values.get('value_value'),
            "img_url": values.get('img_url'),
            "values_active": v
        }

        for field_name, val_str in lists_to_check.items():
            if val_str is not None:
                num_parts = len(val_str.split('|'))
                if num_parts != num_names:
                    raise ValueError(
                        f"Mismatch in number of pipe-separated parts: "
                        f"'{field_name}' has {num_parts} parts, "
                        f"but 'values_name' ({names_str}) has {num_names} parts."
                    )
        return v

class ReturnPolicyCsvModel(BaseModel):
    """Pydantic model for validating a row from a Return Policy CSV file."""
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

        if policy_type == "SALES_RETURN_ALLOWED":
            if time_period is None:
                raise ValueError("'time_period_return' is required when 'return_policy_type' is 'SALES_RETURN_ALLOWED'.")
        elif policy_type == "SALES_ARE_FINAL":
            pass

        return values

class ProductCsvModel(BaseModel):
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

# Error Handling Models
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

