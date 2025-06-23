
from pydantic import BaseModel, Field, validator, constr, root_validator # Added constr and root_validator
from typing import Optional, List # Added List
from datetime import datetime # Added datetime

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

class AttributeCsvModel(BaseModel):
    """
    Pydantic model for validating a row from an Attributes CSV file.
    A single row defines an attribute and all its associated values.
    """
    attribute_name: constr(strip_whitespace=True, min_length=1) # Name of the parent attribute, e.g., "Color", "Size"
    is_color: bool = False # Mandatory, True if this attribute represents color swatches
    attribute_active: Optional[str] = None # Active status of the attribute itself (e.g., "ACTIVE", "INACTIVE")

    # Pipe-separated strings for attribute values
    values_name: Optional[str] = None       # e.g., "Red|Blue|Green" or "Small|Medium|Large"
    value_value: Optional[str] = None       # e.g., "FF0000|0000FF|00FF00" or "S|M|L" (optional for non-colors if same as name)
    img_url: Optional[str] = None           # e.g., "url1|url2|url3"
    values_active: Optional[str] = None     # e.g., "ACTIVE|INACTIVE|ACTIVE" (defaults to INACTIVE if not specified for a value part)

    # Optional audit fields for the parent attribute, if sourced from CSV
    # created_by: Optional[int] = None
    # created_date: Optional[int] = None # Epoch timestamp
    # updated_by: Optional[int] = None
    # updated_date: Optional[int] = None # Epoch timestamp

    class Config:
        anystr_strip_whitespace = True
        # extra = "forbid"

    @validator('values_name', 'value_value', 'img_url', 'values_active', pre=True, always=True)
    def ensure_optional_fields_are_not_empty_strings(cls, v):
        # If an optional field is provided as an empty string in CSV, convert to None
        # so that Pydantic's Optional typing works as expected (None vs. actual value)
        if v == "":
            return None
        return v

    @validator('values_name', always=True)
    def check_values_name_provided_if_any_value_list_is_provided(cls, v, values):
        # If any of value_value, img_url, or values_active are provided, values_name must also be provided.
        # This ensures we have display names for the values.
        # 'values' here is a dict of already validated/processed fields by Pydantic up to this point for this model instance.
        if (values.get('value_value') is not None or \
            values.get('img_url') is not None or \
            values.get('values_active') is not None) and not v: # v is values_name
            raise ValueError("'values_name' must be provided if 'value_value', 'img_url', or 'values_active' are specified.")
        return v

    @validator('values_active', always=True) # This validator should ideally run after the others
    def check_list_lengths_consistency(cls, v, values):
        # This validator checks if all provided pipe-separated value lists have the same number of elements.

        names_str = values.get('values_name')
        # If values_name is None (either not provided or converted from "" by previous validator),
        # then other lists should also be None for consistency.
        if names_str is None:
            if values.get('value_value') is not None or \
               values.get('img_url') is not None or \
               v is not None: # v is values_active
                # This situation might indicate an issue if, for example, values_name was truly missing
                # but other lists were provided. The check_values_name_provided... validator aims to catch this.
                # However, if values_name was an empty string and became None, and others were also empty strings
                # and became None, this is consistent.
                # If one of the others is not None here, it means values_name was None but others were not.
                # This should have been caught by check_values_name_provided_if_any_value_list_is_provided.
                # This check remains as a safeguard for direct instantiation or complex cases.
                pass # Let previous validator handle mandatory nature of names_str if others are present.
            return v # All value lists are None or consistently handled.

        num_names = len(names_str.split('|'))

        lists_to_check = {
            "value_value": values.get('value_value'),
            "img_url": values.get('img_url'),
            "values_active": v # 'v' is the current field being validated ('values_active')
        }

        for field_name, val_str in lists_to_check.items():
            if val_str is not None: # Only check if the string is provided (was not an empty string from CSV)
                num_parts = len(val_str.split('|'))
                if num_parts != num_names:
                    raise ValueError(
                        f"Mismatch in number of pipe-separated parts: "
                        f"'{field_name}' has {num_parts} parts, "
                        f"but 'values_name' ({names_str}) has {num_names} parts."
                    )
        return v # Return the original value for 'values_active'

class ReturnPolicyCsvModel(BaseModel):
    """Pydantic model for validating a row from a Return Policy CSV file,
    aligning with the new return_policy table DDL."""
    id: Optional[int] = None # For matching existing records if provided in CSV

    # Timestamps can be provided in CSV (e.g., ISO format) or system-generated
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None

    grace_period_return: Optional[int] = None # Corresponds to bigint in DB
    policy_name: Optional[str] = None # Corresponds to text in DB
    return_policy_type: str # E.g., "SALES_ARE_FINAL", "SALES_RETURN_ALLOWED"
    time_period_return: Optional[int] = None # Corresponds to bigint in DB

    business_details_id: Optional[int] = None # Integer ID for business

    class Config:
        anystr_strip_whitespace = True
        extra = "forbid"

    @root_validator(pre=False, skip_on_failure=True) # Run after individual field validation
    def check_conditional_fields(cls, values):
        policy_type = values.get('return_policy_type')
        time_period = values.get('time_period_return')
        # policy_name = values.get('policy_name') # policy_name can be null in DDL

        if policy_type == "SALES_RETURN_ALLOWED":
            if time_period is None:
                raise ValueError("'time_period_return' is required when 'return_policy_type' is 'SALES_RETURN_ALLOWED'.")
            # policy_name is optional as per DDL and CSV examples.

        elif policy_type == "SALES_ARE_FINAL":
            # For "SALES_ARE_FINAL", other fields are typically null or ignored.
            # Pydantic Optional already handles this.
            pass

        # Further validation for specific enum values of return_policy_type could be added here
        # if policy_type not in ["SALES_ARE_FINAL", "SALES_RETURN_ALLOWED", "OTHER_POLICY_TYPE"]:
        #     raise ValueError(f"Invalid 'return_policy_type': {policy_type}. Expected known types.")

        return values

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

# from datetime import datetime # Moved to top
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
    meta_title: Optional[str] = None
    meta_keywords: Optional[str] = None
    meta_description: Optional[str] = None

    @validator('product_name')
    def meta_product_name_must_not_be_empty(cls, value):
        if not value.strip():
            raise ValueError('product_name must not be empty')
        return value
