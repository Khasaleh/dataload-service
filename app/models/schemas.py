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
    
from pydantic import BaseModel, Field, validator, root_validator, constr
from typing import Optional

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
python
Copy
Edit
# app/services/db_loaders.py

import logging
from typing import Any, Dict, Optional
from sqlalchemy.exc import IntegrityError, DataError
from app.utils.redis_utils import add_to_id_map, DB_PK_MAP_SUFFIX
from app.utils.date_utils import ServerDateTime
from app.exceptions import DataLoaderError
from app.models import ErrorType
from app.db.models import CategoryOrm

logger = logging.getLogger(__name__)

def load_category_to_db(
    db_session,
    business_details_id: int,
    record_data: Dict[str, Any],
    session_id: str,
    db_pk_redis_pipeline: Any = None,
    user_id: int = None
) -> int:
    """
    Upsert a hierarchical category path (e.g. "Electronics/Computers/Laptops").
    - New rows: set created_by/created_date, updated_by/updated_date, business_details_id, enabled, active, url slug.
    - If CSV explicitly provides order_type or shipping_type (even blank), convert blank→NULL; if omitted, leave None.
    - Existing leaf: update only mutable fields + updated_by/updated_date.
    Returns the final category ID.
    """
    path = record_data.get("category_path", "").strip()
    if not path:
        raise DataLoaderError(
            message="Missing or empty 'category_path'",
            error_type=ErrorType.VALIDATION,
            field_name="category_path"
        )

    # helpers
    def _bool(val: Any) -> bool:
        return str(val or "").strip().lower() in ("true", "1", "yes")
    def _active(val: Any) -> str:
        return "INACTIVE" if isinstance(val, str) and val.strip().lower()=="inactive" else "ACTIVE"

    # extract & normalize CSV fields
    name             = record_data["name"].strip()
    description      = record_data.get("description")
    enabled          = _bool(record_data.get("enabled", True))
    image_name       = record_data.get("image_name")
    long_description = record_data.get("long_description")
    order_type_raw   = record_data.get("order_type")   # may be absent
    order_type       = order_type_raw.strip() if order_type_raw is not None and order_type_raw.strip() else None
    shipping_raw     = record_data.get("shipping_type")
    shipping_type    = shipping_raw.strip() if shipping_raw is not None and shipping_raw.strip() else None
    active_flag      = _active(record_data.get("active"))
    seo_description  = record_data.get("seo_description")
    seo_keywords     = record_data.get("seo_keywords")
    seo_title        = record_data.get("seo_title")
    position         = record_data.get("position_on_site")
    url              = record_data.get("url") or f"/{ServerDateTime.now_epoch_ms()}"  # fallback, but CSV-model should fill

    segments = [seg.strip() for seg in path.split("/") if seg.strip()]
    parent_id: Optional[int] = None
    full_path = ""
    final_id: Optional[int] = None

    try:
        for idx, seg in enumerate(segments):
            full_path = f"{full_path}/{seg}" if full_path else seg
            is_leaf = (idx == len(segments)-1)

            # lookup existing at this level
            orm_name = name if is_leaf else seg
            cat = (
                db_session.query(CategoryOrm)
                          .filter_by(
                              business_details_id=business_details_id,
                              parent_id=parent_id,
                              name=orm_name
                          )
                          .first()
            )

            if cat:
                # update only leaf‐level metadata
                if is_leaf:
                    logger.info(f"Updating category '{full_path}' (ID={cat.id})")
                    cat.description      = description      or cat.description
                    cat.enabled          = enabled
                    cat.image_name       = image_name       or cat.image_name
                    cat.long_description = long_description or cat.long_description
                    if "order_type" in record_data:
                        cat.order_type    = order_type
                    if "shipping_type" in record_data:
                        cat.shipping_type = shipping_type
                    cat.active           = active_flag
                    cat.seo_description  = seo_description  or cat.seo_description
                    cat.seo_keywords     = seo_keywords     or cat.seo_keywords
                    cat.seo_title        = seo_title        or cat.seo_title
                    cat.url              = url              or cat.url
                    cat.position_on_site = position or cat.position_on_site
                    cat.updated_by       = user_id
                    cat.updated_date     = ServerDateTime.now_epoch_ms()
                final_id = cat.id

            else:
                # create new
                now = ServerDateTime.now_epoch_ms()
                payload: Dict[str, Any] = {
                    "business_details_id": business_details_id,
                    "parent_id": parent_id,
                    "name": orm_name,
                    "created_by": user_id,
                    "created_date": now,
                    "updated_by": user_id,
                    "updated_date": now,
                    "enabled": enabled if is_leaf else True,
                    "active": active_flag,
                    "description": description if is_leaf else seg,
                    "url": url,
                }
                if is_leaf:
                    payload.update({
                        "image_name":       image_name,
                        "long_description": long_description,
                        "order_type":       order_type,
                        "shipping_type":    shipping_type,
                        "seo_description":  seo_description,
                        "seo_keywords":     seo_keywords,
                        "seo_title":        seo_title,
                        "position_on_site": position,
                    })
                new_cat = CategoryOrm(**payload)
                db_session.add(new_cat)
                db_session.flush()
                final_id = new_cat.id
                logger.info(f"Created category '{full_path}' (ID={final_id})")

            # cache in Redis
            add_to_id_map(
                session_id,
                f"categories{DB_PK_MAP_SUFFIX}",
                full_path,
                final_id,
                pipeline=db_pk_redis_pipeline
            )

            parent_id = final_id  # type: ignore

        return final_id  # type: ignore

    except IntegrityError as e:
        logger.error(f"DB integrity error for '{path}': {e.orig}")
        raise DataLoaderError(
            message=f"Integrity error for '{path}': {e.orig}",
            error_type=ErrorType.DATABASE,
            field_name="category_path",
            offending_value=path,
            original_exception=e
        )
    except DataError as e:
        logger.error(f"DB data error for '{path}': {e.orig}")
        raise DataLoaderError(
            message=f"Data error for '{path}': {e.orig}",
            error_type=ErrorType.DATABASE,
            field_name="category_path",
            offending_value=path,
            original_exception=e
        )
    except Exception as e:
        logger.exception(f"Unexpected error for '{path}'")
        raise DataLoaderError(
            message=f"Unexpected error for category path '{path}': {str(e)}",
            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
            field_name="category_path",
            offending_value=path,
            original_exception=e
        )
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
