from typing import Optional, Any
from pydantic import BaseModel, Field, field_validator, model_validator, validator
import re

def generate_url_slug(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    slug = name.lower()
    slug = re.sub(r'\s+', '-', slug)
    slug = re.sub(r'[^\w\-]', '', slug)
    slug = re.sub(r'--+', '-', slug)
    slug = slug.strip('-')
    return slug if slug else None

class ProductCsvModel(BaseModel):
    product_name: str = Field(..., min_length=1)
    self_gen_product_id: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    brand_name: str = Field(..., min_length=1)
    category_path: str = Field(..., min_length=1)

    shopping_category_name: Optional[str] = None

    price: float = Field(..., gt=0)
    sale_price: Optional[float] = None
    cost_price: Optional[float] = None

    quantity: int = Field(..., ge=0)

    package_size_length: float = Field(..., gt=0)
    package_size_width: float = Field(..., gt=0)
    package_size_height: float = Field(..., gt=0)
    product_weights: float = Field(..., gt=0)

    size_unit: str
    weight_unit: str

    active: str
    return_type: str
    return_fee_type: Optional[str] = None
    return_fee: Optional[float] = None
    warehouse_location: Optional[str] = None
    store_location: Optional[str] = None
    return_policy: Optional[str] = None
    size_chart_img: Optional[str] = None

    url: Optional[str] = None
    video_url: Optional[str] = None
    video_thumbnail_url: Optional[str] = None

    images: Optional[str] = None
    specifications: Optional[str] = None

    is_child_item: int

    ean: Optional[str] = None
    isbn: Optional[str] = None
    keywords: Optional[str] = None
    mpn: Optional[str] = None
    seo_description: Optional[str] = None
    seo_title: Optional[str] = None
    upc: Optional[str] = None

    #
    # Universal strip for key fields
    #
    @field_validator(
        'active', 'return_type', 'return_fee_type',
        'warehouse_location', 'store_location',
        'return_policy', 'size_chart_img',
        'size_unit', 'weight_unit',
        'shopping_category_name', 'ean', 'isbn',
        'keywords', 'mpn', 'seo_description',
        'seo_title', 'upc',
        mode='before'
    )
    @classmethod
    def strip_strings(cls, v):
        if v is None:
            return None
        return v.strip()

    #
    # Parse floats robustly from CSV, even with spaces
    #
    @field_validator(
        'price', 'sale_price', 'cost_price', 'return_fee',
        'package_size_length', 'package_size_width',
        'package_size_height', 'product_weights',
        mode='before'
    )
    @classmethod
    def parse_float(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            if v == '':
                return None
        try:
            return float(v)
        except Exception:
            raise ValueError("Input should be a valid number")

    #
    # Parse integers robustly from CSV
    #
    @field_validator('quantity', 'is_child_item', mode='before')
    @classmethod
    def parse_int(cls, v):
        if v is None:
            return 0
        if isinstance(v, str):
            v = v.strip()
            if v == '':
                return 0
        try:
            return int(v)
        except Exception:
            raise ValueError("Input should be a valid integer")

    #
    # Clean category_path
    #
    @validator('category_path')
    def clean_category_path(cls, v):
        return '/'.join(part.strip() for part in v.strip().split('/') if part.strip())

    #
    # Active field must be ACTIVE or INACTIVE
    #
    @field_validator('active')
    @classmethod
    def validate_active_status(cls, value: str) -> str:
        if value.upper() not in ["ACTIVE", "INACTIVE"]:
            raise ValueError("Status must be 'ACTIVE' or 'INACTIVE'")
        return value.upper()

    #
    # Return type check
    #
    @field_validator('return_type')
    @classmethod
    def validate_return_type(cls, value: str) -> str:
        value = value.strip()
        if value not in ["SALES_RETURN_ALLOWED", "SALES_ARE_FINAL"]:
            raise ValueError("return_type must be 'SALES_RETURN_ALLOWED' or 'SALES_ARE_FINAL'")
        return value

    #
    # Validate return_fee_type
    #
    @field_validator('return_fee_type')
    @classmethod
    def validate_return_fee_type_format(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            value = value.strip()
            if value not in ["FIXED", "PERCENTAGE", "FREE"]:
                raise ValueError("return_fee_type, if provided, must be 'FIXED', 'PERCENTAGE', or 'FREE'")
        return value

    #
    # Validate is_child_item
    #
    @field_validator('is_child_item')
    @classmethod
    def validate_is_child_item(cls, value: int) -> int:
        if value not in [0, 1]:
            raise ValueError("is_child_item must be 0 or 1")
        return value

    #
    # Validate URL or generate
    #
    @field_validator('url', mode='before')
    @classmethod
    def generate_or_validate_url(cls, value: Optional[str], info: Any) -> Optional[str]:
        product_name = info.data.get('product_name')
        if value:
            if not re.match(r"^[a-z0-9]+(?:-[a-z0-9]+)*$", value.strip()):
                raise ValueError("Provided URL is not a valid slug (lowercase, alphanumeric, hyphens only)")
            return value.strip()
        return generate_url_slug(product_name)

    #
    # Validate floats for sale_price, cost_price, return_fee
    #
    @field_validator('sale_price', 'cost_price', 'return_fee')
    @classmethod
    def validate_optional_positive_floats(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("Price/fee fields, if provided, must be non-negative.")
        return value

    #
    # Model-level validation logic
    #
    @model_validator(mode='after')
    def check_model_logic(self) -> 'ProductCsvModel':
        if self.return_type == "SALES_ARE_FINAL":
            if self.return_fee_type or self.return_fee:
                raise ValueError("return_fee_type and return_fee must be null or empty when return_type is 'SALES_ARE_FINAL'")
        elif self.return_type == "SALES_RETURN_ALLOWED":
            if not self.return_fee_type:
                raise ValueError("return_fee_type is required when return_type is 'SALES_RETURN_ALLOWED'")
            if self.return_fee_type == "FREE":
                if self.return_fee not in [None, 0, 0.0]:
                    raise ValueError("return_fee must be 0 or null/empty if return_fee_type is 'FREE'")
                object.__setattr__(self, 'return_fee', 0.0)
            elif self.return_fee_type in ["FIXED", "PERCENTAGE"]:
                if self.return_fee is None or self.return_fee < 0:
                    raise ValueError(f"return_fee must be provided and non-negative if return_fee_type is '{self.return_fee_type}'")

        if self.images:
            parts = self.images.split('|')
            if len(parts) % 2 != 0:
                raise ValueError("Images string must have pairs of url and main_image flag.")
            for i in range(0, len(parts), 2):
                if not parts[i].startswith(('http://', 'https://')):
                    raise ValueError(f"Image URL '{parts[i]}' must be a valid URL.")
                if parts[i+1] not in ["main_image:true", "main_image:false"]:
                    raise ValueError(f"Image flag '{parts[i+1]}' must be 'main_image:true' or 'main_image:false'.")

        if self.specifications:
            pairs = self.specifications.split('|')
            for pair in pairs:
                if ':' not in pair or len(pair.split(':', 1)) != 2 or not pair.split(':', 1)[0] or not pair.split(':', 1)[1]:
                    raise ValueError(f"Specification entry '{pair}' must be in 'Name:Value' format and both Name and Value must be non-empty.")

        if self.video_url and not self.video_thumbnail_url:
            raise ValueError("If 'video_url' is provided, 'video_thumbnail_url' must also be provided.")

        return self

    class Config:
        anystr_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"
