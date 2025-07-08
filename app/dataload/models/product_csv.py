from typing import Optional, List, Any
from pydantic import BaseModel, field_validator, model_validator, Field
import re

def generate_url_slug(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    slug = name.lower()
    slug = re.sub(r'\s+', '-', slug)  # Replace spaces with hyphens
    slug = re.sub(r'[^\w\-]', '', slug)  # Remove special characters except hyphen and word characters
    slug = re.sub(r'--+', '-', slug)  # Replace multiple hyphens with a single one
    slug = slug.strip('-')
    return slug if slug else None

class ProductCsvModel(BaseModel):
    product_name: str = Field(..., min_length=1)
    self_gen_product_id: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    brand_name: str = Field(..., min_length=1)
    category_path: str = Field(..., min_length=1)

    shopping_category_name: Optional[str] = None

    price: float = Field(..., gt=0) # Assuming price must be positive
    sale_price: Optional[float] = None
    cost_price: Optional[float] = None

    quantity: int = Field(..., ge=0) # Quantity can be 0

    package_size_length: float = Field(..., gt=0)
    package_size_width: float = Field(..., gt=0)
    package_size_height: float = Field(..., gt=0)
    product_weights: float = Field(..., gt=0)

    size_unit: str # e.g., CENTIMETERS, INCHES
    weight_unit: str # e.g., KILOGRAMS, POUNDS

    active: str # "ACTIVE" or "INACTIVE"

    return_type: str # "SALES_RETURN_ALLOWED", "SALES_ARE_FINAL"
    return_fee_type: Optional[str] = None # "FIXED", "PERCENTAGE", "FREE"
    return_fee: Optional[float] = None

    url: Optional[str] = None # Will be auto-generated if None, or validated
    video_url: Optional[str] = None
    video_thumbnail_url: Optional[str] = None # New field for video thumbnail

    images: Optional[str] = None # Pipe-separated: "url1|main_image:true", "url2|main_image:false"
    specifications: Optional[str] = None # Pipe-separated: "SpecName1:Value1|SpecName2:Value2"

    is_child_item: int # 0 or 1

    # Optional SEO and identifier fields
    ean: Optional[str] = None
    isbn: Optional[str] = None
    keywords: Optional[str] = None
    mpn: Optional[str] = None
    seo_description: Optional[str] = None
    seo_title: Optional[str] = None
    upc: Optional[str] = None

    @field_validator('active')
    @classmethod
    def validate_active_status(cls, value: str) -> str:
        if value.upper() not in ["ACTIVE", "INACTIVE"]:
            raise ValueError("Status must be 'ACTIVE' or 'INACTIVE'")
        return value.upper()

    @field_validator('return_type')
    @classmethod
    def validate_return_type(cls, value: str) -> str:
        if value not in ["SALES_RETURN_ALLOWED", "SALES_ARE_FINAL"]:
            raise ValueError("return_type must be 'SALES_RETURN_ALLOWED' or 'SALES_ARE_FINAL'")
        return value

    # For validators that depend on other fields, use model_validator or ensure order if using field_validator with always=True
    # Pydantic V2 handles inter-field validation mostly via model_validator.
    # We'll keep return_fee_type validation simple here and rely on the model_validator for complex cross-field logic.
    @field_validator('return_fee_type')
    @classmethod
    def validate_return_fee_type_format(cls, value: Optional[str], info: Any) -> Optional[str]:
        # This validator now primarily checks the format if a value is provided.
        # The dependency on 'return_type' is better handled in the model_validator.
        if value is not None and value not in ["FIXED", "PERCENTAGE", "FREE"]:
            raise ValueError("return_fee_type, if provided, must be 'FIXED', 'PERCENTAGE', or 'FREE'")
        return value

    @field_validator('url', mode='before') # mode='before' to intercept before standard validation
    @classmethod
    def generate_or_validate_url(cls, value: Optional[str], info: Any) -> Optional[str]:
        # info.data contains the raw input data to the model
        product_name = info.data.get('product_name')
        if value: # If URL is provided, validate it
            if not re.match(r"^[a-z0-9]+(?:-[a-z0-9]+)*$", value):
                 raise ValueError("Provided URL is not a valid slug (lowercase, alphanumeric, hyphens only)")
            return value
        return generate_url_slug(product_name)


    @field_validator('is_child_item')
    @classmethod
    def validate_is_child_item(cls, value: int) -> int:
        if value not in [0, 1]:
            raise ValueError("is_child_item must be 0 or 1")
        return value

    @field_validator('sale_price', 'cost_price', 'return_fee')
    @classmethod
    def validate_optional_positive_floats(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("Price/fee fields, if provided, must be non-negative.")
        return value

    @model_validator(mode='after')
    def check_model_logic(self) -> 'ProductCsvModel':
        # Access fields as self.field_name
        if self.return_type == "SALES_ARE_FINAL":
            if self.return_fee_type is not None or self.return_fee is not None:
                 if self.return_fee_type or self.return_fee: # Check for non-empty strings too if types were str
                    raise ValueError("return_fee_type and return_fee must be null or empty when return_type is 'SALES_ARE_FINAL'")

        elif self.return_type == "SALES_RETURN_ALLOWED":
            if not self.return_fee_type:
                raise ValueError("return_fee_type is required when return_type is 'SALES_RETURN_ALLOWED'")
            if self.return_fee_type not in ["FIXED", "PERCENTAGE", "FREE"]: # Redundant if individual validator exists, but safe
                raise ValueError("return_fee_type must be 'FIXED', 'PERCENTAGE', or 'FREE'")


            if self.return_fee_type == "FREE":
                if self.return_fee is not None and self.return_fee != 0:
                    raise ValueError("return_fee must be 0 or null/empty if return_fee_type is 'FREE'")
                object.__setattr__(self, 'return_fee', 0.0) # Normalize using object.__setattr__
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

        # New validation: If video_url is provided, video_thumbnail_url must also be provided.
        if self.video_url and not self.video_thumbnail_url:
            raise ValueError("If 'video_url' is provided, 'video_thumbnail_url' must also be provided.")

        return self

    @validator('category_path')
    def clean_category_path(cls, v):
        return '/'.join(
            part.strip() for part in v.strip().split('/') if part.strip()
        )
    class Config:
        anystr_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"
        # If business_details_id is always set programmatically and not from CSV:
        # exclude = {'business_details_id'} # Or handle it in the loader
        pass

# Example usage:
# data_row = {
#     "product_name": "Test Product", "self_gen_product_id": "SKU123", "business_details_id": 10,
#     "description": "A test product", "brand_name": "TestBrand", "category_id": 1,
#     "price": 100.0, "quantity": 10, "package_size_length": 10, "package_size_width": 10,
#     "package_size_height": 10, "product_weights": 1, "size_unit": "CM", "weight_unit": "KG",
#     "active": "ACTIVE", "return_type": "SALES_RETURN_ALLOWED", "return_fee_type": "FREE",
#     "is_child_item": 0, "images": "http://example.com/img1.png|main_image:true",
#     "specifications": "Color:Red|Size:Medium"
# }
# validated_product = ProductCsvModel(**data_row)
# print(validated_product.url)
# print(validated_product.return_fee)
