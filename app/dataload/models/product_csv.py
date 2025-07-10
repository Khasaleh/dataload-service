from typing import Optional, Any
from pydantic import BaseModel, Field, field_validator, model_validator, validator
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
    # self_gen_product_id: str = Field(..., min_length=1) # Removed as per new requirements
    # product_lookup_key: str = Field(..., min_length=1) # Removed: lookup by product_name
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

    size_unit: str # Will be validated and transformed
    weight_unit: str # Will be validated and transformed

    active: Optional[str] = Field(default="ACTIVE") # Default to ACTIVE

    return_type: str
    return_fee_type: Optional[str] = None
    return_fee: Optional[float] = None
    warehouse_location: Optional[str] = None
    store_location: Optional[str] = None
    return_policy: Optional[str] = None
    size_chart_img: Optional[str] = None # Handled by Optional[str] and strip_whitespace

    url: Optional[str] = None
    video_url: Optional[str] = None # Handled by Optional[str] and strip_whitespace
    video_thumbnail_url: Optional[str] = None # Handled by Optional[str] and strip_whitespace

    images: Optional[str] = None # Handled by Optional[str] and strip_whitespace
    specifications: Optional[str] = None

    is_child_item: Optional[int] = None # Changed to Optional[int], default None

    order_limit: Optional[int] = None # New field

    ean: Optional[str] = None
    isbn: Optional[str] = None
    keywords: Optional[str] = None
    mpn: Optional[str] = None
    seo_description: Optional[str] = None
    seo_title: Optional[str] = None
    upc: Optional[str] = None

    # ----- FIELD-LEVEL CLEANUPS -----
    @field_validator('active', mode='before')
    @classmethod
    def validate_and_normalize_active_status(cls, value: Any) -> Optional[str]:
        if isinstance(value, str):
            stripped_value = value.strip()
            if stripped_value == "":
                return None # Will allow Pydantic to use the default="ACTIVE"
            
            upper_value = stripped_value.upper()
            if upper_value not in ["ACTIVE", "INACTIVE"]:
                raise ValueError("Status, if provided and not empty, must be 'ACTIVE' or 'INACTIVE'")
            return upper_value # Return 'ACTIVE' or 'INACTIVE'
        
        if value is None: # If input is None (e.g. field missing in CSV row parsed as None)
            return None # Will allow Pydantic to use the default="ACTIVE"
            
        # If it's not a string or None, it's an invalid type for this logic.
        # Pydantic will likely catch type errors for Optional[str] earlier if not a str or None.
        # This validator primarily focuses on string processing for "ACTIVE"/"INACTIVE" and empty string.
        raise ValueError("Invalid type for active status. Must be a string or None/empty.")


    @field_validator('return_type')
    @classmethod
    def validate_return_type(cls, value: str) -> str:
        if value not in ["SALES_RETURN_ALLOWED", "SALES_ARE_FINAL"]:
            raise ValueError("return_type must be 'SALES_RETURN_ALLOWED' or 'SALES_ARE_FINAL'")
        return value

    @field_validator('return_fee_type', mode='before')
    @classmethod
    def clean_return_fee_type(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v.strip()

    @field_validator('return_fee', mode='before')
    @classmethod
    def clean_return_fee(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        try:
            return float(v)
        except Exception:
            raise ValueError("Input should be a valid number")

    @field_validator('return_fee_type')
    @classmethod
    def validate_return_fee_type_format(cls, value: Optional[str], info: Any) -> Optional[str]:
        if value is not None and value not in ["FIXED", "PERCENTAGE", "FREE"]:
            raise ValueError("return_fee_type, if provided, must be 'FIXED', 'PERCENTAGE', or 'FREE'")
        return value

    @field_validator('url', mode='before')
    @classmethod
    def generate_or_validate_url(cls, value: Optional[str], info: Any) -> Optional[str]:
        product_name = info.data.get('product_name')
        if value:
            if not re.match(r"^[a-z0-9]+(?:-[a-z0-9]+)*$", value):
                raise ValueError("Provided URL is not a valid slug (lowercase, alphanumeric, hyphens only)")
            return value
        return generate_url_slug(product_name)

    @field_validator('is_child_item')
    @classmethod
    def validate_is_child_item(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value not in [0, 1]:
            raise ValueError("is_child_item must be 0 or 1 if provided")
        return value

    @field_validator('order_limit', mode='before')
    @classmethod
    def empty_str_as_none_for_order_limit(cls, v: Any) -> Optional[Any]:
        if isinstance(v, str) and v.strip() == "":
            return None
        # Pydantic will then attempt to validate the (potentially non-None) value as int
        # If v is already None, or a valid int, or a string like "123", it passes through.
        # If v is a non-empty string that's not a valid int, Pydantic's default int parsing will raise error.
        return v

    @field_validator('size_unit', mode='before')
    @classmethod
    def normalize_and_validate_size_unit(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("Size unit must be a string.")
        
        v_lower = value.strip().lower()
        mapping = {
            "m": "METERS", "meters": "METERS",
            "cm": "CENTIMETERS", "centimeters": "CENTIMETERS",
            "ft": "FEET", "foot": "FEET", "foots": "FEET", # foots from original DDL, foot for common use
            "in": "INCHES", "inches": "INCHES",
            "mm": "MILLIMETERS", "millimeters": "MILLIMETERS",
        }
        # Target enums from problem: {METERS, CENTIMETERS,FOOTS,INCHES,MILLIMETERS}
        # Adjusted mapping to use FOOTS as per problem description, assuming FEET was my interpretation.
        # Re-adjusting to use FOOTS as specified in the original prompt, even if FEET is more standard.
        final_mapping = {
            "m": "METERS", "meters": "METERS",
            "cm": "CENTIMETERS", "centimeters": "CENTIMETERS",
            "ft": "FOOTS", "foot": "FOOTS", "foots": "FOOTS", # Using FOOTS
            "in": "INCHES", "inches": "INCHES",
            "mm": "MILLIMETERS", "millimeters": "MILLIMETERS",
        }
        # Validating against the specific enum set provided: {METERS, CENTIMETERS,FOOTS,INCHES,MILLIMETERS}
        valid_enums = {"METERS", "CENTIMETERS", "FOOTS", "INCHES", "MILLIMETERS"}

        if v_lower in final_mapping:
            result = final_mapping[v_lower]
            if result in valid_enums:
                return result
            else: # Should not happen if final_mapping is correct
                raise ValueError(f"Internal mapping error for size unit '{value}'. Mapped to '{result}' which is not in {valid_enums}")

        # If it's already one of the target enum values (e.g., "CENTIMETERS")
        if v_lower.upper() in valid_enums:
            return v_lower.upper()
            
        raise ValueError(f"Invalid size_unit: '{value}'. Must be one of {list(final_mapping.keys())} or {list(valid_enums)}.")

    @field_validator('weight_unit', mode='before')
    @classmethod
    def normalize_and_validate_weight_unit(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("Weight unit must be a string.")

        v_lower = value.strip().lower()
        # CSV enum: {KILOGRAMS,GRAMS,POUNDS,OUNCES,MILLIGRAM,TON,METRIC_TON}
        # My interpretation for 't': METRIC_TON.
        # For 'tonne': METRIC_TON
        mapping = {
            "kg": "KILOGRAMS", "kilograms": "KILOGRAMS",
            "g": "GRAMS", "grams": "GRAMS",
            "lb": "POUNDS", "pounds": "POUNDS",
            "oz": "OUNCES", "ounces": "OUNCES",
            "mg": "MILLIGRAMS", "milligrams": "MILLIGRAMS", # Original prompt uses MILLIGRAM
            "t": "METRIC_TON", "ton": "METRIC_TON", # 'ton' could also map to TON if distinct. Assuming 't' and 'ton' map to METRIC_TON as per common interpretation for data loading.
            "tonne": "METRIC_TON", "metric_ton": "METRIC_TON"
        }
        # Target enums from problem: {KILOGRAMS,GRAMS,POUNDS,OUNCES,MILLIGRAM,TON,METRIC_TON}
        # Correcting MILLIGRAMS to MILLIGRAM
        final_mapping = {
            "kg": "KILOGRAMS", "kilograms": "KILOGRAMS",
            "g": "GRAMS", "grams": "GRAMS",
            "lb": "POUNDS", "pounds": "POUNDS",
            "oz": "OUNCES", "ounces": "OUNCES",
            "mg": "MILLIGRAM", "milligram": "MILLIGRAM", # Corrected to MILLIGRAM
            "t": "METRIC_TON", "ton": "METRIC_TON", # Assuming 'ton' from CSV also means METRIC_TON here. If 'TON' is a distinct short ton, this needs adjustment.
            "tonne": "METRIC_TON", "metric_ton": "METRIC_TON"
        }
        valid_enums = {"KILOGRAMS", "GRAMS", "POUNDS", "OUNCES", "MILLIGRAM", "TON", "METRIC_TON"}

        if v_lower in final_mapping:
            result = final_mapping[v_lower]
            # Special case: if input was 'ton' and we need to distinguish between 'TON' and 'METRIC_TON'
            # For now, this logic maps 'ton' to 'METRIC_TON'. If 'ton' should map to 'TON' enum, it needs explicit handling.
            # The problem states "t (for metric ton/tonne) -> METRIC_TON". It doesn't explicitly state what "TON" (the enum) maps from.
            # Given "TON" is in the enum list, if "ton" (lowercase) from CSV is meant to be "TON" (uppercase enum), the mapping should be:
            # "ton": "TON"
            # Let's adjust: if 't' or 'tonne' means METRIC_TON, then 'ton' (if it appears) should map to 'TON'.
            # This makes the mapping more specific.
            
            # Re-evaluating based on "weight_unit in the csv enum is {KILOGRAMS,GRAMS,POUNDS,OUNCES,MILLIGRAM,TON,METRIC_TON}
            # so if any abbriviation provided here, one of the enums will be added ( KG = KILOGRAMS )"
            # This implies the keys in `final_mapping` are abbreviations, and values are the target enums.
            # 't' -> 'METRIC_TON' is clear.
            # What about 'TON' enum? If CSV provides 'ton', does it mean 'TON' or 'METRIC_TON'?
            # The user clarified "t (for metric ton/tonne) -> METRIC_TON". And "Ton: t (for metric ton, commonly referred to as "tonne")"
            # "Metric Ton: t or tonne"
            # This is slightly circular. I will assume 't' and 'tonne' map to 'METRIC_TON'.
            # If the CSV contains literally "TON" (uppercase) or "ton" (lowercase), it should map to the "TON" enum value.

            if v_lower == "ton": # Explicitly map "ton" (lowercase) to "TON" (enum)
                result = "TON"

            if result in valid_enums:
                return result
            else: # Should not happen
                 raise ValueError(f"Internal mapping error for weight unit '{value}'. Mapped to '{result}' which is not in {valid_enums}")

        if v_lower.upper() in valid_enums: # If input is already "KILOGRAMS", "TON", etc.
            return v_lower.upper()

        raise ValueError(f"Invalid weight_unit: '{value}'. Must be one of {list(final_mapping.keys())} or {list(valid_enums)}.")


    @field_validator('sale_price', 'cost_price', 'return_fee')
    @classmethod
    def validate_optional_positive_floats(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and value < 0:
            raise ValueError("Price/fee fields, if provided, must be non-negative.")
        return value

    # ----- CROSS-FIELD LOGIC -----
    @model_validator(mode='after')
    def check_model_logic(self) -> 'ProductCsvModel':
        if self.return_type == "SALES_ARE_FINAL":
            if self.return_fee_type or self.return_fee:
                raise ValueError("return_fee_type and return_fee must be null or empty when return_type is 'SALES_ARE_FINAL'")

        elif self.return_type == "SALES_RETURN_ALLOWED":
            if not self.return_fee_type:
                raise ValueError("return_fee_type is required when return_type is 'SALES_RETURN_ALLOWED'")
            if self.return_fee_type not in ["FIXED", "PERCENTAGE", "FREE"]:
                raise ValueError("return_fee_type must be 'FIXED', 'PERCENTAGE', or 'FREE'")
            if self.return_fee_type == "FREE":
                if self.return_fee not in (None, 0, 0.0):
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

    @validator('category_path')
    def clean_category_path(cls, v):
        return '/'.join(part.strip() for part in v.strip().split('/') if part.strip())

    class Config:
        str_strip_whitespace = True  # Changed from anystr_strip_whitespace
        validate_assignment = True
        extra = "forbid"
