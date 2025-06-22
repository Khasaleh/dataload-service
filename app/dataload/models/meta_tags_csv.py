from enum import Enum
from typing import Optional
from pydantic import BaseModel, root_validator, constr, Field

class MetaTypeEnum(str, Enum):
    PRODUCT = "PRODUCT"
    CATEGORY = "CATEGORY"

class MetaTagCsvRow(BaseModel):
    meta_type: MetaTypeEnum
    target_identifier: str = Field(..., min_length=1)
    business_details_id: Optional[int] = None
    meta_title: Optional[constr(max_length=256)] = None # Max 255/256
    meta_description: Optional[str] = None
    meta_keywords: Optional[str] = None

    @root_validator(pre=True)
    def preprocess_data(cls, values):
        """
        Convert empty strings to None for optional fields.
        Attempt to convert business_details_id to int if present and string.
        Strip whitespace from all string fields.
        """
        processed_values = {}
        for field_name, value in values.items():
            # Strip whitespace from string values
            if isinstance(value, str):
                value = value.strip()

            # Convert empty strings to None for defined optional fields
            if field_name in ['business_details_id', 'meta_title', 'meta_description', 'meta_keywords'] and value == "":
                processed_values[field_name] = None
                continue

            processed_values[field_name] = value

        # Handle business_details_id conversion to int
        bdi = processed_values.get('business_details_id')
        if bdi is not None and isinstance(bdi, str): # if it's a non-empty string
            try:
                processed_values['business_details_id'] = int(bdi)
            except ValueError:
                # Pydantic will raise a validation error later if it's not a valid int string
                # Or we can raise it here: raise ValueError(f"business_details_id ('{bdi}') must be a valid integer or empty.")
                pass

        return processed_values

    @root_validator(skip_on_failure=True)
    def validate_product_and_category_rules(cls, values):
        meta_type = values.get('meta_type')
        target_identifier = values.get('target_identifier')
        business_details_id = values.get('business_details_id')
        meta_description = values.get('meta_description')
        meta_keywords = values.get('meta_keywords')

        # target_identifier is already required by Field(..., min_length=1)

        if meta_type == MetaTypeEnum.PRODUCT:
            if business_details_id is None:
                raise ValueError(
                    f"business_details_id is required for PRODUCT meta_type (target_identifier: '{target_identifier}')."
                )

            if meta_description and len(meta_description) > 512:
                raise ValueError(
                    f"meta_description for PRODUCT (target_identifier: '{target_identifier}') cannot exceed 512 characters. "
                    f"Current length: {len(meta_description)}."
                )
            # products.keywords is VARCHAR(512)
            if meta_keywords and len(meta_keywords) > 512:
                raise ValueError(
                    f"meta_keywords for PRODUCT (target_identifier: '{target_identifier}') cannot exceed 512 characters. "
                    f"Current length: {len(meta_keywords)}."
                )

        elif meta_type == MetaTypeEnum.CATEGORY:
            # categories.seo_description is VARCHAR(255)
            if meta_description and len(meta_description) > 255:
                raise ValueError(
                    f"meta_description for CATEGORY (target_identifier: '{target_identifier}') cannot exceed 255 characters. "
                    f"Current length: {len(meta_description)}."
                )
            # categories.seo_keywords is VARCHAR(255)
            if meta_keywords and len(meta_keywords) > 255:
                raise ValueError(
                    f"meta_keywords for CATEGORY (target_identifier: '{target_identifier}') cannot exceed 255 characters. "
                    f"Current length: {len(meta_keywords)}."
                )

        return values

    class Config:
        use_enum_values = True # Ensure string values from CSV are correctly mapped to Enum members
        validate_assignment = True # Re-validate on field assignment
        # anystr_strip_whitespace = True # Handled in preprocess_data for more control
        extra = 'forbid' # Forbid any extra fields in the CSV row not defined in the model
        # Pydantic v2: from pydantic import ConfigDict; model_config = ConfigDict(extra='forbid', ...)
