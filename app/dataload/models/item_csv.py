from typing import Optional
from pydantic import BaseModel, Field

class ItemCsvModel(BaseModel):
    product_name: str = Field(..., min_length=1)
    attributes: str = Field(..., min_length=1) # e.g., color|main_attribute:true|size|main_attribute:false
    attribute_combination: str = Field(..., min_length=1) # e.g., {Black|main_sku:true...}|{S:M...}
    
    price: str # e.g., 19.99:19.99...|... (Can be complex)
    quantity: str # e.g., 15:18...|... (Can be complex)
    status: str # e.g., ACTIVE|ACTIVE... (Can be complex)
    
    order_limit: Optional[str] = None # e.g., 10|10... or empty if not provided for any variant
    package_size_length: Optional[str] = None
    package_size_width: Optional[str] = None
    package_size_height: Optional[str] = None
    package_weight: Optional[str] = None
    
    images: Optional[str] = None # Single string with all images for the product row

    class Config:
        str_strip_whitespace = True
        extra = "forbid"
