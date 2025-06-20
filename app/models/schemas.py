
from pydantic import BaseModel, Field, validator
from typing import Optional

class BrandModel(BaseModel):
    brand_name: str

class AttributeModel(BaseModel):
    attribute_name: str
    allowed_values: str

class ReturnPolicyModel(BaseModel):
    return_policy_code: str
    name: str
    return_window_days: int
    grace_period_days: int
    description: str

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

class ProductItemModel(BaseModel):
    product_name: str
    variant_sku: str
    attribute_combination: str
    status: str
    published: str
    default_sku: str
    quantity: int
    image_urls: Optional[str]

class ProductPriceModel(BaseModel):
    product_name: str
    price: float
    cost_per_item: float
    offer_price: Optional[float]

class MetaTagModel(BaseModel):
    product_name: str
    meta_title: Optional[str]
    meta_keywords: Optional[str]
    meta_description: Optional[str]
