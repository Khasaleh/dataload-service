from typing import Optional, Union
from pydantic import BaseModel, validator, Field
from enum import Enum

class PriceTypeEnum(str, Enum):
    PRODUCT = "PRODUCT"
    SKU = "SKU"

class PriceCsv(BaseModel):
    price_type: PriceTypeEnum
    product_id: Optional[str] = Field(default=None)
    sku_id: Optional[str] = Field(default=None)
    price: float
    discount_price: Optional[float] = Field(default=None)
    cost_price: Optional[float] = Field(default=None)
    currency: Optional[str] = Field(default=None)

    @validator('product_id', always=True)
    def check_product_id(cls, v, values):
        if values.get('price_type') == PriceTypeEnum.PRODUCT and not v:
            raise ValueError('product_id is required when price_type is PRODUCT')
        if values.get('price_type') == PriceTypeEnum.SKU and v:
            raise ValueError('product_id must be empty when price_type is SKU')
        return v

    @validator('sku_id', always=True)
    def check_sku_id(cls, v, values):
        if values.get('price_type') == PriceTypeEnum.SKU and not v:
            raise ValueError('sku_id is required when price_type is SKU')
        if values.get('price_type') == PriceTypeEnum.PRODUCT and v:
            raise ValueError('sku_id must be empty when price_type is PRODUCT')
        return v

    @validator('price')
    def price_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError('price must be a positive number')
        return v

    @validator('discount_price', always=True)
    def discount_price_must_be_less_than_price(cls, v, values):
        if v is not None and values.get('price') is not None and v >= values['price']:
            raise ValueError('discount_price must be less than price')
        return v

    @validator('cost_price', always=True)
    def cost_price_must_be_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError('cost_price must be >= 0 if present')
        return v

    class Config:
        use_enum_values = True
