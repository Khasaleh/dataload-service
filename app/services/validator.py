
from app.models.schemas import (
    BrandModel, AttributeModel, ReturnPolicyModel, ProductModel,
    ProductItemModel, ProductPriceModel, MetaTagModel
)
from pydantic import ValidationError

MODEL_MAP = {
    "brands": BrandModel,
    "attributes": AttributeModel,
    "return_policies": ReturnPolicyModel,
    "products": ProductModel,
    "product_items": ProductItemModel,
    "product_prices": ProductPriceModel,
    "meta_tags": MetaTagModel
}

def validate_csv(load_type, records):
    errors = []
    valid_rows = []
    model = MODEL_MAP.get(load_type)
    if not model:
        return [{"error": f"Unsupported load type: {load_type}"}], []

    for i, row in enumerate(records):
        try:
            valid = model(**row)
            valid_rows.append(valid.dict())
        except ValidationError as e:
            for err in e.errors():
                errors.append({
                    "row": i + 1,
                    "field": ".".join(str(f) for f in err['loc']),
                    "error": err['msg']
                })
    return errors, valid_rows
