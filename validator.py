from pydantic import ValidationError
from app.utils.redis_utils import get_from_id_map
from collections import defaultdict
from app.models.schemas import (
    BrandCsvModel, AttributeCsvModel, ReturnPolicyCsvModel, ProductItemModel, 
    ProductPriceModel, MetaTagModel, ProductCsvModel
)
from app.models.schemas import ErrorDetailModel, ErrorType
from typing import List, Dict, Tuple

MODEL_MAP = {
    "brands": BrandCsvModel,
    "attributes": AttributeCsvModel,
    "return_policies": ReturnPolicyCsvModel,
    "product_items": ProductItemModel,
    "product_prices": ProductPriceModel,
    "meta_tags": MetaTagModel,
    "products": ProductCsvModel
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

def check_file_uniqueness(records: List[Dict], unique_key: str) -> List[Dict]:
    errors = []
    key_counts = defaultdict(list)
    
    for i, record in enumerate(records):
        key_value = record.get(unique_key)
        if key_value is not None:
            key_counts[key_value].append(i + 1)  # Store 1-based row index

    for key_value, rows in key_counts.items():
        if len(rows) > 1:
            errors.append({
                "error": "Duplicate key found in file",
                "key": key_value,
                "rows": rows,
                "field": unique_key
            })
    return errors

def check_referential_integrity(
    records: List[Dict],
    field_to_check: str,
    referenced_entity_type: str,
    session_id: str
) -> List[Dict]:
    errors = []
    
    for i, record in enumerate(records):
        key_value = record.get(field_to_check)
        if key_value:
            if not get_from_id_map(session_id, referenced_entity_type, key_value):
                errors.append({
                    "row": i + 1,
                    "field": field_to_check,
                    "error": f"Referenced {referenced_entity_type} not found",
                    "value": key_value
                })
    return errors
