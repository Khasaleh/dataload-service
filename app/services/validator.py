from pydantic import ValidationError
from app.utils.redis_utils import get_from_id_map
from collections import defaultdict
from app.models.schemas import (
    BrandCsvModel, AttributeCsvModel, ReturnPolicyCsvModel,
    ProductItemModel, ProductPriceModel, MetaTagModel,
    ProductCsvModel, ErrorDetailModel, ErrorType,
    CategoryCsvModel                        # ← import it
)
from app.models.schemas import ErrorDetailModel, ErrorType
from typing import List, Dict, Tuple, Optional  # Fix: Imported Optional

MODEL_MAP = {
    "brands":           BrandCsvModel,
    "attributes":       AttributeCsvModel,
    "return_policies":  ReturnPolicyCsvModel,
    "product_items":    ProductItemModel,
    "product_prices":   ProductPriceModel,
    "meta_tags":        MetaTagModel,
    "products":         ProductCsvModel,
    "categories":       CategoryCsvModel    
}

def check_category_hierarchy(
    records: List[Dict],
    session_id: str
) -> List[Dict]:
    """
    Ensure you never add a sub-category under one that already has products.
    We look up any product IDs mapped to a parent path.
    """
    errors = []
    for i, rec in enumerate(records):
        path = rec["category_path"]
        segments = path.split("/")
        # for each prefix (excluding full path), check if any products exist
        for level in range(1, len(segments)):
            parent_path = "/".join(segments[:level])
            # assumes you store product IDs under the key "products"
            if get_from_id_map(session_id, "products", parent_path):
                errors.append({
                    "row": i + 1,
                    "field": "category_path",
                    "error": (
                        f"Cannot create '{path}' under '{parent_path}': "
                        "existing products found in that category."
                    ),
                    "value": path
                })
                break
    return errors

def validate_csv(load_type: str, records: List[Dict], session_id: str) -> (List[Dict], List[Dict]):
    errors: List[Dict] = []
    valid_rows: List[Dict] = []

    Model = MODEL_MAP.get(load_type)
    if not Model:
        return [{"row": None, "field": None, "error": f"Unsupported load type: {load_type}"}], []

    for i, row in enumerate(records):
        try:
            inst = Model(**row)
            valid_rows.append(inst.dict())
        except ValidationError as e:
            for err in e.errors():
                errors.append({
                    "row": i + 1,
                    "field": ".".join(str(f) for f in err["loc"]),
                    "error": err["msg"]
                })

    # Category-specific business rule
    if load_type == "categories" and valid_rows:
        cat_errs = check_category_hierarchy(valid_rows, session_id)
        errors.extend(cat_errs)

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
