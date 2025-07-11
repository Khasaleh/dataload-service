from pydantic import ValidationError
from app.utils.redis_utils import get_from_id_map
from collections import defaultdict
from app.models.schemas import (
    BrandCsvModel, AttributeCsvModel, ReturnPolicyCsvModel, ProductItemModel, 
    ProductPriceModel, MetaTagModel # ProductCsvModel removed from here
)
from app.dataload.models.product_csv import ProductCsvModel # Import the canonical ProductCsvModel
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

def validate_csv(
    load_type: str,
    records: List[Dict],
    session_id: str
) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate raw CSV rows against their Pydantic model, enforce file-level uniqueness,
    and run any load-type–specific business rules (e.g. category hierarchy).
    Returns (errors, valid_rows).
    """
    errors: List[Dict] = []
    valid_rows: List[Dict] = []

    Model = MODEL_MAP.get(load_type)
    if not Model:
        return [{"row": None, "field": None, "error": f"Unsupported load type: {load_type}"}], []

    # 1) Per‐row model validation
    for idx, row in enumerate(records):
        try:
            inst = Model(**row)
            valid_rows.append(inst.model_dump())
        except ValidationError as e:
            for err in e.errors():
                errors.append({
                    "row": idx + 1,
                    "field": ".".join(str(f) for f in err["loc"]),
                    "error": err["msg"]
                })

    # 2) File‐level uniqueness check (if applicable)
    unique_key = UNIQUE_KEY_MAP.get(load_type)
    if unique_key and valid_rows:
        dup_errs = check_file_uniqueness(valid_rows, unique_key)
        errors.extend(dup_errs)

    # 3) Categories need additional hierarchy checks
    if load_type == "categories" and valid_rows:
        cat_errs = check_category_hierarchy(valid_rows, session_id)
        errors.extend(cat_errs)

    return errors, valid_rows

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
