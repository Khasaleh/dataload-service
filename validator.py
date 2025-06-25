from pydantic import ValidationError
from app.utils.redis_utils import get_from_id_map
from collections import defaultdict

# Model mapping dictionary to map load types to Pydantic models (will be lazy-loaded in validate_csv)
MODEL_MAP = {
    "brands": None,  # Lazy load of models will happen inside the function.
    "attributes": None,
    "return_policies": None,
    "product_items": None,
    "product_prices": None,
    "meta_tags": None,
    "products": None  # Lazy load of models
}

def validate_csv(load_type, records):
    # Import models only when needed to prevent circular import
    from app.models.schemas import (
        BrandCsvModel, AttributeCsvModel, ReturnPolicyCsvModel, ProductItemModel,
        ProductPriceModel, MetaTagModel, ProductCsvModel
    )

    errors = []
    valid_rows = []

    # Set the appropriate model based on the load_type
    model = MODEL_MAP.get(load_type)

    if not model:
        if load_type == "brands":
            model = BrandCsvModel
        elif load_type == "attributes":
            model = AttributeCsvModel
        elif load_type == "return_policies":
            model = ReturnPolicyCsvModel
        elif load_type == "product_items":
            model = ProductItemModel
        elif load_type == "product_prices":
            model = ProductPriceModel
        elif load_type == "meta_tags":
            model = MetaTagModel
        elif load_type == "products":
            model = ProductCsvModel
        else:
            return [{"error": f"Unsupported load type: {load_type}"}], []

    # Continue with the validation logic
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

def check_file_uniqueness(records: list[dict], unique_key: str) -> list[dict]:
    """
    Checks for duplicate values of a specified key within a list of records.
    Returns a list of error messages for any duplicate keys found.
    """
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
    records: list[dict],
    field_to_check: str,
    referenced_entity_type: str,
    session_id: str
) -> list[dict]:
    """
    Checks if referenced entities exist in Redis.
    Returns a list of error messages for any references not found.
    """
    errors = []
    for i, record in enumerate(records):
        key_value = record.get(field_to_check)
        if key_value:  # Only check if the field has a value
            # Assuming get_from_id_map returns None or an empty list if not found
            if not get_from_id_map(session_id, referenced_entity_type, key_value):
                errors.append({
                    "row": i + 1,
                    "field": field_to_check,
                    "error": f"Referenced {referenced_entity_type} not found",
                    "value": key_value
                })
    return errors
