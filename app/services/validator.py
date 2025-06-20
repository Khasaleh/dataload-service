
from app.models.schemas import (
    BrandModel, AttributeModel, ReturnPolicyModel, ProductModel,
    ProductItemModel, ProductPriceModel, MetaTagModel
)
from pydantic import ValidationError
from app.services.storage import get_from_id_map
from collections import defaultdict

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
            key_counts[key_value].append(i + 1) # Store 1-based row index

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
        if key_value: # Only check if the field has a value
            # Assuming get_from_id_map returns None or an empty list if not found
            if not get_from_id_map(session_id, referenced_entity_type, key_value):
                errors.append({
                    "row": i + 1,
                    "field": field_to_check,
                    "error": f"Referenced {referenced_entity_type} not found",
                    "value": key_value
                })
    return errors


def validate_csv(load_type, records, session_id: str = None, record_key: str = None, referenced_entity_map: dict = None):
    model_errors = []
    file_uniqueness_errors = []
    referential_integrity_errors = []
    valid_rows = []

    model = MODEL_MAP.get(load_type)
    if not model:
        return [{"error": f"Unsupported load type: {load_type}"}], []

    # 1. Pydantic model validation
    for i, row in enumerate(records):
        try:
            validated_model = model(**row)
            valid_rows.append(validated_model.dict())
        except ValidationError as e:
            for err in e.errors():
                model_errors.append({
                    "row": i + 1,
                    "field": ".".join(str(f) for f in err['loc']),
                    "error": err['msg']
                })

    # If Pydantic validation fails for some rows, we might not want to proceed with other checks for those rows,
    # or we might want to only perform further checks on valid_rows.
    # For now, let's assume further checks are performed on all original records if the model itself is valid.
    # If valid_rows is empty and records is not, it means all rows failed Pydantic validation.

    if not model_errors: # Proceed to other validations only if basic model validation passes for all rows
        # 2. File-level uniqueness check (only on the original records that passed Pydantic validation if needed)
        # For simplicity, checking on all records for now.
        if record_key:
            file_uniqueness_errors = check_file_uniqueness(records, record_key)

        # 3. Referential integrity check
        if session_id and referenced_entity_map:
            for field_to_check, referenced_entity_type in referenced_entity_map.items():
                referential_integrity_errors.extend(
                    check_referential_integrity(records, field_to_check, referenced_entity_type, session_id)
                )

    all_errors = model_errors + file_uniqueness_errors + referential_integrity_errors

    # If there were model errors, valid_rows might not contain all original records.
    # The caller needs to be aware of this. If model_errors exist, valid_rows contains only Pydantic-valid rows.
    # If no model_errors, valid_rows contains all rows.
    # For consistency, if there are *any* errors, perhaps valid_rows should be considered empty or only truly valid ones.
    # Let's adjust: if any error occurs, the valid_rows for those error cases are implicitly not valid.
    # The current valid_rows only contains rows that passed pydantic validation.
    # If other errors occur, those rows are also invalid.

    if all_errors:
        # If there are any errors, we should clarify which rows are considered "valid"
        # For now, let's return all Pydantic-valid rows, and the caller can decide based on errors.
        # Or, more strictly, if any error, no rows are "valid" for processing.
        # Let's stick to returning Pydantic-valid rows for now and let the caller handle it.
        pass

    return all_errors, valid_rows
