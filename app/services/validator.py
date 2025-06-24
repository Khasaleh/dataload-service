
from app.models.schemas import (
    BrandCsvModel, AttributeCsvModel, ReturnPolicyCsvModel, ProductModel, # Corrected model names
    ProductItemModel, ProductPriceModel, MetaTagModel
    # Assuming AttributeModel was meant to be AttributeCsvModel, and ReturnPolicyModel to be ReturnPolicyCsvModel
)
from app.dataload.models.product_csv import ProductCsvModel # Import the correct ProductCsvModel
from pydantic import ValidationError
from app.utils.redis_utils import get_from_id_map
from collections import defaultdict

MODEL_MAP = {
    "brands": BrandCsvModel,
    "attributes": AttributeCsvModel,
    "return_policies": ReturnPolicyCsvModel,
    # "products": ProductModel, # Old, less detailed model
    "product_items": ProductItemModel,
    "product_prices": ProductPriceModel,
    "meta_tags": MetaTagModel,
    "products": ProductCsvModel # Added correct model for products load_type
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
from app.models.schemas import ErrorDetailModel, ErrorType # Import standardized error model
from typing import List, Tuple # For type hinting

def validate_csv(
    load_type: str,
    records: List[Dict],
    session_id: Optional[str] = None,
    record_key: Optional[str] = None,
    referenced_entity_map: Optional[Dict[str, str]] = None
) -> Tuple[List[ErrorDetailModel], List[Dict]]:

    all_error_details: List[ErrorDetailModel] = []
    valid_rows_data: List[Dict] = [] # Will store dicts of Pydantic model validated data

    model_class = MODEL_MAP.get(load_type)
    if not model_class:
        all_error_details.append(ErrorDetailModel(
            error_message=f"Unsupported load type: {load_type}",
            error_type=ErrorType.CONFIGURATION
        ))
        return all_error_details, []

    # 1. Pydantic model validation for each row
    temp_valid_for_further_checks = [] # Store records that pass Pydantic validation for subsequent checks

    for i, row_data in enumerate(records):
        row_num = i + 2 # CSV rows are often 1-indexed, plus header
        try:
            # Add business_details_id if it's expected by the model but not in CSV row directly
            # This is more relevant for ProductCsvModel if it's not passed from task.
            # For now, assume row_data is complete or models handle missing optional fields.
            # if load_type == "products" and 'business_details_id' not in row_data and business_id_for_session:
            #     row_data_with_biz = {**row_data, 'business_details_id': business_id_for_session}
            #     validated_model = model_class(**row_data_with_biz)
            # else:
            validated_model = model_class(**row_data)
            temp_valid_for_further_checks.append({"row_number": row_num, "data": validated_model.model_dump()})
            # valid_rows_data.append(validated_model.model_dump()) # Add to final valid list only if ALL checks pass for this row
        except ValidationError as e:
            for err in e.errors():
                all_error_details.append(ErrorDetailModel(
                    row_number=row_num,
                    field_name=".".join(str(f) for f in err['loc']) if err['loc'] else None,
                    error_message=err['msg'],
                    error_type=ErrorType.VALIDATION,
                    offending_value=str(err.get('input', 'N/A'))[:255] # Truncate offending value
                ))
        except Exception as ex: # Catch any other unexpected error during model instantiation
            all_error_details.append(ErrorDetailModel(
                row_number=row_num,
                error_message=f"Unexpected error validating row: {str(ex)}",
                error_type=ErrorType.UNEXPECTED_ROW_ERROR
            ))


    # If there were Pydantic errors, we might stop or only process fully valid rows.
    # For now, subsequent checks (uniqueness, referential) will run on records that passed Pydantic validation.
    # If a row failed Pydantic validation, it's already in all_error_details.

    # Create a list of just the data from Pydantic-valid records for further checks
    pydantic_valid_data_list = [item["data"] for item in temp_valid_for_further_checks]
    pydantic_valid_row_map = {idx: item for idx, item in enumerate(temp_valid_for_further_checks)}


    # 2. File-level uniqueness check (on Pydantic-valid data)
    if record_key and pydantic_valid_data_list:
        uniqueness_errors_raw = check_file_uniqueness(pydantic_valid_data_list, record_key)
        for err_dict in uniqueness_errors_raw:
            # map internal row indices back to original CSV row numbers
            original_rows_for_error = [pydantic_valid_row_map[internal_idx-1]["row_number"] for internal_idx in err_dict.get("rows", [])]
            all_error_details.append(ErrorDetailModel(
                # Uniqueness error applies to multiple rows, hard to assign one row_number.
                # Could list all offending rows in message.
                row_number=original_rows_for_error[0] if original_rows_for_error else None, # Report first row
                field_name=err_dict.get("field"),
                error_message=f"Duplicate key '{err_dict.get('key')}' found in file for field '{err_dict.get('field')}'. Offending CSV rows: {original_rows_for_error}",
                error_type=ErrorType.VALIDATION,
                offending_value=str(err_dict.get("key"))[:255]
            ))

    # 3. Referential integrity check (on Pydantic-valid data)
    if session_id and referenced_entity_map and pydantic_valid_data_list:
        for field_to_check, referenced_entity_type in referenced_entity_map.items():
            ref_errors_raw = check_referential_integrity(pydantic_valid_data_list, field_to_check, referenced_entity_type, session_id)
            for err_dict in ref_errors_raw:
                 # map internal row index back to original CSV row number
                original_row_num_for_error = pydantic_valid_row_map[err_dict.get("row", 1)-1]["row_number"] # err_dict["row"] is 1-based for pydantic_valid_data_list
                all_error_details.append(ErrorDetailModel(
                    row_number=original_row_num_for_error,
                    field_name=err_dict.get("field"),
                    error_message=f"Referenced '{referenced_entity_type}' with value '{err_dict.get('value')}' not found (from field '{err_dict.get('field')}').",
                    error_type=ErrorType.LOOKUP,
                    offending_value=str(err_dict.get("value"))[:255]
                ))

    # Determine final valid_rows_data: only include rows that did not appear in any error
    if all_error_details:
        errored_row_numbers = {err.row_number for err in all_error_details if err.row_number is not None}
        for item in temp_valid_for_further_checks:
            if item["row_number"] not in errored_row_numbers:
                valid_rows_data.append(item["data"])
    else: # No errors at all
        valid_rows_data = pydantic_valid_data_list

    return all_error_details, valid_rows_data
