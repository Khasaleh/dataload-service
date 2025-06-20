import csv
import io
from typing import List, Dict, Tuple
from app.models.schemas import BrandValidationResult, BrandCreate, AttributeValidationResult, AttributeCreate, ReturnPolicyValidationResult, ReturnPolicyCreate # Add new models
from pydantic import ValidationError # To catch Pydantic validation errors

async def validate_brands_csv(file_content: bytes) -> BrandValidationResult:
    result = BrandValidationResult()
    seen_brand_names = set()

    try:
        # Decode bytes to string and use StringIO to mimic a file
        content_str = file_content.decode('utf-8')
        csvfile = io.StringIO(content_str)
        reader = csv.DictReader(csvfile)

        if not reader.fieldnames or 'brand_name' not in reader.fieldnames:
            result.is_valid = False
            result.errors.append({"line_number": 1, "field": "CSV Headers", "error": "Missing 'brand_name' header."})
            return result

        for i, row in enumerate(reader):
            line_number = i + 2 # Account for header row

            brand_name = row.get('brand_name', '').strip()

            if not brand_name:
                result.is_valid = False
                result.errors.append({
                    "line_number": line_number,
                    "field": "brand_name",
                    "error": "is required"
                })
                continue # Continue to find all errors

            if brand_name in seen_brand_names:
                result.is_valid = False
                result.errors.append({
                    "line_number": line_number,
                    "field": "brand_name",
                    "error": f"'{brand_name}' is not unique in this file."
                })
            seen_brand_names.add(brand_name)

            # Optional: Pydantic validation for each row if more complex rules were in BrandCreate
            # try:
            #     BrandCreate(brand_name=brand_name)
            # except ValueError as e: # Pydantic's ValidationError
            #     result.is_valid = False
            #     result.errors.append({
            #         "line_number": line_number,
            #         "field": "brand_name", # Or more specific field from Pydantic
            #         "error": str(e)
            #     })

    except UnicodeDecodeError:
        result.is_valid = False
        result.errors.append({"line_number": 1, "field": "File Encoding", "error": "File must be UTF-8 encoded."})
        return result
    except csv.Error as e:
        result.is_valid = False
        result.errors.append({"line_number": 1, "field": "CSV Format", "error": f"Invalid CSV format: {e}"})
        return result
    except Exception as e: # Catch any other unexpected errors during validation
        result.is_valid = False
        result.errors.append({"line_number": "N/A", "field": "General Error", "error": f"An unexpected error occurred: {str(e)}"})

    return result


async def validate_return_policies_csv(file_content: bytes) -> ReturnPolicyValidationResult:
    result = ReturnPolicyValidationResult()
    seen_policy_codes = set()

    try:
        content_str = file_content.decode('utf-8')
        csvfile = io.StringIO(content_str)
        reader = csv.DictReader(csvfile)

        required_headers = ['return_policy_code', 'name', 'return_window_days', 'grace_period_days'] # 'description' is often optional
        if not reader.fieldnames or not all(header in reader.fieldnames for header in required_headers):
            missing = [h for h in required_headers if not reader.fieldnames or h not in reader.fieldnames]
            result.is_valid = False
            result.errors.append({"line_number": 1, "field": "CSV Headers", "error": f"Missing required headers: {', '.join(missing)}."})
            return result

        for i, row in enumerate(reader):
            line_number = i + 2
            current_row_errors = []

            # Basic presence checks
            for header in required_headers:
                if not row.get(header, '').strip() and header != 'description': # Description can be empty
                     current_row_errors.append({
                        "line_number": line_number,
                        "field": header,
                        "error": "is required"
                    })

            return_policy_code = row.get('return_policy_code', '').strip()
            if return_policy_code:
                if return_policy_code in seen_policy_codes:
                    current_row_errors.append({
                        "line_number": line_number,
                        "field": "return_policy_code",
                        "error": f"'{return_policy_code}' is not unique in this file."
                    })
                else:
                    seen_policy_codes.add(return_policy_code)

            # Attempt Pydantic validation for numeric fields and constraints
            try:
                # Pydantic will try to convert numeric strings to int
                ReturnPolicyCreate(
                    return_policy_code=return_policy_code,
                    name=row.get('name', '').strip(),
                    return_window_days=row.get('return_window_days'), # Let Pydantic handle conversion/validation
                    grace_period_days=row.get('grace_period_days'),   # Let Pydantic handle conversion/validation
                    description=row.get('description', '').strip()
                )
            except ValidationError as e:
                for error in e.errors():
                    field_name = error['loc'][0] if error['loc'] else 'unknown_field'
                    current_row_errors.append({
                        "line_number": line_number,
                        "field": field_name,
                        "error": error['msg']
                    })

            if current_row_errors:
                result.is_valid = False
                result.errors.extend(current_row_errors)

    except UnicodeDecodeError:
        result.is_valid = False
        result.errors.append({"line_number": 1, "field": "File Encoding", "error": "File must be UTF-8 encoded."})
        return result
    except csv.Error as e:
        result.is_valid = False
        result.errors.append({"line_number": 1, "field": "CSV Format", "error": f"Invalid CSV format: {e}"})
        return result
    except Exception as e:
        result.is_valid = False
        result.errors.append({"line_number": "N/A", "field": "General Error", "error": f"An unexpected error occurred: {str(e)}"})

    return result


async def validate_attributes_csv(file_content: bytes) -> AttributeValidationResult:
    result = AttributeValidationResult()
    seen_attribute_names = set()

    try:
        content_str = file_content.decode('utf-8')
        csvfile = io.StringIO(content_str)
        reader = csv.DictReader(csvfile)

        required_headers = ['attribute_name', 'allowed_values']
        if not reader.fieldnames or not all(header in reader.fieldnames for header in required_headers):
            missing = [h for h in required_headers if not reader.fieldnames or h not in reader.fieldnames]
            result.is_valid = False
            result.errors.append({"line_number": 1, "field": "CSV Headers", "error": f"Missing required headers: {', '.join(missing)}."})
            return result

        for i, row in enumerate(reader):
            line_number = i + 2 # Account for header row

            attribute_name = row.get('attribute_name', '').strip()
            allowed_values = row.get('allowed_values', '').strip()

            # Validate attribute_name
            if not attribute_name:
                result.is_valid = False
                result.errors.append({
                    "line_number": line_number,
                    "field": "attribute_name",
                    "error": "is required"
                })
            elif attribute_name in seen_attribute_names:
                result.is_valid = False
                result.errors.append({
                    "line_number": line_number,
                    "field": "attribute_name",
                    "error": f"'{attribute_name}' is not unique in this file."
                })
            seen_attribute_names.add(attribute_name)

            # Validate allowed_values
            if not allowed_values:
                result.is_valid = False
                result.errors.append({
                    "line_number": line_number,
                    "field": "allowed_values",
                    "error": "is required"
                })
            # Optional: Further validation on allowed_values format (e.g., no empty parts like "Red||Blue")
            # values_list = [v.strip() for v in allowed_values.split('|')]
            # if not all(values_list) and allowed_values: # Checks for empty strings if allowed_values is not empty itself
            #     result.is_valid = False
            #     result.errors.append({
            #         "line_number": line_number,
            #         "field": "allowed_values",
            #         "error": "contains empty values when split by '|'."
            #     })

            # If row is partially invalid, we might skip Pydantic validation or do it field by field
            if not result.is_valid and any(err['line_number'] == line_number for err in result.errors):
                continue # Skip Pydantic if basic errors already found for this line to avoid redundant errors

            # Pydantic validation for the row (if needed for more complex type checks)
            # try:
            #     AttributeCreate(attribute_name=attribute_name, allowed_values=allowed_values)
            # except ValueError as e: # Pydantic's ValidationError
            #     result.is_valid = False
            #     result.errors.append({
            #         "line_number": line_number,
            #         "field": "row_data",
            #         "error": str(e)
            #     })

    except UnicodeDecodeError:
        result.is_valid = False
        result.errors.append({"line_number": 1, "field": "File Encoding", "error": "File must be UTF-8 encoded."})
        return result
    except csv.Error as e:
        result.is_valid = False
        result.errors.append({"line_number": 1, "field": "CSV Format", "error": f"Invalid CSV format: {e}"})
        return result
    except Exception as e:
        result.is_valid = False
        result.errors.append({"line_number": "N/A", "field": "General Error", "error": f"An unexpected error occurred: {str(e)}"})

    return result
