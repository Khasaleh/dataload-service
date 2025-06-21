import csv
from typing import List, Optional, Tuple
from data_models import SKU, ProductImage, ParsedAttribute

def parse_bool(value: str) -> bool:
    """Converts common string representations of boolean to bool."""
    return value.strip().lower() == 'true'

def parse_optional_float(value: str) -> Optional[float]:
    """Converts string to float if not empty, else None."""
    if not value or value.strip() == '':
        return None
    try:
        return float(value)
    except ValueError:
        return None # Or raise an error/log a warning

def parse_optional_int(value: str) -> Optional[int]:
    """Converts string to int if not empty, else None."""
    if not value or value.strip() == '':
        return None
    try:
        return int(value)
    except ValueError:
        return None # Or raise an error/log a warning

def parse_images(image_str: str) -> Tuple[List[ProductImage], List[str]]:
    """
    Parses the image string into a list of ProductImage objects.
    Example: "https://cdn.com/img1.jpg|main_image:true|https://cdn.com/img2.jpg|main_image:false"
    Returns a tuple: (list_of_product_images, list_of_errors)
    """
    images: List[ProductImage] = []
    errors: List[str] = []
    if not image_str or image_str.strip() == '':
        return images, errors

    parts = image_str.split('|')
    # "Up to 6 image URLs, each followed by |main_image:true/false" - this is clear.
    # So, parts must be an even number if not empty.
    if len(parts) % 2 != 0:
        errors.append(f"Image string '{image_str}' has an invalid format. It should be pairs of URL and main_image flag (e.g., url|main_image:true). Found odd number of parts.")
        return images, errors # Return early if format is fundamentally broken

    idx = 0
    while idx < len(parts):
        url = parts[idx].strip()
        flag_part_str = parts[idx+1].strip().lower() # Get flag part for error reporting if needed

        if not url: # URL part is empty
            # If an empty URL is part of a pair, it's likely an error or ignorable.
            # Let's record an error if its corresponding flag part is not also empty or implies intentional skip.
            # For now, let's assume an empty URL is an error if the flag part is not also clearly ignorable.
            errors.append(f"Empty image URL found in image string: '{image_str}' at part {idx+1}.")
            idx += 2 # Move to next pair
            continue

        if idx + 1 >= len(parts): # Should be caught by len(parts) % 2 != 0, but as safeguard
            errors.append(f"Missing main_image flag for URL '{url}' in '{image_str}'.")
            break

        is_main = False
        if flag_part_str == "main_image:true":
            is_main = True
        elif flag_part_str == "main_image:false":
            is_main = False
        else:
            errors.append(f"Invalid main_image flag format: '{parts[idx+1]}' for URL '{url}'. Expected 'main_image:true' or 'main_image:false'.")
            # Default to is_main = False if flag is malformed, but error is logged.

        images.append(ProductImage(url=url, is_main=is_main))
        idx += 2

    return images, errors

def parse_attribute_combination(main_attribute: str, combination_str: str) -> Tuple[List[ParsedAttribute], List[str]]:
    """
    Parses the main_attribute and attribute_combination string.
    Example: main_attribute="Color", combination_str="Red|M"
    This implies the first part of attribute_combination ("Red") belongs to main_attribute ("Color").
    Subsequent parts ("M") are for other attributes whose names are not in the CSV row.
    We assign placeholder names like "secondary_attribute_1" for these.
    """
    parsed_attributes: List[ParsedAttribute] = []
    errors: List[str] = []

    if not main_attribute or not main_attribute.strip():
        errors.append("Main attribute name is missing or empty.")
        # If main_attribute name is missing, we can't accurately parse the first attribute.
        # Depending on strictness, we could try to parse combination_str with placeholders for all,
        # or return early. Returning early seems safer.
        return parsed_attributes, errors

    if not combination_str or not combination_str.strip():
        errors.append("Attribute combination string is missing or empty.")
        # If combination_str is empty, no attributes can be parsed.
        return parsed_attributes, errors

    attr_values = [val.strip() for val in combination_str.split('|')]

    if not attr_values or not attr_values[0]: # First attribute value must exist
        errors.append("Attribute combination string is present but the first attribute value is missing after splitting (e.g. 'Color| |Value2').")
        return parsed_attributes, errors

    # First value corresponds to the main_attribute
    parsed_attributes.append(ParsedAttribute(attribute_name=main_attribute.strip(), attribute_value=attr_values[0]))

    # Subsequent values are for other, unnamed attributes from CSV perspective
    for i, value in enumerate(attr_values[1:]):
        if not value: # Handle empty subsequent attribute values, e.g. "Red||Blue"
            errors.append(f"Empty value found for secondary attribute at position {i+1} in combination '{combination_str}'.")
            # Optionally, skip adding this attribute or add with empty value and rely on validation.
            # For now, let's add it, validator can check for empty values.
            parsed_attributes.append(ParsedAttribute(attribute_name=f"secondary_attribute_{i+1}", attribute_value=""))
        else:
            parsed_attributes.append(ParsedAttribute(attribute_name=f"secondary_attribute_{i+1}", attribute_value=value))

    return parsed_attributes, errors


def parse_csv_row(row: dict, original_row_index: int) -> SKU: # Changed row_index to original_row_index
    """Parses a single CSV row (as a dict) into an SKU object."""
    errors: List[str] = []

    # Helper for required string fields
    def get_required_str(field_name: str) -> str:
        val = row.get(field_name)
        if val is None or not val.strip():
            errors.append(f"Missing required field: {field_name} (Row {original_row_index}).") # Changed to original_row_index
            return ""
        return val.strip()

    product_name = get_required_str('product_name')
    business_details_id = get_required_str('business_details_id')
    main_attribute_csv = get_required_str('main_attribute')
    attribute_combination_csv = get_required_str('attribute_combination')

    price_str = row.get('price', '')
    price: Optional[float] = None
    if not price_str or price_str.strip() == '':
        errors.append(f"Missing required field: price (Row {original_row_index}).")
        price = 0.0 # Assign a default for object creation, error is logged
    else:
        try:
            price = float(price_str)
            # Price validation (e.g. non-negative) is done in the validator.py
        except ValueError:
            errors.append(f"Invalid format for price: '{price_str}' (Row {original_row_index}). Must be a number.")
            price = 0.0

    quantity_str = row.get('quantity', '')
    quantity: Optional[int] = None
    if not quantity_str or quantity_str.strip() == '':
        errors.append(f"Missing required field: quantity (Row {original_row_index}).")
        quantity = 0 # Assign a default, error is logged
    else:
        try:
            quantity = int(quantity_str)
            # Quantity validation (e.g. non-negative) is in validator.py
        except ValueError:
            errors.append(f"Invalid format for quantity: '{quantity_str}' (Row {original_row_index}). Must be an integer.")
            quantity = 0

    parsed_attributes, attr_errors = parse_attribute_combination(main_attribute_csv, attribute_combination_csv)
    if attr_errors: # Add context to attribute parsing errors
        for err in attr_errors:
            errors.append(f"{err} (Row {original_row_index}, MainAttr: '{main_attribute_csv}', Combo: '{attribute_combination_csv}')")


    images_str = row.get('images', '')
    product_images, img_errors = parse_images(images_str)
    if img_errors: # Add context to image parsing errors
        for err in img_errors:
            errors.append(f"{err} (Row {original_row_index}, ImagesStr: '{images_str[:50]}...')")


    sku = SKU(
        product_name=product_name,
        business_details_id=business_details_id,
        main_attribute_name=main_attribute_csv,
        attributes=parsed_attributes,
        is_default_sku=parse_bool(row.get('is_default_sku', 'false')),
        price=price, # price will be float, even if it was 0.0 due to error
        discount_price=parse_optional_float(row.get('discount_price', '')),
        quantity=quantity, # quantity will be int, even if it was 0 due to error
        status=get_required_str('status'),
        published=get_required_str('published'),
        order_limit=parse_optional_int(row.get('order_limit', '')),
        package_size_length=parse_optional_float(row.get('package_size_length', '')),
        package_size_width=parse_optional_float(row.get('package_size_width', '')),
        package_size_height=parse_optional_float(row.get('package_size_height', '')),
        package_weight=parse_optional_float(row.get('package_weight', '')),
        images=product_images,
        original_row_index=original_row_index, # This was the parameter, so it's correct
        errors=errors # All accumulated errors for this row
    )
    return sku

def load_skus_from_csv(file_path: str) -> List[SKU]:
    """Loads SKUs from a CSV file."""
    skus: List[SKU] = []
    try:
        with open(file_path, mode='r', encoding='utf-8') as infile:
            # Read lines manually to skip comments before passing to DictReader
            # This is a bit more involved; an easier way is to filter rows from DictReader.
            # However, DictReader needs a file-like object.
            # Alternative: check the raw line if possible, or check first field.

            # Simpler: filter rows after DictReader yields them, but adjust row indexing.
            # For DictReader to work, it needs to read the header correctly.
            # We can filter rows if they are "comment-like" based on their content.

            reader = csv.DictReader(infile)
            processed_data_row_idx = 0 # For calculating original_row_index correctly
            for i, row_dict in enumerate(reader):
                # Check if the row is a comment row or empty
                # A simple check: if the first field's value starts with #, or if all values are empty
                first_field_name = reader.fieldnames[0] if reader.fieldnames else None
                if first_field_name and row_dict.get(first_field_name, "").strip().startswith("#"):
                    # This is a comment line, skip it.
                    # original_row_index still increments based on physical line number.
                    # The 'i' from enumerate(reader) is the physical data row index (0-based after header).
                    print(f"Skipping comment line in CSV (approx physical row {i+2}): {row_dict.get(first_field_name)}")
                    continue

                # Check for completely empty rows (all values are None or empty strings)
                if all(not value for value in row_dict.values()):
                    print(f"Skipping empty line in CSV (approx physical row {i+2})")
                    continue

                # CSV row numbers are typically 1-based. Header is row 1. Data starts row 2.
                # 'i' is the 0-based index of rows yielded by DictReader *including* filtered ones.
                # To maintain correct original_row_index for error reporting relative to the physical file:
                # We use 'i + 2' because 'i' is the 0-based index from the header.
                current_physical_row_in_file = i + 2
                sku = parse_csv_row(row_dict, original_row_index=current_physical_row_in_file)
                skus.append(sku)
                processed_data_row_idx +=1

    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred while reading the CSV file {file_path}: {e}")
    return skus

# Removed the if __name__ == '__main__' block to simplify debugging of SyntaxError.
# The main testing will be done via main.py.
