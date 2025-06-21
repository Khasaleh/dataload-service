from typing import List, Dict, Tuple
from data_models import SKU

# Define valid values for status and published fields
VALID_STATUSES = {"ACTIVE", "INACTIVE"}
VALID_PUBLISHED_STATUSES = {"Published", "Unpublished"}

def validate_sku_list(skus: List[SKU]) -> List[SKU]:
    """
    Validates a list of SKU objects based on business rules.
    It appends errors to the sku.errors list if validation fails.
    """

    # Rule: Only one is_default_sku = true per product_name + business_details_id
    # We need to track default SKUs per product group
    default_sku_tracker: Dict[Tuple[str, str], int] = {} # (product_name, biz_id) -> original_row_index of default SKU

    for sku in skus:
        # If there are already parsing errors, we might skip some validations or be aware of them.
        # For now, validations will run even if parsing errors exist, potentially adding more specific errors.

        # --- Individual SKU Validations ---

        # Validate required fields (parser already adds errors for missing, this is a double check or for semantic emptiness)
        if not sku.product_name: sku.errors.append("Validation: Product name is required.")
        if not sku.business_details_id: sku.errors.append("Validation: Business details ID is required.")
        if not sku.main_attribute_name: sku.errors.append("Validation: Main attribute name is required.")
        if not sku.attributes: sku.errors.append("Validation: Attribute combination did not yield any attributes (e.g., empty string).")
        # Price and Quantity are checked for format in parser, here check for semantic validity if needed (e.g. > 0)
        if sku.price <= 0: # Assuming price must be positive. The spec says "numeric".
            sku.errors.append(f"Validation: Price must be a positive number, got {sku.price}.")
        if sku.quantity < 0: # Assuming quantity cannot be negative.
            sku.errors.append(f"Validation: Quantity cannot be negative, got {sku.quantity}.")


        # Validate 'status'
        if sku.status not in VALID_STATUSES:
            sku.errors.append(f"Validation: Invalid status '{sku.status}'. Must be one of {VALID_STATUSES}.")

        # Validate 'published'
        if sku.published not in VALID_PUBLISHED_STATUSES:
            sku.errors.append(f"Validation: Invalid published status '{sku.published}'. Must be one of {VALID_PUBLISHED_STATUSES}.")

        # Validate 'images'
        if len(sku.images) > 6:
            sku.errors.append(f"Validation: Too many images. Maximum 6 allowed, found {len(sku.images)}.")

        main_image_count = 0
        for img in sku.images:
            if img.is_main:
                main_image_count += 1
            # Basic URL validation
            if not (img.url.startswith("http://") or img.url.startswith("https://")):
                sku.errors.append(f"Validation: Invalid image URL format '{img.url}'. Must start with http:// or https://.")

        if sku.images: # Only apply main image count validation if there are images
            if main_image_count == 0:
                sku.errors.append("Validation: No main image specified. Exactly one image must be marked as main_image:true.")
            elif main_image_count > 1:
                sku.errors.append(f"Validation: Multiple main images specified ({main_image_count}). Exactly one image must be marked as main_image:true.")

        # Validate 'attribute_combination' (Rule: "Attribute combinations must match existing attribute_value records")
        # This is complex without DB access. For now, we've parsed them.
        # We can check if any ParsedAttribute has an empty value, which would be invalid.
        for pa in sku.attributes:
            if not pa.attribute_name: # Should not happen with current parser for main_attribute
                 sku.errors.append(f"Validation: An attribute was parsed with no name (value: '{pa.attribute_value}').")
            if not pa.attribute_value:
                 sku.errors.append(f"Validation: Attribute '{pa.attribute_name}' has an empty value.")

        # --- Cross-SKU Validations (is_default_sku) ---
        product_key = (sku.product_name, sku.business_details_id)
        if sku.is_default_sku:
            if not sku.product_name or not sku.business_details_id:
                # This error would typically be caught by required field checks first.
                # If a default SKU is marked but its identifiers are missing, it's an issue for grouping.
                sku.errors.append("Validation: 'is_default_sku' is true, but product_name or business_details_id is missing, cannot verify uniqueness.")
            elif product_key in default_sku_tracker:
                # Another SKU for this product is already marked as default. This one is an error.
                # And the one previously marked also becomes an error retrospectively (or the first one wins).
                # Let's say the first one encountered wins, and subsequent ones are errors.
                sku.errors.append(f"Validation: Another SKU (e.g., from row {default_sku_tracker[product_key]}) is already marked as default for product '{sku.product_name}' (Business ID: {sku.business_details_id}). Only one default SKU is allowed per product.")
                # Optionally, find the first SKU and add an error to it too, or decide on a "first wins" or "last wins" policy.
                # For simplicity, current SKU gets the error. The problem implies "Only one SKU per product can have is_default_sku = true".
                # This means any row that violates this should be flagged.
            else:
                default_sku_tracker[product_key] = sku.original_row_index

    # --- Cross-SKU Validations (is_default_sku uniqueness) ---
    # Collect all SKUs that claim to be default, grouped by their product key.
    default_sku_candidates: Dict[Tuple[str, str], List[SKU]] = {}
    for sku in skus:
        if sku.is_default_sku:
            # Ensure product_name and business_details_id are present for grouping.
            # If not, it's a separate error already caught or should be.
            if sku.product_name and sku.business_details_id:
                product_key = (sku.product_name, sku.business_details_id)
                if product_key not in default_sku_candidates:
                    default_sku_candidates[product_key] = []
                default_sku_candidates[product_key].append(sku)
            else:
                # This SKU is marked default but is missing key identifiers.
                # This error should ideally be caught by required field checks for product_name/business_details_id.
                # If it wasn't, or to be explicit for default SKU logic:
                sku.errors.append(
                    f"Validation (Row {sku.original_row_index}): SKU is marked as default but is missing product_name or business_details_id, preventing uniqueness check."
                )

    # Now, iterate through the collected candidates and flag groups with multiple defaults.
    for product_key, default_list in default_sku_candidates.items():
        if len(default_list) > 1:
            row_indices = sorted([s.original_row_index for s in default_list if s.original_row_index is not None])
            error_msg = (
                f"Validation: Multiple SKUs (from CSV rows: {row_indices}) are marked as 'is_default_sku=true' "
                f"for the same product group (Product: '{product_key[0]}', Business ID: '{product_key[1]}'). "
                f"Only one SKU per product group can be the default."
            )
            for sku_to_flag in default_list:
                # Add this error to each of the conflicting SKUs.
                # Avoid adding duplicate messages if this validation is somehow run multiple times,
                # or if a similar message was already added.
                if error_msg not in sku_to_flag.errors:
                    sku_to_flag.errors.append(error_msg)
        elif not default_list and skus: # No default SKU found for a product group that has SKUs
            # This check requires knowing all SKUs for a product group, not just default candidates.
            # This specific check ("no default SKU for a product group") is better done
            # by sku_processor or if validator had full group context.
            # For now, the validator focuses on "more than one default".
            # The processor will note if a group has no default.
            pass


    return skus


if __name__ == '__main__':
    from csv_parser import load_skus_from_csv # Assuming csv_parser.py is in the same directory or PYTHONPATH

    # Use the test CSV created by csv_parser.py or create a new one for specific validation tests
    test_csv_file = 'test_product_items.csv'
    # Make sure test_product_items.csv exists (it's created if you run csv_parser.py)
    # You might want to add more specific validation test cases to this CSV for thorough testing.
    # Example: two SKUs for the same product marked as default.
    # Example: image count > 6, or no main image.

    # Re-create dummy CSV with validation-specific cases if csv_parser's isn't sufficient
    extended_dummy_csv_content = """product_name,business_details_id,main_attribute,attribute_combination,is_default_sku,price,discount_price,quantity,status,published,order_limit,package_size_length,package_size_width,package_size_height,package_weight,images
# Valid Case
Valid Product,1,Color,Red,True,100,80,10,ACTIVE,Published,,,,,,,https://cdn.com/img1.jpg|main_image:true
# Invalid Status
Product Status,2,Color,Blue,True,100,,10,INVALID_STATUS,Published,,,,,,,https://cdn.com/img1.jpg|main_image:true
# Invalid Published
Product Published,3,Color,Green,True,100,,10,ACTIVE,INVALID_PUBLISHED,,,,,,,https://cdn.com/img1.jpg|main_image:true
# Too many images
Product Many Images,4,Size,L,True,50,,5,ACTIVE,Published,,,,,,,https://cdn.com/1.jpg|main_image:false|https://cdn.com/2.jpg|main_image:false|https://cdn.com/3.jpg|main_image:false|https://cdn.com/4.jpg|main_image:false|https://cdn.com/5.jpg|main_image:false|https://cdn.com/6.jpg|main_image:false|https://cdn.com/7.jpg|main_image:true
# No main image
Product No Main Image,5,Size,M,True,50,,5,ACTIVE,Published,,,,,,,https://cdn.com/1.jpg|main_image:false|https://cdn.com/2.jpg|main_image:false
# Multiple main images
Product Multi Main,6,Size,S,True,50,,5,ACTIVE,Published,,,,,,,https://cdn.com/1.jpg|main_image:true|https://cdn.com/2.jpg|main_image:true
# Invalid URL
Product Invalid URL,7,Material,Cotton,True,60,,3,ACTIVE,Published,,,,,,,ftp://cdn.com/img.png|main_image:true
# Multiple Default SKUs for same product
DefaultTestProd,10,Color,Red,True,120,99,30,ACTIVE,Published,,,,,,,https://cdn.com/red1.jpg|main_image:true
DefaultTestProd,10,Color,Blue,True,120,,20,ACTIVE,Published,,,,,,,https://cdn.com/blue1.jpg|main_image:true
DefaultTestProd,10,Color,Green,False,120,,15,ACTIVE,Published,,,,,,,https://cdn.com/green1.jpg|main_image:true
# Negative Price/Quantity
Product Negatives,11,Color,Yellow,True,-5,-2,-1,ACTIVE,Published,,,,,,,https://cdn.com/yellow.jpg|main_image:true
# Empty Attribute Value
Product Empty AttrVal,12,Color,,True,20,,5,ACTIVE,Published,,,,,,,https://cdn.com/img.jpg|main_image:true
Product Empty AttrVal2,12,Color,Red|,True,20,,5,ACTIVE,Published,,,,,,,https://cdn.com/img.jpg|main_image:true
"""
    validation_test_csv_file = 'validation_test_product_items.csv'
    with open(validation_test_csv_file, 'w', newline='', encoding='utf-8') as f:
        f.write(extended_dummy_csv_content)

    print(f"\n--- Validating SKUs from {validation_test_csv_file} ---")
    # Load SKUs using the parser (which also does initial parsing error checks)
    parsed_skus = load_skus_from_csv(validation_test_csv_file)

    # Validate the parsed SKUs
    validated_skus = validate_sku_list(parsed_skus)

    print(f"\nValidation Results (Total SKUs processed: {len(validated_skus)}):\n")
    has_any_errors = False
    for sku_item in validated_skus:
        print(f"--- SKU from CSV Row {sku_item.original_row_index} (Product: {sku_item.product_name}, BizID: {sku_item.business_details_id}) ---")
        if sku_item.errors:
            has_any_errors = True
            print(f"  ERRORS for row {sku_item.original_row_index}:")
            for error in sku_item.errors:
                print(f"    - {error}")
        else:
            print("  No errors found.")
        print("")

    if not has_any_errors:
        print("All SKUs in the validation test file passed all checks (or had no data to trigger errors).")

    # Consider cleaning up:
    # import os
    # os.remove(validation_test_csv_file)
    print(f"Validation test CSV file '{validation_test_csv_file}' created. You may want to inspect or delete it.")
