from csv_parser import load_skus_from_csv
from validator import validate_sku_list
from sku_processor import process_skus, ProcessedSKUData # Ensure ProcessedSKUData is imported if not already
from data_models import SKU # For type hinting if needed

def run_sku_processing_pipeline(csv_file_path: str) -> ProcessedSKUData:
    """
    Runs the full SKU processing pipeline:
    1. Load SKUs from CSV.
    2. Validate SKUs.
    3. Process SKUs into output structures.
    Returns a ProcessedSKUData object containing the results and any errors.
    """
    print(f"Starting SKU processing for file: {csv_file_path}\n")

    # 1. Load SKUs from CSV
    print("Step 1: Loading SKUs from CSV...")
    raw_skus = load_skus_from_csv(csv_file_path)
    if not raw_skus:
        print("No SKUs loaded. Exiting.")
        return ProcessedSKUData(processing_errors=["Failed to load any SKUs from the CSV."])
    print(f"Loaded {len(raw_skus)} raw SKUs from CSV.\n")

    # Print initial parsing errors
    print("--- Initial Parsing Errors (if any) ---")
    found_parsing_errors = False
    for sku in raw_skus:
        if sku.product_name and sku.errors: # Only print if there's a product name to identify the row somewhat
            print(f"Row ~{sku.original_row_index} (Product: {sku.product_name}): Errors: {sku.errors}")
            found_parsing_errors = True
    if not found_parsing_errors:
        print("No parsing errors found in initial load.")
    print("---------------------------------------\n")

    # 2. Validate SKUs
    print("Step 2: Validating SKUs...")
    # The validator adds errors to sku.errors list
    validated_skus = validate_sku_list(raw_skus)
    print("Finished SKU validation.\n")

    print("--- Validation Errors (extending parsing errors, if any) ---")
    skus_with_validation_errors_count = 0
    for sku in validated_skus:
        # Display errors if any were added *during validation* or if they existed before
        # This will show cumulative errors.
        if sku.product_name and sku.errors:
             # To avoid re-printing parsing errors if we only want to show new validation errors,
             # one would need to compare error lists before and after.
             # For now, show all errors on items that have any.
            print(f"Row ~{sku.original_row_index} (Product: {sku.product_name}): Final Errors: {sku.errors}")
            skus_with_validation_errors_count +=1

    if skus_with_validation_errors_count == 0:
        print("No SKUs found with errors after validation.")
    else:
        print(f"{skus_with_validation_errors_count} SKUs have errors after parsing and/or validation.")
    print("-----------------------------------------------------------\n")

    # 3. Process Validated SKUs
    # The processor internally skips SKUs that have errors.
    print("Step 3: Processing SKUs into final data structures...")
    processed_data = process_skus(validated_skus)
    print("Finished SKU processing.\n")

    return processed_data

def print_processing_results(processed_data: ProcessedSKUData):
    """Prints the results from the SKU processing pipeline."""
    print("--- Processing Results ---")

    if processed_data.processing_errors:
        print("\nOverall Processing Errors Encountered by the Processor:")
        for err in processed_data.processing_errors:
            print(f"  - {err}")
    else:
        print("\nNo overall processing errors reported by the processor.")

    print(f"\nMain SKUs Created: {len(processed_data.main_skus)}")
    for ms in processed_data.main_skus:
        print(f"  MainSKU ID: {ms.main_sku_identifier} (Source Row: {ms.original_row_index_of_default_source})")
        print(f"    Product: {ms.product_name}, BizID: {ms.business_details_id}")
        print(f"    Default Child SKU ID: {ms.default_child_sku_identifier}")
        print(f"    Price: {ms.price}, Qty: {ms.quantity}, Status: {ms.status}, Published: {ms.published}")

    print(f"\nChild SKUs Created: {len(processed_data.child_skus)}")
    for i, cs in enumerate(processed_data.child_skus):
        if i < 5 or i > len(processed_data.child_skus) - 3: # Print first 5 and last 2 if many
            print(f"  ChildSKU ID: {cs.child_sku_identifier} (Source Row: {cs.original_row_index})")
            print(f"    Main SKU Link: {cs.main_sku_identifier}, Is Default in Group: {cs.is_default_in_group}")
            print(f"    Price: {cs.price}, Qty: {cs.quantity}, Status: {cs.status}, Published: {cs.published}")
        elif i == 5:
            print(f"    ... (omitting {len(processed_data.child_skus) - 7} child SKUs for brevity) ...")


    print(f"\nProduct Images Created: {len(processed_data.product_images)}")
    for i, pi in enumerate(processed_data.product_images):
        if i < 5 or i > len(processed_data.product_images) - 3:
            print(f"  Image ID: {pi.image_id} (Source SKU Row: {pi.original_row_index_of_source_sku})")
            print(f"    Main SKU Link: {pi.main_sku_identifier}, URL: {pi.url}, Is Main: {pi.is_main_image}")
        elif i == 5:
            print(f"    ... (omitting {len(processed_data.product_images) - 7} images for brevity) ...")


    print(f"\nProduct Variant Attributes Created: {len(processed_data.product_variant_attributes)}")
    for i, pva in enumerate(processed_data.product_variant_attributes):
        if i < 5 or i > len(processed_data.product_variant_attributes) - 3: # Print first 5 and last 2 if many
            print(f"  Variant Attr ID: {pva.variant_attribute_id} (Source SKU Row: {pva.original_row_index_of_source_sku})")
            print(f"    Child SKU Link: {pva.child_sku_identifier}, Attribute: {pva.attribute_name}={pva.attribute_value}")
        elif i == 5: # Add ellipsis if there are many items
            print(f"    ... (omitting {len(processed_data.product_variant_attributes) - 7} variant attributes for brevity) ...")

    print("\n--- End of Processing Results ---")

if __name__ == "__main__":
    # Define the input CSV file.
    # We can use the one generated by validator.py or the original example.
    # For a comprehensive test, using 'validation_test_product_items.csv' is good.
    # Or provide the path to the user's actual `product_items.csv`

    # First, ensure `validation_test_product_items.csv` exists or create it.
    # (This part is normally done by running validator.py's main block)
    try:
        with open('validation_test_product_items.csv', 'r') as f:
            pass
        print("Using existing 'validation_test_product_items.csv'")
        csv_to_process = 'validation_test_product_items.csv'
    except FileNotFoundError:
        print("'validation_test_product_items.csv' not found. Trying 'test_product_items.csv'.")
        # test_product_items.csv was also removed from csv_parser.py, so it likely won't be found.
        # We will rely on creating the dummy CSV here.
        try:
            with open('test_product_items.csv', 'r') as f: # This check might be redundant now
                pass
            print("Using existing 'test_product_items.csv'") # Unlikely to hit this path
            csv_to_process = 'test_product_items.csv'
        except FileNotFoundError:
            print("'test_product_items.csv' also not found. Creating a dummy CSV for main.py demonstration.")
            # Corrected number of commas for empty optional fields before 'images'
            # Added a '1' to order_limit for "Demo Product A" (row 2) for testing image parsing.
            dummy_csv_for_main = """product_name,business_details_id,main_attribute,attribute_combination,is_default_sku,price,discount_price,quantity,status,published,order_limit,package_size_length,package_size_width,package_size_height,package_weight,images
Demo Product A,101,Color,Red,True,50,45,10,ACTIVE,Published,1,,,,https://cdn.example.com/red.jpg|main_image:true
Demo Product A,101,Color,Blue,False,50,,5,ACTIVE,Published,,,,,https://cdn.example.com/blue.jpg|main_image:true
Demo Product B,102,Size,Large,True,100,90,20,INACTIVE,Unpublished,1,,,,,https://cdn.example.com/large.jpg|main_image:true|https://cdn.example.com/large_detail.jpg|main_image:false
Demo Product C (Error),103,Flavor,Vanilla,True,invalid_price,,10,ACTIVE,Published,,,,,https://cdn.example.com/vanilla.jpg|main_image:true
"""
            # order_limit, package_size_length, package_size_width, package_size_height, package_weight (5 fields)
            # So 5 commas if all are empty.
            csv_to_process = 'main_dummy_product_items.csv'
            with open(csv_to_process, 'w', newline='', encoding='utf-8') as f:
                f.write(dummy_csv_for_main)
            print(f"Created '{csv_to_process}' for demonstration.")

    # Run the pipeline
    final_data = run_sku_processing_pipeline(csv_to_process)

    # Print the results
    print_processing_results(final_data)

    print("\nMain script execution complete.")
