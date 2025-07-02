import logging
import sys
from pathlib import Path
from csv_parser import load_skus_from_csv
from validator import validate_sku_list
from sku_processor import process_skus, ProcessedSKUData
from data_models import SKU  # For type hinting

# ----------------------
# Logging Setup
# ----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------
# Main Processing Logic
# ----------------------
def run_sku_processing_pipeline(csv_file_path: str) -> ProcessedSKUData:
    logger.info(f"Starting SKU processing for file: {csv_file_path}")

    raw_skus = load_skus_from_csv(csv_file_path)
    if not raw_skus:
        logger.error("No SKUs loaded. Exiting.")
        return ProcessedSKUData(processing_errors=["Failed to load any SKUs from the CSV."])

    logger.info(f"Loaded {len(raw_skus)} raw SKUs from CSV.")
    _log_errors("Initial Parsing Errors", raw_skus)

    validated_skus = validate_sku_list(raw_skus)
    logger.info("Finished SKU validation.")
    _log_errors("Validation Errors", validated_skus)

    processed_data = process_skus(validated_skus)
    logger.info("Finished SKU processing.")

    return processed_data


def _log_errors(title: str, sku_list):
    logger.info(f"--- {title} ---")
    found_errors = False
    for sku in sku_list:
        if sku.product_name and sku.errors:
            logger.warning(f"Row ~{sku.original_row_index} (Product: {sku.product_name}): Errors: {sku.errors}")
            found_errors = True
    if not found_errors:
        logger.info("No errors found.")
    logger.info("--------------------------")


def print_processing_results(processed_data: ProcessedSKUData):
    print("\n--- Processing Results ---\n")

    if processed_data.processing_errors:
        print("\nOverall Processing Errors:")
        for err in processed_data.processing_errors:
            print(f"  - {err}")
    else:
        print("\nNo overall processing errors reported.")

    print(f"\nMain SKUs Created: {len(processed_data.main_skus)}")
    for ms in processed_data.main_skus:
        print(f"  MainSKU ID: {ms.main_sku_identifier} (Row: {ms.original_row_index_of_default_source})")

    print(f"\nChild SKUs Created: {len(processed_data.child_skus)}")
    print(f"\nProduct Images Created: {len(processed_data.product_images)}")
    print(f"\nProduct Variant Attributes Created: {len(processed_data.product_variant_attributes)}")
    print("\n--- End of Results ---\n")


# ----------------------
# CLI Entrypoint
# ----------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process SKU CSV File.")
    parser.add_argument(
        "csv",
        nargs="?",
        default=None,
        help="Path to the CSV file to process."
    )
    args = parser.parse_args()

    if args.csv and Path(args.csv).exists():
        csv_to_process = args.csv
        logger.info(f"Using provided CSV: {csv_to_process}")
    else:
        default_csv = Path("validation_test_product_items.csv")
        if default_csv.exists():
            csv_to_process = str(default_csv)
            logger.info("Using existing 'validation_test_product_items.csv'")
        else:
            csv_to_process = "main_dummy_product_items.csv"
            dummy_csv = """product_name,business_details_id,main_attribute,attribute_combination,is_default_sku,price,discount_price,quantity,status,published,order_limit,package_size_length,package_size_width,package_size_height,package_weight,images
Demo Product A,101,Color,Red,True,50,45,10,ACTIVE,Published,1,,,,https://cdn.example.com/red.jpg|main_image:true
Demo Product A,101,Color,Blue,False,50,,5,ACTIVE,Published,,,,,https://cdn.example.com/blue.jpg|main_image:true
Demo Product B,102,Size,Large,True,100,90,20,INACTIVE,Unpublished,1,,,,,https://cdn.example.com/large.jpg|main_image:true|https://cdn.example.com/large_detail.jpg|main_image:false
Demo Product C (Error),103,Flavor,Vanilla,True,invalid_price,,10,ACTIVE,Published,,,,,https://cdn.example.com/vanilla.jpg|main_image:true
"""
            with open(csv_to_process, "w", encoding="utf-8") as f:
                f.write(dummy_csv)
            logger.info(f"Created '{csv_to_process}' with sample data.")

    final_data = run_sku_processing_pipeline(csv_to_process)
    print_processing_results(final_data)
