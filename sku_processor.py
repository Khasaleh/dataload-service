from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from data_models import (
    SKU,
    MainSKUOutput,
    ChildSKUOutput,
    ProductImageOutput,
    ProductVariantAttributeOutput,
    ParsedAttribute
)
import hashlib # For generating somewhat unique IDs if needed
from dataclasses import dataclass, field # Import dataclass and field

def generate_main_sku_id(product_name: str, business_details_id: str) -> str:
    """Generates a consistent identifier for a main SKU."""
    return f"{product_name}|{business_details_id}"

def generate_child_sku_id(main_sku_id: str, attributes: List[ParsedAttribute], original_row_index: int) -> str:
    """Generates a somewhat unique identifier for a child SKU."""
    # Using original_row_index makes it unique if attributes are identical for some reason in bad data
    # A more robust way might involve hashing the attribute values.
    attr_str = "_".join(sorted([f"{pa.attribute_name}-{pa.attribute_value}" for pa in attributes]))
    # To keep it shorter and somewhat stable, hash it.
    # Or simply use original_row_index if that's guaranteed unique for input.
    # For now, let's use a hash of main_sku_id and attributes for content-based ID,
    # and fallback or append original_row_index if needed for absolute uniqueness for this run.
    # The CSV row index is a good candidate for a temporary unique ID for this specific input file.
    return f"child_{original_row_index}" # Simplest for now, guarantees uniqueness for this batch

def generate_image_id(main_sku_id: str, url: str) -> str:
    """Generates an identifier for an image."""
    # Hash of URL to keep it somewhat consistent if the same image appears.
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"img_{main_sku_id}_{url_hash}"

def generate_variant_attribute_id(child_sku_id: str, attribute_name: str, attribute_value: str) -> str:
    """Generates an identifier for a product variant attribute entry."""
    return f"attr_{child_sku_id}_{attribute_name}_{hashlib.md5(attribute_value.encode()).hexdigest()[:4]}"


@dataclass
class ProcessedSKUData:
    main_skus: List[MainSKUOutput] = field(default_factory=list)
    child_skus: List[ChildSKUOutput] = field(default_factory=list)
    product_images: List[ProductImageOutput] = field(default_factory=list)
    product_variant_attributes: List[ProductVariantAttributeOutput] = field(default_factory=list)
    processing_errors: List[str] = field(default_factory=list)


def process_skus(validated_skus: List[SKU]) -> ProcessedSKUData:
    """
    Processes a list of validated SKUs to generate MainSKU, ChildSKU,
    ProductImage, and ProductVariantAttribute records.
    """
    output = ProcessedSKUData()

    # Group SKUs by (product_name, business_details_id)
    # Using defaultdict to simplify appending to lists
    grouped_skus: Dict[Tuple[str, str], List[SKU]] = defaultdict(list)
    for sku in validated_skus:
        if sku.errors: # Skip SKUs with parsing/validation errors from being processed further
            output.processing_errors.append(
                f"Row {sku.original_row_index}: Skipped due to existing errors: {'; '.join(sku.errors)}"
            )
            continue
        # Only process SKUs that don't have errors.
        # product_name and business_details_id should be present if no errors.
        grouped_skus[(sku.product_name, sku.business_details_id)].append(sku)

    for (product_name, business_details_id), skus_in_group in grouped_skus.items():
        main_sku_id = generate_main_sku_id(product_name, business_details_id)

        # Find the default SKU for this group
        default_skus_in_group = [s for s in skus_in_group if s.is_default_sku]

        default_sku_source: Optional[SKU] = None

        if not default_skus_in_group:
            output.processing_errors.append(
                f"Product Group '{main_sku_id}': No default SKU found. Cannot create MainSKU or link images."
            )
            # Optionally, could still create ChildSKUOutput entries if that's desired,
            # but they wouldn't correctly link to a MainSKU or its images.
            # For now, if no default, this group might be problematic.
            # However, validator should have caught this. This is a safeguard.
            # Let's assume if we reach here, validator passed, meaning there IS one default.
            # This case should ideally not be hit if validator works perfectly.
            # If it can be hit (e.g. validator only flags but doesn't stop), we must handle it.
            # For now, we'll proceed to create child SKUs, but MainSKU and its images might be missing/incomplete.
            # A better strategy: if validator flags an error, those SKUs should not be processed here.
            # The initial loop `if sku.errors: continue` handles this. So, this block is less likely.
            pass # No default SKU, so MainSKUOutput might be incomplete or not created.

        elif len(default_skus_in_group) > 1:
            output.processing_errors.append(
                f"Product Group '{main_sku_id}': Multiple default SKUs found (rows: {[s.original_row_index for s in default_skus_in_group]}). Validator should have caught this. Using the first one found."
            )
            default_sku_source = default_skus_in_group[0] # Pick the first one
        else:
            default_sku_source = default_skus_in_group[0]

        # Create MainSKUOutput if a default SKU source is identified
        if default_sku_source:
            # The child SKU that is the default needs its own ID
            default_child_sku_id = generate_child_sku_id(
                main_sku_id, default_sku_source.attributes, default_sku_source.original_row_index or 0
            )

            main_sku_obj = MainSKUOutput(
                main_sku_identifier=main_sku_id,
                product_name=default_sku_source.product_name,
                business_details_id=default_sku_source.business_details_id,
                price=default_sku_source.price, # As per interpretation
                discount_price=default_sku_source.discount_price,
                quantity=default_sku_source.quantity, # Assumption: main SKU quantity from default variant
                status=default_sku_source.status,
                published=default_sku_source.published,
                default_child_sku_identifier=default_child_sku_id,
                original_row_index_of_default_source=default_sku_source.original_row_index
            )
            output.main_skus.append(main_sku_obj)

            # Create ProductImageOutput entries from the default SKU's images
            for img_data in default_sku_source.images:
                image_id = generate_image_id(main_sku_id, img_data.url)
                image_output = ProductImageOutput(
                    image_id=image_id,
                    main_sku_identifier=main_sku_id,
                    url=img_data.url,
                    is_main_image=img_data.is_main,
                    original_row_index_of_source_sku=default_sku_source.original_row_index
                )
                output.product_images.append(image_output)
        else:
            # If no default_sku_source, MainSKU and its images cannot be created.
            # ChildSKUs processed below will lack a valid main_sku_identifier if we used a temp one.
            # This situation implies data errors that should have been caught by the validator.
            # If we are here, it means a product group has SKUs but none are default (error).
            output.processing_errors.append(f"Product Group '{main_sku_id}': Could not determine a definitive default SKU. Main SKU and its images will not be generated for this group.")


        # Create ChildSKUOutput and ProductVariantAttributeOutput for ALL SKUs in the group
        for sku_in_group in skus_in_group:
            child_sku_id = generate_child_sku_id(
                main_sku_id, sku_in_group.attributes, sku_in_group.original_row_index or 0
            )

            child_sku_obj = ChildSKUOutput(
                child_sku_identifier=child_sku_id,
                main_sku_identifier=main_sku_id, # All child SKUs link to the same main_sku_id for the group
                product_name=sku_in_group.product_name,
                business_details_id=sku_in_group.business_details_id,
                price=sku_in_group.price,
                discount_price=sku_in_group.discount_price,
                quantity=sku_in_group.quantity,
                status=sku_in_group.status,
                published=sku_in_group.published,
                order_limit=sku_in_group.order_limit,
                package_size_length=sku_in_group.package_size_length,
                package_size_width=sku_in_group.package_size_width,
                package_size_height=sku_in_group.package_size_height,
                package_weight=sku_in_group.package_weight,
                is_default_in_group=sku_in_group.is_default_sku,
                original_row_index=sku_in_group.original_row_index
            )
            output.child_skus.append(child_sku_obj)

            # Create ProductVariantAttributeOutput entries for this child SKU
            for pa in sku_in_group.attributes:
                variant_attr_id = generate_variant_attribute_id(child_sku_id, pa.attribute_name, pa.attribute_value)
                variant_attr_obj = ProductVariantAttributeOutput(
                    variant_attribute_id=variant_attr_id,
                    child_sku_identifier=child_sku_id,
                    main_sku_identifier=main_sku_id,
                    attribute_name=pa.attribute_name,
                    attribute_value=pa.attribute_value,
                    original_row_index_of_source_sku=sku_in_group.original_row_index
                )
                output.product_variant_attributes.append(variant_attr_obj)

    return output


if __name__ == '__main__':
    from csv_parser import load_skus_from_csv
    from validator import validate_sku_list
    from dataclasses import field # Required for ProcessedSKUData

    # Use the validation_test_product_items.csv or a more comprehensive one
    test_file = 'validation_test_product_items.csv'
    # Ensure this file exists and has valid + invalid cases.
    # The validator should have added errors to SKUs that fail validation.
    # The processor should skip SKUs with errors.

    print(f"\n--- Processing SKUs from {test_file} ---")
    # 1. Load SKUs
    raw_skus = load_skus_from_csv(test_file)
    print(f"Loaded {len(raw_skus)} raw SKUs from CSV.")

    # 2. Validate SKUs
    # The validator adds errors to sku.errors list but doesn't remove skus.
    validated_skus = validate_sku_list(raw_skus)
    print("Finished SKU validation.")

    # Count SKUs with errors after validation to see what processor will skip
    skus_with_errors_count = sum(1 for sku in validated_skus if sku.errors)
    print(f"{skus_with_errors_count} SKUs have errors after validation and will be skipped by the processor.")
    for sku in validated_skus:
        if sku.errors:
            print(f"  Row {sku.original_row_index} errors: {sku.errors}")


    # 3. Process Validated SKUs
    processed_data = process_skus(validated_skus)
    print("Finished SKU processing.")

    print(f"\n--- Processing Results ---")
    print(f"Main SKUs Created: {len(processed_data.main_skus)}")
    for ms in processed_data.main_skus:
        print(f"  MainSKU: {ms.main_sku_identifier}, Default Child: {ms.default_child_sku_identifier}, Source Row: {ms.original_row_index_of_default_source}")

    print(f"\nChild SKUs Created: {len(processed_data.child_skus)}")
    for cs in processed_data.child_skus:
        print(f"  ChildSKU: {cs.child_sku_identifier}, MainLink: {cs.main_sku_identifier}, DefaultInGroup: {cs.is_default_in_group}, Source Row: {cs.original_row_index}")

    print(f"\nProduct Images Created: {len(processed_data.product_images)}")
    for pi in processed_data.product_images:
        print(f"  Image: {pi.image_id}, MainSKULink: {pi.main_sku_identifier}, URL: {pi.url}, IsMain: {pi.is_main_image}, Source Row: {pi.original_row_index_of_source_sku}")

    print(f"\nProduct Variant Attributes Created: {len(processed_data.product_variant_attributes)}")
    for pva in processed_data.product_variant_attributes:
        print(f"  VariantAttr: {pva.variant_attribute_id}, ChildSKULink: {pva.child_sku_identifier}, Attr: {pva.attribute_name}={pva.attribute_value}, Source Row: {pva.original_row_index_of_source_sku}")

    if processed_data.processing_errors:
        print("\nProcessing Errors Encountered:")
        for err in processed_data.processing_errors:
            print(f"  - {err}")

    print("\nProcessing step demonstration complete.")
