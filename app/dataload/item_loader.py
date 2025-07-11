import logging
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, NoResultFound, DataError
from sqlalchemy import func # Added for func.lower


# ORM Models
from app.db.models import (
    ProductOrm,
    SkuOrm,
    MainSkuOrm,
    ProductVariantOrm,
    ProductImageOrm,
    AttributeOrm,
    AttributeValueOrm
)
# Pydantic CSV Model
from app.dataload.models.item_csv import ItemCsvModel

# Parsing Utilities
from app.dataload.parsers.item_parser import (
    parse_attributes_string,
    parse_attribute_combination_string,
    generate_sku_variants,
    get_price_for_combination,
    get_quantity_for_combination,
    get_status_for_combination,
    get_order_limit_for_combination,
    get_package_size_length_for_combination,
    get_package_size_width_for_combination,
    get_package_size_height_for_combination,
    get_package_weight_for_combination,
    ItemParserError # For handling parsing errors
)
# Product Image parsing (similar to product_loader)
from app.dataload.product_loader import parse_images as parse_product_level_images


# General Utilities / Exceptions
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType # Assuming ErrorType is used in DataLoaderError
try:
    from app.utils.date_utils import now_epoch_ms
except ImportError: # Fallback if not available, for basic compilation
    import time
    def now_epoch_ms() -> int:
        return int(time.time() * 1000)

from app.utils import barcode_helper # For barcode generation

logger = logging.getLogger(__name__)

# Placeholder for functions to be implemented:
# def _get_or_create_attribute_value_ids(...) # Renamed to _lookup_...
# def load_item_record_to_db(...)
# def load_items_to_db(...)
# pass # Removed pass

def _lookup_attribute_ids(db: Session, business_details_id: int, attribute_names: List[str]) -> Dict[str, int]:
    """Helper to look up attribute IDs by name for a business."""
    attr_id_map: Dict[str, int] = {}
    missing_attrs = []
    # Ensure unique names for lookup to avoid redundant DB calls, though list(set()) already does this.
    for name in sorted(list(set(attribute_names))):
        try:
            # Use func.lower for case-insensitive comparison on name
            attr_orm_result = db.query(AttributeOrm.id).filter(
                func.lower(AttributeOrm.name) == func.lower(name), 
                AttributeOrm.business_details_id == business_details_id
            ).first() # Changed to .first()

            if attr_orm_result:
                attr_id_map[name] = attr_orm_result.id
            else: # attr_orm_result is None, meaning not found
                missing_attrs.append(name)
        except Exception as e: # Catch other potential DB errors or unexpected issues
            logger.error(f"Error looking up attribute '{name}' for business {business_details_id}: {e}", exc_info=True)
            # Ensure name is added to missing_attrs if an error occurred, to signify lookup failure.
            # Avoid adding duplicates if already added by a NoResultFound-like path (though .first() doesn't raise NoResultFound for empty results)
            if name not in missing_attrs and f"{name} (DB error)" not in missing_attrs:
                 missing_attrs.append(f"{name} (DB error)")
            
    if missing_attrs:
        # Consolidate error reporting for missing attributes or DB errors during lookup
        # The "(DB error)" suffix helps distinguish.
        # If any item contains "(DB error)", it implies a more critical issue than just "not found".
        is_critical_error = any("(DB error)" in ma for ma in missing_attrs)
        error_message_intro = "Critical error during attribute lookup or some attributes not found" if is_critical_error else "Attributes not found"
        
        raise DataLoaderError(
            message=f"{error_message_intro} for business {business_details_id}: {', '.join(missing_attrs)}",
                error_type=ErrorType.LOOKUP,
                field_name="attributes (derived names)",
                offending_value=str(missing_attrs)
            )
        raise DataLoaderError(
            message=f"Attributes not found for business {business_details_id}: {', '.join(missing_attrs)}",
            error_type=ErrorType.LOOKUP,
            field_name="attributes (derived names)",
            offending_value=str(missing_attrs)
        )
    return attr_id_map

def _lookup_attribute_value_ids(
    db: Session, 
    attr_id_map: Dict[str, int], 
    attr_value_pairs_to_lookup: List[tuple[str, str]]
) -> Dict[tuple[str, str], int]:
    """Helper to look up attribute value IDs."""
    attr_val_id_map: Dict[tuple[str, str], int] = {}
    missing_vals = []
    unique_pairs = sorted(list(set(attr_value_pairs_to_lookup)))

    for attr_name, val_name in unique_pairs:
        attr_id = attr_id_map.get(attr_name)
        if attr_id is None: # attr_name itself was not found in the previous step.
            # This indicates a problem if _lookup_attribute_ids didn't raise an error for attr_name.
            # Or, if attr_value_pairs_to_lookup contains attr_names not in attr_id_map (should not happen if logic is correct).
            logger.error(f"Internal inconsistency: Attribute ID for '{attr_name}' not present in attr_id_map during value lookup for '{val_name}'.")
            missing_vals.append(f"{attr_name} -> {val_name} (Attribute '{attr_name}' missing its ID)")
            continue 
        
        logger.debug(
            f"Looking up AttributeValue: attr_name='{attr_name}', attr_id='{attr_id}', val_name_from_csv='{val_name}'"
        )
        try:
            # Use func.lower for case-insensitive comparison on value
            attr_val_orm = db.query(AttributeValueOrm.id).filter(
                AttributeValueOrm.attribute_id == attr_id,
                func.lower(AttributeValueOrm.value) == func.lower(val_name)
            ).one()
            attr_val_id_map[(attr_name, val_name)] = attr_val_orm.id
        except NoResultFound:
            missing_vals.append(f"{attr_name} -> {val_name}")
        except Exception as e:
            logger.error(f"Error looking up attribute value '{val_name}' for attribute '{attr_name}' (ID: {attr_id}): {e}", exc_info=True)
            missing_vals.append(f"{attr_name} -> {val_name} (DB error)")


    if missing_vals:
        if any("(DB error)" in mv or "(Attribute" in mv for mv in missing_vals):
            raise DataLoaderError(
                message=f"Critical error during attribute value lookup or some values not found: {', '.join(missing_vals)}",
                error_type=ErrorType.LOOKUP,
                field_name="attribute_combination (derived values)",
                offending_value=str(missing_vals)
            )
        raise DataLoaderError(
            message=f"Attribute values not found: {', '.join(missing_vals)}",
            error_type=ErrorType.LOOKUP,
            field_name="attribute_combination (derived values)",
            offending_value=str(missing_vals)
        )
    return attr_val_id_map


def load_item_record_to_db(
    db: Session, 
    business_details_id: int, 
    item_csv_row: ItemCsvModel, 
    user_id: int,
) -> List[int]:
    """
    Processes a single item CSV row, creates SKU, MainSKU, ProductVariant records.
    Returns a list of created/updated main_sku_ids for this row.
    """
    log_prefix = f"[ItemCSV Product: {item_csv_row.product_name}]"
    logger.info(f"{log_prefix} Starting processing of item CSV row.")

    created_main_sku_ids_for_row: List[int] = [] # Renamed to avoid conflict with potential outer scope vars

    try:
        # 1. Product Lookup
        product_orm_result = db.query(ProductOrm.id).filter(
            ProductOrm.name == item_csv_row.product_name,
            ProductOrm.business_details_id == business_details_id
        ).one_or_none() # Use one_or_none for explicit handling

        if not product_orm_result:
            raise DataLoaderError(
                message=f"Product '{item_csv_row.product_name}' not found for business ID {business_details_id}.",
                error_type=ErrorType.LOOKUP,
                field_name="product_name",
                offending_value=item_csv_row.product_name
            )
        product_id: int = product_orm_result.id
        logger.debug(f"{log_prefix} Found product_id: {product_id}")

        # 2. Parse Attributes and Combinations from CSV strings
        parsed_attributes = parse_attributes_string(item_csv_row.attributes)
        logger.debug(f"{log_prefix} Parsed attributes definitions: {parsed_attributes}")
        
        parsed_attribute_values_by_type = parse_attribute_combination_string(
            item_csv_row.attribute_combination,
            parsed_attributes
        )
        logger.debug(f"{log_prefix} Parsed attribute values by type: {parsed_attribute_values_by_type}")

        all_sku_variants = generate_sku_variants(
            parsed_attribute_values_by_type,
            parsed_attributes
        )
        logger.info(f"{log_prefix} Generated {len(all_sku_variants)} SKU variants.")
        
        if not all_sku_variants: # If parsing results in no variants (e.g., empty attribute value lists)
            logger.warning(f"{log_prefix} No SKU variants were generated based on CSV input. Skipping SKU creation for this row.")
            return [] # Return empty list, no SKUs created

        # 3. Initialize for image assignment
        first_main_sku_orm_id_for_images: Optional[int] = None
        
        # 4. Pre-fetch Attribute and AttributeValue IDs
        unique_attr_names_to_lookup = sorted(list(set(attr_def['name'] for attr_def in parsed_attributes)))
        
        unique_attr_value_pairs_to_lookup: List[tuple[str,str]] = []
        if all_sku_variants: # Only try to collect if there are variants
            for variant_combination in all_sku_variants:
                for attr_detail in variant_combination: # attr_detail is {'attribute_name': ..., 'value': ...}
                    unique_attr_value_pairs_to_lookup.append( (attr_detail['attribute_name'], attr_detail['value']) )
        
        # Ensure unique_attr_value_pairs_to_lookup is not empty before calling helpers if it's possible
        # though if all_sku_variants is not empty, this should also not be empty.
        
        attr_id_map = {}
        if unique_attr_names_to_lookup:
            attr_id_map = _lookup_attribute_ids(db, business_details_id, unique_attr_names_to_lookup)
            logger.debug(f"{log_prefix} Fetched attribute IDs: {attr_id_map}")
        
        attr_val_id_map = {}
        if unique_attr_value_pairs_to_lookup and attr_id_map : # Ensure pairs and map exist
            attr_val_id_map = _lookup_attribute_value_ids(db, attr_id_map, unique_attr_value_pairs_to_lookup)
            logger.debug(f"{log_prefix} Fetched attribute value IDs: {attr_val_id_map}")

        # --- Part 2: SKU Variant Loop will start here (next plan step) ---
        logger.debug(f"{log_prefix} Starting SKU variant processing loop for {len(all_sku_variants)} variants.")
        current_time_epoch_ms = now_epoch_ms() # For created_date / updated_date

        for sku_variant_idx, current_sku_variant in enumerate(all_sku_variants):
            variant_log_prefix = f"{log_prefix} VarIdx:{sku_variant_idx} "
            # Example current_sku_variant: [{'attribute_name': 'color', 'value': 'Black', 'is_default_sku_value': True}, ...]
            
            try:
                # 1. Extract Per-Combination Data
                price = get_price_for_combination(
                    item_csv_row.price, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                quantity = get_quantity_for_combination(
                    item_csv_row.quantity, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                status_str = get_status_for_combination( # This is per main attribute value group
                    item_csv_row.status, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                active_db_val = status_str.upper() # Should be "ACTIVE" or "INACTIVE"

                order_limit = get_order_limit_for_combination(
                    item_csv_row.order_limit, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                pkg_length = get_package_size_length_for_combination(
                    item_csv_row.package_size_length, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                pkg_width = get_package_size_width_for_combination(
                    item_csv_row.package_size_width, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                pkg_height = get_package_size_height_for_combination(
                    item_csv_row.package_size_height, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                pkg_weight = get_package_weight_for_combination(
                    item_csv_row.package_weight, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                
                discount_price = None # Placeholder for now

                logger.debug(f"{variant_log_prefix}Data extracted: Price={price}, Qty={quantity}, Active={active_db_val}")

                # 2. Determine is_default_sku_for_main_skus_table
                is_default_sku = False
                main_attribute_def = next((attr for attr in parsed_attributes if attr['is_main']), None)
                # main_attribute_def should exist due to validation in parse_attributes_string
                if main_attribute_def: # Should always be true
                    main_attr_name_for_variant = main_attribute_def['name']
                    variant_main_attr_detail = next(
                        (vad for vad in current_sku_variant if vad['attribute_name'] == main_attr_name_for_variant), None
                    )
                    if variant_main_attr_detail and variant_main_attr_detail.get('is_default_sku_value', False):
                        is_default_sku = True
                logger.debug(f"{variant_log_prefix}Is Default SKU: {is_default_sku}")
                
                # TODO: Implement SKU lookup for updates. For now, focusing on creation.

                # 3. Create MainSkuOrm instance
                main_sku_orm = MainSkuOrm(
                    product_id=product_id,
                    price=price,
                    discount_price=discount_price,
                    quantity=quantity,
                    active=active_db_val,
                    is_default=is_default_sku,
                    order_limit=order_limit,
                    package_size_length=pkg_length,
                    package_size_width=pkg_width,
                    package_size_height=pkg_height,
                    package_weight=pkg_weight,
                    part_number="TEMP_MAIN_PN", 
                    mobile_barcode="TEMP_MAIN_MB",
                    barcode="TEMP_MAIN_B",      
                    created_by=user_id,
                    created_date=current_time_epoch_ms,
                    updated_by=user_id,
                    updated_date=current_time_epoch_ms,
                    business_details_id=business_details_id 
                )
                db.add(main_sku_orm)
                db.flush() 
                logger.debug(f"{variant_log_prefix}Created MainSkuOrm ID: {main_sku_orm.id}")

                main_sku_orm.part_number = str(main_sku_orm.id).zfill(9)
                main_sku_orm.mobile_barcode = f"S{main_sku_orm.id}P{product_id}"
                try:
                    barcode_bytes = barcode_helper.generate_barcode_image(main_sku_orm.mobile_barcode, 350, 100)
                    main_sku_orm.barcode = barcode_helper.encode_barcode_to_base64(barcode_bytes)
                except barcode_helper.BarcodeGenerationError as bge:
                    raise DataLoaderError(f"Barcode gen failed for MainSKU {main_sku_orm.id}: {bge}", ErrorType.PROCESSING, "main_sku.barcode", main_sku_orm.mobile_barcode)
                
                # 4. Create SkuOrm instance
                sku_orm = SkuOrm(
                    product_id=product_id,
                    main_sku_id=main_sku_orm.id, 
                    price=price, 
                    discount_price=discount_price,
                    quantity=quantity, 
                    active=active_db_val, 
                    order_limit=order_limit,
                    package_size_length=pkg_length,
                    package_size_width=pkg_width,
                    package_size_height=pkg_height,
                    package_weight=pkg_weight,
                    description=None, 
                    name=None,        
                    part_number="TEMP_SKU_PN", 
                    mobile_barcode="TEMP_SKU_MB",
                    barcode="TEMP_SKU_B",      
                    created_by=user_id,
                    created_date=current_time_epoch_ms,
                    updated_by=user_id,
                    updated_date=current_time_epoch_ms,
                    business_details_id=business_details_id 
                )
                db.add(sku_orm)
                db.flush() 
                logger.debug(f"{variant_log_prefix}Created SkuOrm ID: {sku_orm.id}")

                sku_orm.part_number = str(sku_orm.id).zfill(9)
                sku_orm.mobile_barcode = f"S{sku_orm.id}P{product_id}"
                try:
                    barcode_bytes_sku = barcode_helper.generate_barcode_image(sku_orm.mobile_barcode, 350, 100)
                    sku_orm.barcode = barcode_helper.encode_barcode_to_base64(barcode_bytes_sku)
                except barcode_helper.BarcodeGenerationError as bge:
                     raise DataLoaderError(f"Barcode gen failed for SKU {sku_orm.id}: {bge}", ErrorType.PROCESSING, "sku.barcode", sku_orm.mobile_barcode)

                if main_sku_orm.is_default and first_main_sku_orm_id_for_images is None:
                    first_main_sku_orm_id_for_images = main_sku_orm.id
                    logger.debug(f"{variant_log_prefix}Set first_main_sku_id_for_images to {main_sku_orm.id}")

                main_attribute_product_variant_id: Optional[int] = None
                for attr_detail_in_variant in current_sku_variant:
                    attr_name = attr_detail_in_variant['attribute_name']
                    attr_value_str = attr_detail_in_variant['value']
                    current_attr_id = attr_id_map[attr_name] 
                    current_attr_val_id = attr_val_id_map[(attr_name, attr_value_str)]

                    pv_orm = ProductVariantOrm(
                        attribute_id=current_attr_id,
                        attribute_value_id=current_attr_val_id,
                        sku_id=sku_orm.id,
                        main_sku_id=main_sku_orm.id,
                        active=active_db_val, 
                        created_by=user_id,
                        created_date=current_time_epoch_ms,
                        updated_by=user_id,
                        updated_date=current_time_epoch_ms,
                        business_details_id=business_details_id 
                    )
                    db.add(pv_orm)
                    db.flush() 
                    logger.debug(f"{variant_log_prefix}Created ProductVariantOrm ID: {pv_orm.id} for {attr_name}={attr_value_str}")

                    if main_attribute_def and attr_name == main_attribute_def['name']:
                        main_attribute_product_variant_id = pv_orm.id
                
                if main_attribute_product_variant_id is None and main_attribute_def: # main_attribute_def should always exist
                    raise ItemParserError(f"{variant_log_prefix}Could not determine main_attribute_product_variant_id for MainSkuOrm {main_sku_orm.id}")
                
                main_sku_orm.variant_id = main_attribute_product_variant_id
                logger.debug(f"{variant_log_prefix}Set MainSkuOrm {main_sku_orm.id} variant_id to {main_attribute_product_variant_id}")

                created_main_sku_ids_for_row.append(main_sku_orm.id)

            except ItemParserError as ipe: 
                logger.error(f"{variant_log_prefix}Parsing error for this variant: {ipe}", exc_info=True)
                raise 
            except DataLoaderError as dle: 
                logger.error(f"{variant_log_prefix}Data loading error for this variant: {dle}", exc_info=True)
                raise
            except Exception as e:
                 logger.error(f"{variant_log_prefix}Unexpected error processing this variant: {e}", exc_info=True)
                 raise DataLoaderError(message=f"Unexpected error for variant {sku_variant_idx}: {e}", error_type=ErrorType.UNEXPECTED_ROW_ERROR, offending_value=item_csv_row.product_name)


        logger.info(f"{log_prefix} SKU variant loop completed. {len(created_main_sku_ids_for_row)} MainSKUs processed for creation.")
        # --- Part 2 ends ---

        # --- Part 3: Image Processing starts here ---
        if item_csv_row.images and item_csv_row.images.strip():
            if first_main_sku_orm_id_for_images is not None:
                logger.info(f"{log_prefix} Processing images for main_sku_id {first_main_sku_orm_id_for_images}.")
                try:
                    parsed_image_data = parse_product_level_images(item_csv_row.images) 
                except Exception as img_parse_exc: # Catch potential errors from parse_product_level_images
                    logger.error(f"{log_prefix} Error parsing images string '{item_csv_row.images}': {img_parse_exc}", exc_info=True)
                    # Decide: fail the row or just skip images? For now, let it be part of overall row error.
                    raise DataLoaderError(message=f"Error parsing images string: {img_parse_exc}", error_type=ErrorType.VALIDATION, field_name="images", offending_value=item_csv_row.images) from img_parse_exc

                # TODO: Implement image upsert logic (e.g., delete existing for this main_sku_id then add new).
                # For now, focusing on creation.
                # Example delete strategy (if needed):
                # try:
                #     db.query(ProductImageOrm).filter_by(main_sku_id=first_main_sku_orm_id_for_images).delete()
                #     logger.debug(f"{log_prefix} Deleted existing images for main_sku_id {first_main_sku_orm_id_for_images} before adding new ones.")
                # except Exception as del_exc:
                #     logger.error(f"{log_prefix} Error deleting existing images for main_sku_id {first_main_sku_orm_id_for_images}: {del_exc}", exc_info=True)
                #     # Potentially raise or log and continue depending on desired atomicity.

                for img_data in parsed_image_data:
                    image_orm = ProductImageOrm(
                        name=img_data["url"],
                        product_id=product_id, 
                        main_sku_id=first_main_sku_orm_id_for_images, 
                        main_image=img_data["main_image"],
                        active="ACTIVE", 
                        created_by=user_id,
                        created_date=current_time_epoch_ms, 
                        updated_by=user_id,
                        updated_date=current_time_epoch_ms,
                        business_details_id=business_details_id 
                    )
                    db.add(image_orm)
                    logger.debug(f"{log_prefix} Added ProductImageOrm for URL: {img_data['url']}, Main: {img_data['main_image']}")
                logger.info(f"{log_prefix} Added {len(parsed_image_data)} images for main_sku_id {first_main_sku_orm_id_for_images}.")
            else:
                logger.warning(
                    f"{log_prefix} Images were provided in the CSV ('{item_csv_row.images}'), but no 'default' SKU was identified "
                    f"(first_main_sku_orm_id_for_images is None). Images for product '{item_csv_row.product_name}' will not be assigned to a specific SKU."
                )
        elif item_csv_row.images and not item_csv_row.images.strip(): # Explicitly empty string
             logger.info(f"{log_prefix} Images field provided but is empty. No images to process.")
        else: # None or not provided
            logger.info(f"{log_prefix} No images provided in CSV for this product row.")
        # --- Part 3 ends ---
        
        logger.info(f"{log_prefix} Successfully processed item CSV row. Returning {len(created_main_sku_ids_for_row)} main SKU IDs.")

    except ItemParserError as e: # Errors from initial parsing or propagated from loop
        logger.error(f"{log_prefix} Item parsing error: {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Error parsing item CSV structure for '{item_csv_row.product_name}': {e}", 
            error_type=ErrorType.VALIDATION, 
            field_name="CSV item structure rules", # General field name
            offending_value=item_csv_row.product_name # Or more specific part of CSV if known
        ) from e
    except DataLoaderError: # Re-raise if it's already a DataLoaderError (e.g. from lookups)
        # logger.error(f"{log_prefix} DataLoaderError encountered: {e}", exc_info=True) # Already logged by caller or lookup
        raise
    except Exception as e:
        logger.error(f"{log_prefix} Unexpected error during item record processing setup: {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Unexpected error processing item '{item_csv_row.product_name}': {e}", 
            error_type=ErrorType.UNEXPECTED_ROW_ERROR, 
            offending_value=item_csv_row.product_name
        ) from e
    
    return created_main_sku_ids_for_row


# Batch loader function
def load_items_to_db(
    db: Session,
    business_details_id: int,
    item_records_data: List[Dict[str, Any]], # List of raw dicts from CSV parser
    session_id: str, # Upload session ID, for context/logging
    user_id: int
) -> Dict[str, int]:
    """
    Loads a batch of item records (parsed from CSV) into the database.
    Each record corresponds to one product and its variants.
    """
    log_prefix = f"[ItemBatchLoader SID:{session_id} BID:{business_details_id}]"
    logger.info(f"{log_prefix} Starting batch load of {len(item_records_data)} item CSV rows.")

    # Summary of the batch operation
    summary = {
        "csv_rows_processed": 0, # Number of CSV rows attempted
        "csv_rows_with_errors": 0,
        "total_main_skus_created_or_updated": 0 # Sum of len(created_main_sku_ids_for_row)
    }
    # Pydantic import for validating each raw_record_dict
    from pydantic import ValidationError

    for idx, raw_record_dict in enumerate(item_records_data):
        # Using product_name for logging if available, otherwise fallback.
        product_name_for_log = raw_record_dict.get('product_name', f"CSV_Row_Idx_{idx}")
        row_log_prefix = f"{log_prefix} Row:{idx+2} Product:'{product_name_for_log}'" # CSV rows are typically 1-indexed, +1 for header
        
        summary["csv_rows_processed"] += 1
        
        try:
            # Validate the raw dict against the Pydantic model for the CSV row structure
            item_csv_model = ItemCsvModel(**raw_record_dict)
            logger.info(f"{row_log_prefix} Successfully validated CSV row structure. Proceeding to load item record.")

            # Each call to load_item_record_to_db processes one product line from CSV.
            # It's responsible for creating all its SKUs and related entities.
            # We use a nested transaction (savepoint) for each CSV row.
            # This allows us to commit changes for successful rows or rollback for failed rows
            # without affecting the entire batch transaction (which is managed by the Celery task).
            
            savepoint = db.begin_nested()
            try:
                created_main_sku_ids_for_this_row = load_item_record_to_db(
                    db=db,
                    business_details_id=business_details_id,
                    item_csv_row=item_csv_model,
                    user_id=user_id
                )
                
                savepoint.commit() # Commit changes for this successful CSV row
                summary["total_main_skus_created_or_updated"] += len(created_main_sku_ids_for_this_row)
                logger.info(f"{row_log_prefix} Successfully processed, created/updated {len(created_main_sku_ids_for_this_row)} MainSKUs.")

            except (DataLoaderError, ItemParserError, IntegrityError, DataError) as e:
                savepoint.rollback() # Rollback changes for this specific failed CSV row
                # Detailed error already logged within load_item_record_to_db or its helpers
                # Log a summary error here for the batch context.
                error_message = e.message if isinstance(e, DataLoaderError) else str(e)
                logger.error(f"{row_log_prefix} Error processing row: {error_message}", exc_info=False) # exc_info=False if already logged deeply
                summary["csv_rows_with_errors"] += 1
                # TODO: Persist detailed error for this row (e.g., to UploadSessionOrm.details or a related error table)
                # For now, the error is logged.
            except Exception as e_gen:
                savepoint.rollback()
                logger.error(f"{row_log_prefix} Unexpected critical error processing row: {e_gen}", exc_info=True)
                summary["csv_rows_with_errors"] += 1
                # TODO: Persist detailed error

        except ValidationError as pve: # Pydantic validation error for the row structure itself
            logger.error(f"{row_log_prefix} Pydantic validation error for raw data. Errors: {pve.errors()}", exc_info=False) # exc_info=False as pve.errors() is detailed
            summary["csv_rows_with_errors"] += 1
            # TODO: Persist detailed error (pve.errors())
        except Exception as e_outer: # Catch-all for unexpected errors before individual row processing could even start
            logger.error(f"{row_log_prefix} Critical error before processing row: {e_outer}", exc_info=True)
            summary["csv_rows_with_errors"] += 1
            # TODO: Persist detailed error

    logger.info(f"{log_prefix} Item batch load finished. Summary: {summary}")
    return summary
