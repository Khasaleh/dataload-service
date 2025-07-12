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
    AttributeValueOrm,
    MainSkuOrm # Ensure MainSkuOrm is imported if not already
)
# Pydantic CSV Model
from app.dataload.models.item_csv import ItemCsvModel

# Parsing Utilities
# Need to import sql_func for sqlalchemy.sql.func
from sqlalchemy import and_, func as sql_func

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
            # Compare CSV value (val_name) against AttributeValueOrm.name column, case-insensitively
            attr_val_orm_result = db.query(AttributeValueOrm.id).filter(
                AttributeValueOrm.attribute_id == attr_id,
                func.lower(AttributeValueOrm.name) == func.lower(val_name)
            ).one() # Expect exactly one match
            
            attr_val_id_map[(attr_name, val_name)] = attr_val_orm_result.id
        except NoResultFound:
            missing_vals.append(f"{attr_name} -> {val_name}")
        except MultipleResultsFound:
            logger.error(
                f"Multiple attribute values found for attr_id='{attr_id}', "
                f"val_name='{val_name}' (case-insensitive). This indicates a data integrity issue "
                f"where (attribute_id, LOWER(name)) is not unique in attribute_value table."
            )
            missing_vals.append(f"{attr_name} -> {val_name} (Multiple results found in DB)")
        except Exception as e:
            logger.error(f"Error looking up attribute value '{val_name}' for attribute '{attr_name}' (ID: {attr_id}): {e}", exc_info=True)
            missing_vals.append(f"{attr_name} -> {val_name} (DB error)")


    if missing_vals:
        # Check if any missing_val contains specific error indicators to modify message
        is_critical_error = any("(DB error)" in mv or "(Attribute" in mv or "(Multiple results found in DB)" in mv for mv in missing_vals)
        error_message_intro = "Critical error during attribute value lookup or some values not found/unique" if is_critical_error else "Attribute values not found"
        
        raise DataLoaderError(
            message=f"{error_message_intro}: {', '.join(missing_vals)}",
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


def find_existing_sku_by_attributes(
    db: Session,
    product_id: int,
    target_attribute_value_ids: List[int],
    log_prefix: str = ""
) -> Optional[Tuple[SkuOrm, MainSkuOrm]]:
    """
    Finds an existing SkuOrm and its corresponding MainSkuOrm based on product_id
    and a specific combination of attribute_value_ids.
    A SKU is considered a match if it's linked to all target_attribute_value_ids
    and no other attribute_value_ids.
    """
    if not target_attribute_value_ids:
        logger.debug(f"{log_prefix} No target attribute value IDs provided for SKU lookup.")
        return None

    num_target_attributes = len(target_attribute_value_ids)

    # Subquery to find sku_ids that are linked to *all* target_attribute_value_ids
    # It counts, for each sku_id, how many of the target_attribute_value_ids it is linked to.
    # We are interested in sku_ids where this count equals num_target_attributes.
    subquery_matching_attributes = (
        db.query(
            ProductVariantOrm.sku_id,
            sql_func.count(ProductVariantOrm.attribute_value_id).label("matching_attributes_count")
        )
        .filter(ProductVariantOrm.attribute_value_id.in_(target_attribute_value_ids))
        .group_by(ProductVariantOrm.sku_id)
        .having(sql_func.count(ProductVariantOrm.attribute_value_id) == num_target_attributes)
        .subquery("sq_matching_skus")
    )

    # Subquery to count the *total* number of attributes linked to each sku_id.
    # This is to ensure the SKU doesn't have additional attributes beyond the target ones.
    subquery_total_attributes_for_sku = (
        db.query(
            ProductVariantOrm.sku_id,
            sql_func.count(ProductVariantOrm.attribute_value_id).label("total_attributes_count")
        )
        .group_by(ProductVariantOrm.sku_id)
        .subquery("sq_total_attributes_for_sku")
    )

    # Main query to find the SkuOrm
    # It must:
    # 1. Belong to the given product_id.
    # 2. Be present in subquery_matching_attributes (has all target attributes).
    # 3. Its total number of attributes (from subquery_total_attributes_for_sku) must also be num_target_attributes.
    existing_sku_orm = (
        db.query(SkuOrm)
        .join(subquery_matching_attributes, SkuOrm.id == subquery_matching_attributes.c.sku_id)
        .join(subquery_total_attributes_for_sku, SkuOrm.id == subquery_total_attributes_for_sku.c.sku_id)
        .filter(SkuOrm.product_id == product_id)
        .filter(subquery_total_attributes_for_sku.c.total_attributes_count == num_target_attributes)
        .first() # Expect at most one such SKU
    )

    if existing_sku_orm:
        logger.debug(f"{log_prefix} Found existing SkuOrm ID: {existing_sku_orm.id} by attribute combination.")
        # Fetch the corresponding MainSkuOrm
        # Assuming existing_sku_orm.main_sku_id correctly links SkuOrm to MainSkuOrm
        main_sku_orm = db.query(MainSkuOrm).filter(MainSkuOrm.id == existing_sku_orm.main_sku_id).one_or_none()
        if main_sku_orm:
            logger.debug(f"{log_prefix} Found corresponding MainSkuOrm ID: {main_sku_orm.id}.")
            return existing_sku_orm, main_sku_orm
        else:
            logger.error(f"{log_prefix} Data integrity issue: SkuOrm ID {existing_sku_orm.id} found, but its MainSkuOrm (ID: {existing_sku_orm.main_sku_id}) is missing for product {product_id}.")
            return None # Or raise an error indicating data inconsistency

    logger.debug(f"{log_prefix} No existing SKU found for product_id {product_id} with attributes {target_attribute_value_ids}.")
    return None


def load_item_record_to_db(
    db: Session, 
    business_details_id: int, 
    item_csv_row: ItemCsvModel, 
    user_id: int,
) -> List[int]:
    """
    Processes a single item CSV row. If SKUs exist, updates them. If not, skips creation.
    Returns a list of processed (created/updated) main_sku_ids for this row.
    """
    log_prefix = f"[ItemCSV Product: {item_csv_row.product_name}]"
    logger.info(f"{log_prefix} Starting processing of item CSV row.")

    processed_main_sku_ids_for_row: List[int] = []

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
                
                # --- SKU Lookup and Update/Skip Logic ---
                current_target_attr_value_ids = sorted([
                    attr_val_id_map[(attr_detail['attribute_name'], attr_detail['value'])]
                    for attr_detail in current_sku_variant
                ])

                existing_sku_tuple = find_existing_sku_by_attributes(
                    db, product_id, current_target_attr_value_ids, variant_log_prefix
                )

                main_sku_orm_instance: Optional[MainSkuOrm] = None
                sku_orm_instance: Optional[SkuOrm] = None

                if existing_sku_tuple:
                    sku_orm_instance, main_sku_orm_instance = existing_sku_tuple
                    logger.info(f"{variant_log_prefix}Found existing MainSKU ID: {main_sku_orm_instance.id}, SKU ID: {sku_orm_instance.id}. Updating.")

                    # Update existing MainSkuOrm
                    main_sku_orm_instance.price = price
                    main_sku_orm_instance.discount_price = discount_price
                    main_sku_orm_instance.quantity = quantity
                    main_sku_orm_instance.active = active_db_val
                    # main_sku_orm_instance.is_default = is_default_sku # is_default is structural, generally not updated unless explicitly required.
                                                                    # If it needs to change, it implies a change in which variant is "main",
                                                                    # which might have other side effects (e.g. image assignment).
                                                                    # For now, assume is_default is not changed on simple data updates.
                    main_sku_orm_instance.order_limit = order_limit
                    main_sku_orm_instance.package_size_length = pkg_length
                    main_sku_orm_instance.package_size_width = pkg_width
                    main_sku_orm_instance.package_size_height = pkg_height
                    main_sku_orm_instance.package_weight = pkg_weight
                    main_sku_orm_instance.updated_by = user_id
                    main_sku_orm_instance.updated_date = current_time_epoch_ms

                    # Barcodes and part numbers are usually stable after creation.
                    # Regenerate if business logic dictates they can change on update.
                    # For now, assuming they are stable.

                    # Update existing SkuOrm
                    sku_orm_instance.price = price
                    sku_orm_instance.discount_price = discount_price
                    sku_orm_instance.quantity = quantity
                    sku_orm_instance.active = active_db_val
                    sku_orm_instance.order_limit = order_limit
                    sku_orm_instance.package_size_length = pkg_length
                    sku_orm_instance.package_size_width = pkg_width
                    sku_orm_instance.package_size_height = pkg_height
                    sku_orm_instance.package_weight = pkg_weight
                    sku_orm_instance.updated_by = user_id
                    sku_orm_instance.updated_date = current_time_epoch_ms

                    # ProductVariantOrm entries define the SKU structure.
                    # Their attribute_id/attribute_value_id links are not "updated".
                    # If these change, it implies a different SKU.
                    # We might update their 'active' status if it can vary independently of SkuOrm/MainSkuOrm.
                    # For now, assuming ProductVariantOrm `active` status is tied to SkuOrm/MainSkuOrm `active` status.
                    # If ProductVariantOrm records need individual status updates, that logic would go here.
                    # Example:
                    # existing_pvs = db.query(ProductVariantOrm).filter(ProductVariantOrm.sku_id == sku_orm_instance.id).all()
                    # for pv in existing_pvs:
                    #    pv.active = active_db_val # Or some other logic
                    #    pv.updated_by = user_id
                    #    pv.updated_date = current_time_epoch_ms

                    logger.debug(f"{variant_log_prefix}Updated MainSkuOrm ID: {main_sku_orm_instance.id} and SkuOrm ID: {sku_orm_instance.id}")

                    if main_sku_orm_instance.is_default and first_main_sku_orm_id_for_images is None:
                         first_main_sku_orm_id_for_images = main_sku_orm_instance.id

                    processed_main_sku_ids_for_row.append(main_sku_orm_instance.id)

                else: # SKU not found, skip creation as per requirement
                    logger.warning(f"{variant_log_prefix}SKU with attributes {current_target_attr_value_ids} (Product ID: {product_id}) not found. Skipping creation.")
                    continue # Move to the next variant in the CSV row
                
                # End of Update/Skip logic. If we are here, we either updated an existing SKU or skipped.
                # The original creation logic is now bypassed if an SKU is not found.

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
                        updated_date=current_time_epoch_ms
                        # business_details_id=business_details_id # Removed, not in ProductImageOrm DDL
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
