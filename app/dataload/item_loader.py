import logging
from typing import List, Dict, Any, Optional,Tuple

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
        # This second raise was unreachable and has been removed in subsequent versions.
        # raise DataLoaderError(
        #     message=f"Attributes not found for business {business_details_id}: {', '.join(missing_attrs)}",
        #     error_type=ErrorType.LOOKUP,
        #     field_name="attributes (derived names)",
        #     offending_value=str(missing_attrs)
        # )
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
        if attr_id is None: 
            logger.error(f"Internal inconsistency: Attribute ID for '{attr_name}' not present in attr_id_map during value lookup for '{val_name}'.")
            missing_vals.append(f"{attr_name} -> {val_name} (Attribute '{attr_name}' missing its ID)")
            continue 
        
        logger.debug(
            f"Attempting to look up AttributeValue: attr_name='{attr_name}', attr_id='{attr_id}', val_name_from_csv='{val_name}'"
        )
        try:
            attr_val_orm_result = db.query(AttributeValueOrm.id, AttributeValueOrm.name).filter(
                AttributeValueOrm.attribute_id == attr_id,
                func.lower(AttributeValueOrm.name) == func.lower(val_name)
            ).first() 
            
            if attr_val_orm_result:
                attr_val_id_map[(attr_name, val_name)] = attr_val_orm_result.id
                logger.debug(
                    f"Found AttributeValue: ID='{attr_val_orm_result.id}', DBName='{attr_val_orm_result.name}' for CSV val_name='{val_name}', attr_id='{attr_id}'"
                )
            else:
                logger.warning(
                    f"AttributeValue NOT FOUND for attr_name='{attr_name}' (ID: {attr_id}), val_name_from_csv='{val_name}' using .first()"
                )
                missing_vals.append(f"{attr_name} -> {val_name}")
        except Exception as e:
            logger.error(f"Error during lookup for attribute value '{val_name}' for attribute '{attr_name}' (ID: {attr_id}): {e}", exc_info=True)
            missing_vals.append(f"{attr_name} -> {val_name} (DB error during lookup)")

    if missing_vals:
        is_critical_error = any("(DB error during lookup)" in mv or "(Attribute" in mv for mv in missing_vals) 
        error_message_intro = "Critical error during attribute value lookup or some attribute values not found" if is_critical_error else "Attribute values not found"
        
        raise DataLoaderError(
            message=f"{error_message_intro}: {', '.join(sorted(list(set(missing_vals))))}", 
            error_type=ErrorType.LOOKUP,
            field_name="attribute_combination (derived values)",
            offending_value=str(sorted(list(set(missing_vals))))
        )
    return attr_val_id_map

def find_existing_sku_by_attributes(
    db: Session, 
    product_id: int, 
    target_attribute_value_ids: List[int],
    log_prefix: str = "" 
) -> Optional[Tuple[SkuOrm, MainSkuOrm]]:
    if not target_attribute_value_ids:
        logger.debug(f"{log_prefix} No target attribute value IDs provided for SKU lookup.")
        return None

    num_target_attributes = len(target_attribute_value_ids)

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

    subquery_total_attributes_for_sku = (
        db.query(
            ProductVariantOrm.sku_id,
            sql_func.count(ProductVariantOrm.attribute_value_id).label("total_attributes_count")
        )
        .group_by(ProductVariantOrm.sku_id)
        .subquery("sq_total_attributes_for_sku")
    )

    existing_sku_orm = (
        db.query(SkuOrm)
        .join(subquery_matching_attributes, SkuOrm.id == subquery_matching_attributes.c.sku_id)
        .join(subquery_total_attributes_for_sku, SkuOrm.id == subquery_total_attributes_for_sku.c.sku_id)
        .filter(SkuOrm.product_id == product_id)
        .filter(subquery_total_attributes_for_sku.c.total_attributes_count == num_target_attributes)
        .first()
    )

    if existing_sku_orm:
        logger.debug(f"{log_prefix} Found existing SkuOrm ID: {existing_sku_orm.id} by attribute combination.")
        main_sku_orm = db.query(MainSkuOrm).filter(MainSkuOrm.id == existing_sku_orm.main_sku_id).one_or_none()
        if main_sku_orm:
            logger.debug(f"{log_prefix} Found corresponding MainSkuOrm ID: {main_sku_orm.id}.")
            return existing_sku_orm, main_sku_orm
        else:
            logger.error(f"{log_prefix} Data integrity issue: SkuOrm ID {existing_sku_orm.id} found, but its MainSkuOrm (ID: {existing_sku_orm.main_sku_id}) is missing for product {product_id}.")
            return None 
    
    logger.debug(f"{log_prefix} No existing SKU found for product_id {product_id} with attributes {target_attribute_value_ids}.")
    return None

def load_item_record_to_db(
    db: Session, 
    business_details_id: int, 
    item_csv_row: ItemCsvModel, 
    user_id: int,
) -> List[int]:
    log_prefix = f"[ItemCSV Product: {item_csv_row.product_name}]"
    logger.info(f"{log_prefix} Starting processing of item CSV row.")

    processed_main_sku_ids_for_row: List[int] = []

    try:
        product_orm_result = db.query(ProductOrm.id).filter(
            ProductOrm.name == item_csv_row.product_name,
            ProductOrm.business_details_id == business_details_id
        ).one_or_none()

        if not product_orm_result:
            raise DataLoaderError(
                message=f"Product '{item_csv_row.product_name}' not found for business ID {business_details_id}.",
                error_type=ErrorType.LOOKUP,
                field_name="product_name",
                offending_value=item_csv_row.product_name
            )
        product_id: int = product_orm_result.id
        logger.debug(f"{log_prefix} Found product_id: {product_id}")

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
        
        if not all_sku_variants:
            logger.warning(f"{log_prefix} No SKU variants were generated based on CSV input. Skipping SKU creation for this row.")
            return []

        first_main_sku_orm_id_for_images: Optional[int] = None
        
        unique_attr_names_to_lookup = sorted(list(set(attr_def['name'] for attr_def in parsed_attributes)))
        
        unique_attr_value_pairs_to_lookup: List[tuple[str,str]] = []
        if all_sku_variants:
            for variant_combination in all_sku_variants:
                for attr_detail in variant_combination:
                    unique_attr_value_pairs_to_lookup.append( (attr_detail['attribute_name'], attr_detail['value']) )
        
        attr_id_map = {}
        if unique_attr_names_to_lookup:
            attr_id_map = _lookup_attribute_ids(db, business_details_id, unique_attr_names_to_lookup)
            logger.debug(f"{log_prefix} Fetched attribute IDs: {attr_id_map}")
        
        attr_val_id_map = {}
        if unique_attr_value_pairs_to_lookup and attr_id_map :
            attr_val_id_map = _lookup_attribute_value_ids(db, attr_id_map, unique_attr_value_pairs_to_lookup)
            logger.debug(f"{log_prefix} Fetched attribute value IDs: {attr_val_id_map}")

        logger.debug(f"{log_prefix} Starting SKU variant processing loop for {len(all_sku_variants)} variants.")
        current_time_epoch_ms = now_epoch_ms()

        for sku_variant_idx, current_sku_variant in enumerate(all_sku_variants):
            variant_log_prefix = f"{log_prefix} VarIdx:{sku_variant_idx} "
            
            try:
                price = get_price_for_combination(
                    item_csv_row.price, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                quantity = get_quantity_for_combination(
                    item_csv_row.quantity, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                status_str = get_status_for_combination(
                    item_csv_row.status, parsed_attributes, parsed_attribute_values_by_type, current_sku_variant
                )
                active_db_val = status_str.upper()

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
                
                discount_price = None

                logger.debug(f"{variant_log_prefix}Data extracted: Price={price}, Qty={quantity}, Active={active_db_val}")

                is_default_sku = False
                main_attribute_def = next((attr for attr in parsed_attributes if attr['is_main']), None)
                if main_attribute_def:
                    main_attr_name_for_variant = main_attribute_def['name']
                    variant_main_attr_detail = next(
                        (vad for vad in current_sku_variant if vad['attribute_name'] == main_attr_name_for_variant), None
                    )
                    if variant_main_attr_detail and variant_main_attr_detail.get('is_default_sku_value', False):
                        is_default_sku = True
                logger.debug(f"{variant_log_prefix}Is Default SKU: {is_default_sku}")
                
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

                    main_sku_orm_instance.price = price
                    main_sku_orm_instance.discount_price = discount_price
                    main_sku_orm_instance.quantity = quantity
                    main_sku_orm_instance.active = active_db_val
                    main_sku_orm_instance.order_limit = order_limit
                    main_sku_orm_instance.package_size_length = pkg_length
                    main_sku_orm_instance.package_size_width = pkg_width
                    main_sku_orm_instance.package_size_height = pkg_height
                    main_sku_orm_instance.package_weight = pkg_weight
                    main_sku_orm_instance.updated_by = user_id
                    main_sku_orm_instance.updated_date = current_time_epoch_ms
                    
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
                    
                    logger.debug(f"{variant_log_prefix}Updated MainSkuOrm ID: {main_sku_orm_instance.id} and SkuOrm ID: {sku_orm_instance.id}")
                    
                    if main_sku_orm_instance.is_default and first_main_sku_orm_id_for_images is None:
                         first_main_sku_orm_id_for_images = main_sku_orm_instance.id

                    processed_main_sku_ids_for_row.append(main_sku_orm_instance.id)
                else: 
                    logger.warning(f"{variant_log_prefix}SKU with attributes {current_target_attr_value_ids} (Product ID: {product_id}) not found. Skipping creation.")
                    continue
            except ItemParserError as ipe:
                logger.error(f"{variant_log_prefix}Parsing error for this variant: {ipe}", exc_info=True)
                raise 
            except DataLoaderError as dle: 
                logger.error(f"{variant_log_prefix}Data loading error for this variant: {dle}", exc_info=True)
                raise
            except Exception as e:
                 logger.error(f"{variant_log_prefix}Unexpected error processing this variant: {e}", exc_info=True)
                 raise DataLoaderError(message=f"Unexpected error for variant {sku_variant_idx}: {e}", error_type=ErrorType.UNEXPECTED_ROW_ERROR, offending_value=item_csv_row.product_name)

        logger.info(f"{log_prefix} SKU variant loop completed. {len(processed_main_sku_ids_for_row)} MainSKUs processed/skipped for this row.")

        if item_csv_row.images and item_csv_row.images.strip():
            if first_main_sku_orm_id_for_images is not None:
                logger.info(f"{log_prefix} Processing images for main_sku_id {first_main_sku_orm_id_for_images}.")
                try:
                    parsed_image_data = parse_product_level_images(item_csv_row.images) 
                except Exception as img_parse_exc:
                    logger.error(f"{log_prefix} Error parsing images string '{item_csv_row.images}': {img_parse_exc}", exc_info=True)
                    raise DataLoaderError(message=f"Error parsing images string: {img_parse_exc}", error_type=ErrorType.VALIDATION, field_name="images", offending_value=item_csv_row.images) from img_parse_exc

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
                    )
                    db.add(image_orm)
                    logger.debug(f"{log_prefix} Added ProductImageOrm for URL: {img_data['url']}, Main: {img_data['main_image']}")
                logger.info(f"{log_prefix} Added {len(parsed_image_data)} images for main_sku_id {first_main_sku_orm_id_for_images}.")
            else:
                logger.warning(
                    f"{log_prefix} Images were provided in the CSV ('{item_csv_row.images}'), but no 'default' SKU was identified "
                    f"(first_main_sku_orm_id_for_images is None). Images for product '{item_csv_row.product_name}' will not be assigned to a specific SKU."
                )
        elif item_csv_row.images and not item_csv_row.images.strip():
             logger.info(f"{log_prefix} Images field provided but is empty. No images to process.")
        else: 
            logger.info(f"{log_prefix} No images provided in CSV for this product row.")
        
        logger.info(f"{log_prefix} Successfully processed item CSV row. Returning {len(processed_main_sku_ids_for_row)} main SKU IDs.")

    except ItemParserError as e: 
        logger.error(f"{log_prefix} Item parsing error: {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Error parsing item CSV structure for '{item_csv_row.product_name}': {e}", 
            error_type=ErrorType.VALIDATION, 
            field_name="CSV item structure rules", 
            offending_value=item_csv_row.product_name 
        ) from e
    except DataLoaderError: 
        raise
    except Exception as e:
        logger.error(f"{log_prefix} Unexpected error during item record processing setup: {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Unexpected error processing item '{item_csv_row.product_name}': {e}", 
            error_type=ErrorType.UNEXPECTED_ROW_ERROR, 
            offending_value=item_csv_row.product_name
        ) from e
    
    return processed_main_sku_ids_for_row

# Batch loader function
def load_items_to_db(
    db: Session,
    business_details_id: int,
    item_records_data: List[Dict[str, Any]], 
    session_id: str, 
    user_id: int
) -> Dict[str, int]:
    log_prefix = f"[ItemBatchLoader SID:{session_id} BID:{business_details_id}]"
    logger.info(f"{log_prefix} Starting batch load of {len(item_records_data)} item CSV rows.")

    summary = {
        "csv_rows_processed": 0,
        "csv_rows_with_errors": 0,
        "total_main_skus_created_or_updated": 0
    }
    from pydantic import ValidationError

    for idx, raw_record_dict in enumerate(item_records_data):
        product_name_for_log = raw_record_dict.get('product_name', f"CSV_Row_Idx_{idx}")
        row_log_prefix = f"{log_prefix} Row:{idx+2} Product:'{product_name_for_log}'"
        
        summary["csv_rows_processed"] += 1
        
        try:
            item_csv_model = ItemCsvModel(**raw_record_dict)
            logger.info(f"{row_log_prefix} Successfully validated CSV row structure. Proceeding to load item record.")
            
            savepoint = db.begin_nested()
            try:
                created_main_sku_ids_for_this_row = load_item_record_to_db(
                    db=db,
                    business_details_id=business_details_id,
                    item_csv_row=item_csv_model,
                    user_id=user_id
                )
                
                savepoint.commit()
                summary["total_main_skus_created_or_updated"] += len(created_main_sku_ids_for_this_row)
                logger.info(f"{row_log_prefix} Successfully processed, created/updated {len(created_main_sku_ids_for_this_row)} MainSKUs.")

            except (DataLoaderError, ItemParserError, IntegrityError, DataError) as e:
                savepoint.rollback() 
                error_message = e.message if isinstance(e, DataLoaderError) else str(e)
                logger.error(f"{row_log_prefix} Error processing row: {error_message}", exc_info=False) 
                summary["csv_rows_with_errors"] += 1
            except Exception as e_gen:
                savepoint.rollback()
                logger.error(f"{row_log_prefix} Unexpected critical error processing row: {e_gen}", exc_info=True)
                summary["csv_rows_with_errors"] += 1

        except ValidationError as pve: 
            logger.error(f"{row_log_prefix} Pydantic validation error for raw data. Errors: {pve.errors()}", exc_info=False) 
            summary["csv_rows_with_errors"] += 1
        except Exception as e_outer: 
            logger.error(f"{row_log_prefix} Critical error before processing row: {e_outer}", exc_info=True)
            summary["csv_rows_with_errors"] += 1

    logger.info(f"{log_prefix} Item batch load finished. Summary: {summary}")
    return summary
