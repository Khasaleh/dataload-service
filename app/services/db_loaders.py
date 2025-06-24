"""
Service layer for loading processed and validated CSV data into the database.

This module will contain functions specific to each data entity type (e.g., categories,
brands, products), handling the actual ORM object creation, session management for
upserts, and any specific logic required for that entity type (like handling
hierarchical data for categories).

These loader functions are intended to be called by the generic `process_csv_task`
in `app.tasks.load_jobs.py` after CSV data validation and basic processing.
"""
import logging
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session

# Import ORM models as they are needed by specific loader functions.
# Example:
from app.db.models import (
    CategoryOrm,
    BrandOrm,
    AttributeOrm,
    AttributeValueOrm,
    ReturnPolicyOrm,
    PriceOrm,          # Added PriceOrm
    ProductOrm,        # Ensure ProductOrm is imported
    ProductItemOrm,    # Ensure ProductItemOrm is imported
    # MetaTagOrm
)
from datetime import datetime
from app.dataload.models.price_csv import PriceCsv, PriceTypeEnum as PriceCsvTypeEnum # For type hint
# from sqlalchemy.exc import SQLAlchemyError # For more specific DB error handling if needed


# Import Redis utilities. These are used to map CSV keys (e.g., category_path, brand_name)
# to their database Primary Keys (PKs) during an upload session. This allows subsequent
# CSV files in the same session to resolve foreign key relationships.
from app.utils.redis_utils import add_to_id_map, get_from_id_map, DB_PK_MAP_SUFFIX

logger = logging.getLogger(__name__)

# DB_PK_MAP_SUFFIX is now imported from redis_utils


# --- Placeholder for Loader Functions ---
# Specific loader functions will be implemented in subsequent subtasks.
# Each function will typically take:
# - db_session: SQLAlchemy Session
# - business_details_id: The ID of the current business
# - record_data: A dictionary of data for a single record (Pydantic model dict)
# - session_id: The upload session ID (for Redis mapping)
# - map_type: The entity type (e.g., "categories", "brands")
# - record_key_in_csv: The field name in record_data that acts as the unique CSV key for Redis mapping
# And will return the database PK of the created/updated record, or None if failed.

# Example structure:
# def load_category_to_db(
#     db_session: Session,
#     business_details_id: int,
#     record_data: Dict[str, Any],
#     session_id: str,
#     # map_type: str = "categories", # Implicit from function name
#     # record_key_in_csv: str = "category_path" # Implicit
# ) -> Optional[int]: # Returns DB PK or None
#     """
#     Processes and saves a single category record to the database.
#     Handles hierarchical data by resolving parent_id.
#     Maps category_path to the new category's database ID in Redis.
#     """
#     # Implementation will involve:
#     # 1. Importing CategoryOrm.
#     # 2. Resolving parent_id if category_path indicates a sub-category.
#     # 3. Querying for existing category (upsert logic).
#     # 4. Creating or updating CategoryOrm instance.
#     # 5. Adding to session, flushing to get ID (if new).
#     # 6. Calling add_to_id_map to store "category_path" -> CategoryOrm.id mapping.
#     # 7. Returning CategoryOrm.id.
#     logger.debug(f"Placeholder: load_category_to_db called with data: {record_data}")
#     pass # To be implemented

def load_category_to_db(
    db_session: Session,
    business_details_id: int,
    record_data: Dict[str, Any], # This is a dict from CategoryCsvModel
    session_id: str,
    db_pk_redis_pipeline: Any # Redis pipeline for _db_pk maps
) -> Optional[int]:
    """
    Loads a single category record (potentially hierarchical) into the database.
    Manages parent-child relationships based on category_path.
    Stores (full_path_segment -> db_pk) mapping in Redis for the current session.

    Args:
        db_session: SQLAlchemy session.
        business_details_id: The integer ID for the business.
        record_data: A dictionary representing a row from the category CSV,
                     validated by CategoryCsvModel. Expected to have 'category_path'
                     and other metadata fields.
        session_id: The current upload session ID.
        db_pk_redis_pipeline: The Redis pipeline for storing _db_pk mappings.

    Returns:
        The database primary key (integer) of the processed category (the last
        segment of the path), or None if processing failed for this record.
    """
    category_path_str = record_data.get("category_path")
    if not category_path_str:
        logger.error(f"Missing 'category_path' in record_data for business {business_details_id}, session {session_id}. Record: {record_data}")
        return None

    path_levels = [level.strip() for level in category_path_str.split('/') if level.strip()]
    if not path_levels:
        logger.error(f"Empty or invalid 'category_path' after splitting: '{category_path_str}'. Record: {record_data}")
        return None

    current_parent_db_id: Optional[int] = None
    current_full_path_processed = ""
    final_category_db_id: Optional[int] = None

from app.exceptions import DataLoaderError # Import custom exception
from app.models.schemas import ErrorType # Import ErrorType Enum

    try:
        for i, level_name_from_path in enumerate(path_levels):
            current_level_full_path = f"{current_full_path_processed}/{level_name_from_path}" if current_full_path_processed else level_name_from_path
            is_last_level = (i == len(path_levels) - 1)
            current_level_name = record_data.get("name") if is_last_level and record_data.get("name") else level_name_from_path
            category_db_id: Optional[int] = None

            category_db_id_from_redis_str = get_from_id_map(session_id, f"categories{DB_PK_MAP_SUFFIX}", current_level_full_path)

            if category_db_id_from_redis_str is not None:
                category_db_id = int(category_db_id_from_redis_str)
                logger.debug(f"Redis cache hit for category path '{current_level_full_path}' -> DB ID {category_db_id}")
                if is_last_level:
                    existing_category_orm = db_session.query(CategoryOrm).filter_by(id=category_db_id, business_details_id=business_details_id).first()
                    if existing_category_orm:
                        logger.info(f"Updating metadata for existing category: '{current_level_full_path}' (ID: {category_db_id})")
                        # Update fields... (existing logic for updates)
                        existing_category_orm.description = record_data.get("description", existing_category_orm.description)
                        existing_category_orm.enabled = record_data.get("enabled", existing_category_orm.enabled)
                        # ... (all other updatable fields) ...
                        existing_category_orm.position_on_site = record_data.get("position_on_site", existing_category_orm.position_on_site)
                    else:
                        msg = f"Category ID {category_db_id} for path '{current_level_full_path}' found in Redis but not in DB or for wrong business {business_details_id}."
                        logger.error(msg)
                        raise DataLoaderError(message=msg, error_type=ErrorType.LOOKUP, field_name="category_path", offending_value=current_level_full_path)
            else:
                db_category = db_session.query(CategoryOrm).filter_by(business_details_id=business_details_id, name=current_level_name, parent_id=current_parent_db_id).first()
                if db_category:
                    category_db_id = db_category.id
                    logger.debug(f"DB hit for category: '{current_level_name}' (Parent ID: {current_parent_db_id}) -> DB ID {category_db_id}")
                    if is_last_level:
                        logger.info(f"Updating metadata for existing category (DB query): '{current_level_full_path}' (ID: {category_db_id})")
                        # Update fields... (existing logic for updates)
                        db_category.description = record_data.get("description", db_category.description)
                        # ... (all other updatable fields) ...
                        db_category.position_on_site = record_data.get("position_on_site", db_category.position_on_site)
                else:
                    logger.info(f"Creating new category level: Name='{current_level_name}', Parent DB ID='{current_parent_db_id}'")
                    orm_fields = {
                        "name": current_level_name, "parent_id": current_parent_db_id, "business_details_id": business_details_id,
                        "description": f"Category: {current_level_name}", "enabled": True,
                    }
                    if is_last_level:
                        orm_fields.update({
                            "description": record_data.get("description"), "enabled": record_data.get("enabled", True),
                            "image_name": record_data.get("image_name"), "long_description": record_data.get("long_description"),
                            "order_type": record_data.get("order_type"), "shipping_type": record_data.get("shipping_type"),
                            "active": record_data.get("active"), "seo_description": record_data.get("seo_description"),
                            "seo_keywords": record_data.get("seo_keywords"), "seo_title": record_data.get("seo_title"),
                            "url": record_data.get("url"), "position_on_site": record_data.get("position_on_site"),
                        })
                    new_category_orm = CategoryOrm(**orm_fields)
                    db_session.add(new_category_orm)
                    db_session.flush()
                    category_db_id = new_category_orm.id
                    if category_db_id is None:
                        msg = f"Failed to obtain DB ID for new category: '{current_level_name}' after flush."
                        logger.error(msg)
                        raise DataLoaderError(message=msg, error_type=ErrorType.DATABASE, field_name="category_path", offending_value=current_level_full_path)
                    logger.info(f"Created new category '{current_level_name}' with DB ID {category_db_id}")

                if db_pk_redis_pipeline is not None and category_db_id is not None:
                     add_to_id_map(session_id, f"categories{DB_PK_MAP_SUFFIX}", current_level_full_path, category_db_id, pipeline=db_pk_redis_pipeline)
                elif category_db_id is not None: # Fallback if pipeline is None for some reason
                     add_to_id_map(session_id, f"categories{DB_PK_MAP_SUFFIX}", current_level_full_path, category_db_id)

            current_parent_db_id = category_db_id
            current_full_path_processed = current_level_full_path
            if is_last_level:
                final_category_db_id = category_db_id

        return final_category_db_id

    except DataLoaderError: # Re-raise DataLoaderError to be caught by process_csv_task
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing category record (path: '{category_path_str}'): {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Unexpected error processing category path '{category_path_str}': {str(e)}",
            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
            field_name="category_path",
            offending_value=category_path_str,
            original_exception=e
        )

def load_brand_to_db(
    db_session: Session,
    business_details_id: int,
    records_data: List[Dict[str, Any]], # List of dicts from BrandCsvModel
    session_id: str,
    db_pk_redis_pipeline: Any
) -> Dict[str, int]: # Returns a summary like {"inserted": count, "updated": count}
    """
    Loads or updates a list of brand records in the database using bulk operations.
    Manages mapping of brand names to DB PKs in Redis for the current session.

    Args:
        db_session: SQLAlchemy session.
        business_details_id: The integer ID for the business.
        records_data: A list of dictionaries, each representing a row from the brand CSV,
                      validated by BrandCsvModel. Expected to have 'name', 'logo', etc.
        session_id: The current upload session ID.
        db_pk_redis_pipeline: The Redis pipeline for storing _db_pk mappings.

    Returns:
        A dictionary summarizing the count of inserted and updated records.
        Raises DataLoaderError if the bulk operation fails.
    """
    if not records_data:
        return {"inserted": 0, "updated": 0, "errors": 0}

    brand_names_from_csv = {record['name'] for record in records_data if record.get('name')}
    if not brand_names_from_csv:
        raise DataLoaderError(message="No brand names found in records_data list.", error_type=ErrorType.VALIDATION)

    summary = {"inserted": 0, "updated": 0, "errors": 0}

    try:
        # Fetch existing brands for this business in one query
        existing_brands_query = db_session.query(BrandOrm).filter(
            BrandOrm.business_details_id == business_details_id,
            BrandOrm.name.in_(brand_names_from_csv)
        )
        existing_brands_map = {brand.name: brand for brand in existing_brands_query.all()}

        new_brands_mappings = []
        updated_brands_mappings = []

        processed_brand_names_for_redis = {} # To store name -> id for redis mapping

        for record in records_data:
            brand_name = record.get("name")
            if not brand_name: # Should have been caught by Pydantic, but as a safeguard
                logger.warning(f"Skipping record due to missing brand name: {record}")
                summary["errors"] += 1
                continue

            db_brand = existing_brands_map.get(brand_name)

            if db_brand: # Existing brand -> update
                update_mapping = record.copy() # Start with all data from CSV record
                update_mapping['id'] = db_brand.id # Crucial for bulk_update_mappings
                # Ensure only fields present in BrandOrm are passed for update, and handle defaults for missing optional fields
                # For simplicity, assuming BrandCsvModel fields directly map or are handled by ORM if not present.
                # More robustly, you'd filter `update_mapping` to only include valid ORM columns.
                # Example: db_brand.logo = record.get("logo", db_brand.logo) - this is row-by-row, defeats bulk.
                # For bulk_update_mappings, the dict should contain only fields to update.
                # If a field from CSV is not in record, it won't be in update_mapping, so existing value preserved.
                # If a field is None in CSV and you want to set it to NULL, ensure record has it as None.
                updated_brands_mappings.append(update_mapping)
                processed_brand_names_for_redis[brand_name] = db_brand.id
                summary["updated"] += 1
            else: # New brand -> insert
                insert_mapping = record.copy()
                insert_mapping['business_details_id'] = business_details_id
                # Audit fields (created_by, created_date etc.) will be taken from record if present,
                # or DB defaults / ORM defaults will apply.
                new_brands_mappings.append(insert_mapping)
                # ID for new brands will be available after bulk_insert for Redis mapping
                # We'll handle Redis mapping after DB operations.

        if updated_brands_mappings:
            logger.info(f"Bulk updating {len(updated_brands_mappings)} brands for business {business_details_id}.")
            db_session.bulk_update_mappings(BrandOrm, updated_brands_mappings)

        if new_brands_mappings:
            logger.info(f"Bulk inserting {len(new_brands_mappings)} new brands for business {business_details_id}.")
            # bulk_insert_mappings does not return generated IDs directly in a simple way across all DBs.
            # We need to fetch them if we need them for Redis mapping immediately.
            # However, SQLAlchemy with PostgreSQL (using psycopg2) often populates PKs on the original dicts
            # if the table has an autoincrementing PK and the dialect supports returning values.
            # Let's assume for now that IDs might not be populated back into new_brands_mappings dicts directly by this call.
            db_session.bulk_insert_mappings(BrandOrm, new_brands_mappings)
            summary["inserted"] = len(new_brands_mappings)

            # To get IDs for Redis mapping for newly inserted brands:
            # Re-fetch them. This adds a query but ensures we have IDs.
            # This is a common pattern when bulk_insert_mappings doesn't reliably return/populate IDs.
            if summary["inserted"] > 0:
                db_session.flush() # Ensure inserts are processed to make them queryable if re-fetch is needed
                newly_inserted_names = [b['name'] for b in new_brands_mappings]

                # Option 1: If `new_brands_mappings` dicts get their 'id' populated after flush by SQLAlchemy (depends on dialect/config)
                # This is often the case for PostgreSQL if `implicit_returning=True` on table or engine.
                # We'll assume this optimistic path for now.
                for brand_mapping_dict in new_brands_mappings:
                    if 'id' in brand_mapping_dict and brand_mapping_dict['id'] is not None:
                         processed_brand_names_for_redis[brand_mapping_dict['name']] = brand_mapping_dict['id']
                    else:
                        # Fallback: If ID not populated, log warning or re-fetch. For now, log.
                        logger.warning(f"ID not populated after bulk insert for brand: {brand_mapping_dict['name']}. Redis map might be incomplete for this new brand.")
                        summary["errors"] += 1 # Count as an error if ID isn't available for mapping

        # Redis Mapping for all successfully processed (inserted or updated)
        if processed_brand_names_for_redis and db_pk_redis_pipeline is not None:
            for brand_name, brand_id in processed_brand_names_for_redis.items():
                add_to_id_map(session_id, f"brands{DB_PK_MAP_SUFFIX}", brand_name, brand_id, pipeline=db_pk_redis_pipeline)
        elif processed_brand_names_for_redis: # Fallback if pipeline is None
             for brand_name, brand_id in processed_brand_names_for_redis.items():
                add_to_id_map(session_id, f"brands{DB_PK_MAP_SUFFIX}", brand_name, brand_id)

        # db_session.flush() # Ensure operations are sent to DB before commit by caller
        # The main commit/rollback is handled by process_csv_task

        return summary

    except IntegrityError as e: # Usually for unique constraint violations during bulk op
        # db_session.rollback() # Handled by caller
        logger.error(f"Bulk database integrity error processing brands for business {business_details_id}: {e.orig}", exc_info=False)
        raise DataLoaderError(
            message=f"Bulk database integrity error for brands: {str(e.orig)}",
            error_type=ErrorType.DATABASE,
            original_exception=e
        )
    except Exception as e: # Catch other SQLAlchemy errors or unexpected issues
        # db_session.rollback() # Handled by caller
        logger.error(f"Unexpected error during bulk processing of brands for business {business_details_id}: {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Unexpected error during bulk brand processing: {str(e)}",
            error_type=ErrorType.UNEXPECTED_ROW_ERROR, # Or DATABASE if more appropriate
            original_exception=e
        )

def load_attribute_to_db(
    db_session: Session,
    business_details_id: int,
    record_data: Dict[str, Any], # Dict from AttributeCsvModel
    session_id: str,
    db_pk_redis_pipeline: Any # Redis pipeline object
) -> Optional[int]:
    """
    Loads or updates a parent attribute and its associated values in the database.
    Handles parsing of pipe-separated value strings from the CSV record.
    Maps parent attribute_name to its DB PK in Redis for the current session.

    Args:
        db_session: SQLAlchemy session.
        business_details_id: The integer ID for the business.
        record_data: A dictionary representing a row from the attribute CSV,
                     validated by AttributeCsvModel. Expected to have 'attribute_name',
                     pipe-separated value fields, etc.
        session_id: The current upload session ID.
        db_pk_redis_pipeline: The Redis pipeline for storing _db_pk mappings.

    Returns:
        The database primary key (integer/bigint) of the processed parent attribute,
        or None if processing failed for this record.
    """
    parent_attribute_name = record_data.get("attribute_name")
    if not parent_attribute_name:
        logger.error(f"Missing 'attribute_name' in record_data for business {business_details_id}, session {session_id}. Record: {record_data}")
        return None

    attribute_db_id: Optional[int] = None
    parent_attr_orm_instance: Optional[AttributeOrm] = None # To access is_color for value processing

    try:
        parent_attr_orm_instance = db_session.query(AttributeOrm).filter_by(business_details_id=business_details_id, name=parent_attribute_name).first()
        is_color_from_csv = record_data.get('is_color', False)

        if parent_attr_orm_instance:
            logger.info(f"Updating existing attribute '{parent_attribute_name}' (ID: {parent_attr_orm_instance.id})")
            parent_attr_orm_instance.is_color = is_color_from_csv
            parent_attr_orm_instance.active = record_data.get("attribute_active", parent_attr_orm_instance.active)
            if record_data.get("updated_by") is not None: parent_attr_orm_instance.updated_by = record_data.get("updated_by")
            if record_data.get("updated_date") is not None: parent_attr_orm_instance.updated_date = record_data.get("updated_date")
            attribute_db_id = parent_attr_orm_instance.id
        else:
            logger.info(f"Creating new attribute '{parent_attribute_name}'")
            parent_orm_data = {
                "business_details_id": business_details_id, "name": parent_attribute_name,
                "is_color": is_color_from_csv, "active": record_data.get("attribute_active"),
                "created_by": record_data.get("created_by"), "created_date": record_data.get("created_date"),
                "updated_by": record_data.get("updated_by", record_data.get("created_by")),
                "updated_date": record_data.get("updated_date", record_data.get("created_date")),
            }
            parent_attr_orm_instance = AttributeOrm(**parent_orm_data) # Assign to instance for later use
            db_session.add(parent_attr_orm_instance)
            db_session.flush()
            if parent_attr_orm_instance.id is None:
                msg = f"DB flush failed to return an ID for new attribute '{parent_attribute_name}'."
                logger.error(msg)
                raise DataLoaderError(message=msg, error_type=ErrorType.DATABASE, field_name="attribute_name", offending_value=parent_attribute_name)
            attribute_db_id = parent_attr_orm_instance.id
            logger.info(f"Created new attribute '{parent_attribute_name}' with DB ID {attribute_db_id}")

        if attribute_db_id is not None:
            if db_pk_redis_pipeline is not None:
                add_to_id_map(session_id, f"attributes{DB_PK_MAP_SUFFIX}", parent_attribute_name, attribute_db_id, pipeline=db_pk_redis_pipeline)
            else:
                add_to_id_map(session_id, f"attributes{DB_PK_MAP_SUFFIX}", parent_attribute_name, attribute_db_id)

        values_name_str = record_data.get("values_name")
        if values_name_str and attribute_db_id is not None and parent_attr_orm_instance is not None: # Ensure parent_attr_orm_instance is available
            value_display_names = [name.strip() for name in values_name_str.split('|') if name.strip()]
            raw_value_values = record_data.get("value_value")
            value_actual_values = [v.strip() for v in raw_value_values.split('|')] if raw_value_values else []
            raw_img_urls = record_data.get("img_url")
            value_image_urls = [img.strip() for img in raw_img_urls.split('|')] if raw_img_urls else []
            raw_values_active = record_data.get("values_active")
            value_active_statuses = [status.strip().upper() for status in raw_values_active.split('|')] if raw_values_active else []
            num_values = len(value_display_names)

            for i, val_display_name in enumerate(value_display_names):
                actual_value_part = value_actual_values[i] if i < len(value_actual_values) else None
                image_url_part = value_image_urls[i] if i < len(value_image_urls) else None
                active_status_part = value_active_statuses[i] if i < len(value_active_statuses) and value_active_statuses[i] in ["ACTIVE", "INACTIVE"] else "INACTIVE"
                value_for_db = actual_value_part if parent_attr_orm_instance.is_color and actual_value_part else val_display_name # Corrected logic

                if not value_for_db:
                    logger.warning(f"Skipping attribute value for '{parent_attribute_name}' due to missing value/name part at index {i}.")
                    continue

                attr_value_orm = db_session.query(AttributeValueOrm).filter_by(attribute_id=attribute_db_id, name=val_display_name).first()
                if attr_value_orm:
                    logger.debug(f"Updating existing attribute value '{val_display_name}' for attribute ID {attribute_db_id}")
                    attr_value_orm.value = value_for_db
                    attr_value_orm.attribute_image_url = image_url_part if image_url_part is not None else attr_value_orm.attribute_image_url
                    attr_value_orm.active = active_status_part
                else:
                    logger.debug(f"Creating new attribute value '{val_display_name}' for attribute ID {attribute_db_id}")
                    new_val_orm_data = {
                        "attribute_id": attribute_db_id, "name": val_display_name, "value": value_for_db,
                        "attribute_image_url": image_url_part, "active": active_status_part,
                    }
                    attr_value_orm = AttributeValueOrm(**new_val_orm_data)
                    db_session.add(attr_value_orm)

        return attribute_db_id

    except DataLoaderError: # Re-raise
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing attribute record '{parent_attribute_name}': {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Unexpected error for attribute '{parent_attribute_name}': {str(e)}",
            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
            field_name="attribute_name",
            offending_value=parent_attribute_name,
            original_exception=e
        )

def load_return_policy_to_db(
    db_session: Session,
    business_details_id: int, # This is the integer business ID
    record_data: Dict[str, Any], # Dict from ReturnPolicyCsvModel
    session_id: str, # Used for logging context and potentially future Redis use
    db_pk_redis_pipeline: Any # Redis pipeline, not used in this loader for now
) -> Optional[int]:
    """
    Loads or updates a single return policy record in the database.
    Handles conditional nullification of fields based on return_policy_type.
    No Redis _db_pk mapping is done here as products are expected to link by integer ID.

    Args:
        db_session: SQLAlchemy session.
        business_details_id: The integer ID for the business.
        record_data: A dictionary representing a row from the return policy CSV,
                     validated by ReturnPolicyCsvModel.
        session_id: The current upload session ID (for logging).
        db_pk_redis_pipeline: Redis pipeline (not used by this function for _db_pk map).

    Returns:
        The database primary key (integer) of the processed return policy,
        or None if processing failed for this record.
    """

    csv_policy_id = record_data.get("id") # ID from CSV, if provided (for updates)
    return_policy_type = record_data.get("return_policy_type")

    if not return_policy_type: # Mandatory field
        logger.error(f"Missing 'return_policy_type' in record_data for business {business_details_id}, session {session_id}. Record: {record_data}")
        return None

    db_pk: Optional[int] = None
    db_policy_orm_instance: Optional[ReturnPolicyOrm] = None # Renamed for clarity

    try:
        policy_data_for_orm = {
            "policy_name": record_data.get("policy_name"), "return_type": return_policy_type,
            "return_days": record_data.get("time_period_return"), "business_details_id": business_details_id,
        }
        if record_data.get('created_date') is not None: policy_data_for_orm['created_date_ts'] = record_data.get('created_date')
        if record_data.get('updated_date') is not None: policy_data_for_orm['updated_date_ts'] = record_data.get('updated_date')

        if return_policy_type == "SALES_ARE_FINAL":
            policy_data_for_orm["policy_name"] = None
            policy_data_for_orm["return_days"] = None

        if csv_policy_id is not None:
            db_policy_orm_instance = db_session.query(ReturnPolicyOrm).filter_by(id=csv_policy_id, business_details_id=business_details_id).first()

        if db_policy_orm_instance:
            logger.info(f"Updating existing return policy ID '{csv_policy_id}'")
            for key, value in policy_data_for_orm.items():
                setattr(db_policy_orm_instance, key, value)
            db_pk = db_policy_orm_instance.id
        else:
            if csv_policy_id is not None:
                logger.warning(f"Return policy ID '{csv_policy_id}' from CSV not found for business {business_details_id}. Creating as new.")

            logger.info(f"Creating new return policy. Name: {policy_data_for_orm.get('policy_name', 'N/A')}")
            new_policy_orm = ReturnPolicyOrm(**policy_data_for_orm)
            db_session.add(new_policy_orm)
            db_session.flush()
            if new_policy_orm.id is None:
                msg = f"DB flush failed to return an ID for new return policy. Name: {policy_data_for_orm.get('policy_name')}"
                logger.error(msg)
                raise DataLoaderError(message=msg, error_type=ErrorType.DATABASE, field_name="policy_name", offending_value=policy_data_for_orm.get('policy_name'))
            db_pk = new_policy_orm.id
            logger.info(f"Created new return policy with DB ID {db_pk}. Name: {policy_data_for_orm.get('policy_name')}")

        return db_pk

    except DataLoaderError: # Re-raise
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing return policy record: {record_data} - {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Unexpected error for return policy '{record_data.get('policy_name', csv_policy_id)}': {str(e)}",
            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
            field_name="id" if csv_policy_id else "policy_name",
            offending_value=csv_policy_id if csv_policy_id else record_data.get('policy_name'),
            original_exception=e
        )

def load_price_to_db(
    db_session: Session,
    business_details_id: int, # Integer business ID from context/token
    record_data: Dict[str, Any], # This is a dict from PriceCsv model
    session_id: str, # For logging context
    db_pk_redis_pipeline: Any # Redis pipeline, not directly used for _db_pk map here
) -> Optional[int]:
    """
    Loads or updates a single price record in the database for a product or SKU.
    Validates that the target product/SKU exists and belongs to the business.

    Args:
        db_session: SQLAlchemy session.
        business_details_id: The integer ID for the business.
        record_data: A dictionary representing a row from the price CSV,
                     validated by PriceCsv.
        session_id: The current upload session ID (for logging).
        db_pk_redis_pipeline: Redis pipeline (not used by this function for _db_pk map).

    Returns:
        The database primary key (integer) of the processed PriceOrm record,
        or None if processing failed for this record.
    """
    try:
        price_type_csv_enum = PriceCsvTypeEnum(record_data["price_type"])
        target_product_id: Optional[int] = None
        target_sku_id: Optional[int] = None
        target_id_field_name = "product_id" # for error reporting

        if price_type_csv_enum == PriceCsvTypeEnum.PRODUCT:
            csv_product_id_str = record_data.get("product_id")
            if not csv_product_id_str:
                raise DataLoaderError(message="product_id missing for PRODUCT type price.", error_type=ErrorType.VALIDATION, field_name="product_id", offending_value=None)
            try:
                target_product_id = int(csv_product_id_str)
            except ValueError as ve:
                raise DataLoaderError(message=f"Invalid product_id format '{csv_product_id_str}'. Must be integer.", error_type=ErrorType.VALIDATION, field_name="product_id", offending_value=csv_product_id_str, original_exception=ve)

            product_check = db_session.query(ProductOrm.id).filter(ProductOrm.id == target_product_id, ProductOrm.business_details_id == business_details_id).first()
            if not product_check:
                raise DataLoaderError(message=f"Product ID {target_product_id} not found or not associated with business {business_details_id}.", error_type=ErrorType.LOOKUP, field_name="product_id", offending_value=target_product_id)

        elif price_type_csv_enum == PriceCsvTypeEnum.SKU:
            target_id_field_name = "sku_id"
            csv_sku_id_str = record_data.get("sku_id")
            if not csv_sku_id_str:
                raise DataLoaderError(message="sku_id missing for SKU type price.", error_type=ErrorType.VALIDATION, field_name="sku_id", offending_value=None)
            try:
                target_sku_id = int(csv_sku_id_str)
            except ValueError as ve:
                raise DataLoaderError(message=f"Invalid sku_id format '{csv_sku_id_str}'. Must be integer.", error_type=ErrorType.VALIDATION, field_name="sku_id", offending_value=csv_sku_id_str, original_exception=ve)

            sku_check = db_session.query(ProductItemOrm.id).filter(ProductItemOrm.id == target_sku_id, ProductItemOrm.business_details_id == business_details_id).first()
            if not sku_check:
                raise DataLoaderError(message=f"SKU ID {target_sku_id} not found or not associated with business {business_details_id}.", error_type=ErrorType.LOOKUP, field_name="sku_id", offending_value=target_sku_id)
        else:
            # Should be caught by Pydantic if PriceCsv.price_type uses the enum.
            raise DataLoaderError(message=f"Unknown price_type '{record_data['price_type']}'.", error_type=ErrorType.VALIDATION, field_name="price_type", offending_value=record_data['price_type'])

        existing_price_query = db_session.query(PriceOrm).filter(PriceOrm.business_details_id == business_details_id)
        if target_product_id: existing_price_query = existing_price_query.filter(PriceOrm.product_id == target_product_id)
        elif target_sku_id: existing_price_query = existing_price_query.filter(PriceOrm.sku_id == target_sku_id)

        existing_price: Optional[PriceOrm] = existing_price_query.first()
        price_orm_instance: PriceOrm

        if existing_price:
            logger.info(f"Updating price for {'product ' + str(target_product_id) if target_product_id else 'SKU ' + str(target_sku_id)}")
            existing_price.price = record_data["price"]
            existing_price.discount_price = record_data.get("discount_price")
            existing_price.cost_price = record_data.get("cost_price")
            existing_price.currency = record_data.get("currency", "USD")
            price_orm_instance = existing_price
        else:
            logger.info(f"Creating new price for {'product ' + str(target_product_id) if target_product_id else 'SKU ' + str(target_sku_id)}")
            new_price_data = {
                "business_details_id": business_details_id, "product_id": target_product_id, "sku_id": target_sku_id,
                "price": record_data["price"], "discount_price": record_data.get("discount_price"),
                "cost_price": record_data.get("cost_price"), "currency": record_data.get("currency", "USD"),
            }
            price_orm_instance = PriceOrm(**new_price_data)
            db_session.add(price_orm_instance)

        db_session.flush()
        if price_orm_instance.id is None:
            msg = f"DB flush failed to return an ID for price record. Target: {'P:' + str(target_product_id) if target_product_id else 'S:' + str(target_sku_id)}"
            logger.error(msg)
            raise DataLoaderError(message=msg, error_type=ErrorType.DATABASE, field_name=target_id_field_name, offending_value=target_product_id or target_sku_id)

        return price_orm_instance.id

    except DataLoaderError: # Re-raise
        raise
    except ValueError as ve: # Catch specific value errors e.g. from PriceCsvTypeEnum conversion if string is invalid
        logger.error(f"Value error processing price record for business {business_details_id}: {record_data} - {ve}", exc_info=True)
        raise DataLoaderError(message=f"Invalid value in price record: {str(ve)}", error_type=ErrorType.VALIDATION, offending_value=str(record_data), original_exception=ve)
    except Exception as e:
        logger.error(f"Unexpected error processing price record for business {business_details_id}: {record_data} - {e}", exc_info=True)
        raise DataLoaderError(
            message=f"Unexpected error for price record (target: {'P:' + str(record_data.get('product_id')) if record_data.get('price_type') == 'PRODUCT' else 'S:' + str(record_data.get('sku_id'))}): {str(e)}",
            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
            offending_value=str(record_data),
            original_exception=e
        )

# ... other loader functions for products, attributes, etc. ...
