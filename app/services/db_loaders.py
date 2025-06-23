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

    try:
        for i, level_name_from_path in enumerate(path_levels):
            if not current_full_path_processed:
                current_level_full_path = level_name_from_path
            else:
                current_level_full_path = f"{current_full_path_processed}/{level_name_from_path}"

            is_last_level = (i == len(path_levels) - 1)

            current_level_name = record_data.get("name") if is_last_level and record_data.get("name") else level_name_from_path

            category_db_id: Optional[int] = None # Initialize here

            # 1. Check Redis for existing DB PK for this full_path segment
            category_db_id_from_redis_str = get_from_id_map(
                session_id, f"categories{DB_PK_MAP_SUFFIX}", current_level_full_path
            )

            if category_db_id_from_redis_str is not None:
                category_db_id = int(category_db_id_from_redis_str)
                logger.debug(f"Redis cache hit for category path '{current_level_full_path}' -> DB ID {category_db_id} (business: {business_details_id})")

                if is_last_level: # Potentially update metadata if this is the target category of the row
                    existing_category_orm = db_session.query(CategoryOrm).filter_by(
                        id=category_db_id,
                        business_details_id=business_details_id # Ensure it belongs to the same business
                    ).first()
                    if existing_category_orm:
                        logger.info(f"Updating metadata for existing category: '{current_level_full_path}' (ID: {category_db_id})")
                        existing_category_orm.description = record_data.get("description", existing_category_orm.description)
                        existing_category_orm.enabled = record_data.get("enabled", existing_category_orm.enabled)
                        existing_category_orm.image_name = record_data.get("image_name", existing_category_orm.image_name)
                        existing_category_orm.long_description = record_data.get("long_description", existing_category_orm.long_description)
                        existing_category_orm.order_type = record_data.get("order_type", existing_category_orm.order_type)
                        existing_category_orm.shipping_type = record_data.get("shipping_type", existing_category_orm.shipping_type)
                        existing_category_orm.active = record_data.get("active", existing_category_orm.active)
                        existing_category_orm.seo_description = record_data.get("seo_description", existing_category_orm.seo_description)
                        existing_category_orm.seo_keywords = record_data.get("seo_keywords", existing_category_orm.seo_keywords)
                        existing_category_orm.seo_title = record_data.get("seo_title", existing_category_orm.seo_title)
                        existing_category_orm.url = record_data.get("url", existing_category_orm.url) # Consider uniqueness for URL if it's a separate field
                        existing_category_orm.position_on_site = record_data.get("position_on_site", existing_category_orm.position_on_site)
                        # created_by, created_date etc. are not typically updated from CSV for existing records
                    else:
                        logger.error(f"Category ID {category_db_id} for path '{current_level_full_path}' found in Redis but not in DB for business {business_details_id}. Inconsistency.")
                        return None # Critical error
            else:
                # 2. Not in Redis, so query DB by name and parent_id for this business
                db_category = db_session.query(CategoryOrm).filter_by(
                    business_details_id=business_details_id,
                    name=current_level_name,
                    parent_id=current_parent_db_id
                ).first()

                if db_category: # Exists in DB
                    category_db_id = db_category.id
                    logger.debug(f"DB hit for category: '{current_level_name}' (Parent ID: {current_parent_db_id}) -> DB ID {category_db_id}")
                    if is_last_level: # Update metadata if this is the target category
                        logger.info(f"Updating metadata for existing category (found via DB query): '{current_level_full_path}' (ID: {category_db_id})")
                        db_category.description = record_data.get("description", db_category.description)
                        db_category.enabled = record_data.get("enabled", db_category.enabled)
                        db_category.image_name = record_data.get("image_name", db_category.image_name)
                        db_category.long_description = record_data.get("long_description", db_category.long_description)
                        db_category.order_type = record_data.get("order_type", db_category.order_type)
                        db_category.shipping_type = record_data.get("shipping_type", db_category.shipping_type)
                        db_category.active = record_data.get("active", db_category.active)
                        db_category.seo_description = record_data.get("seo_description", db_category.seo_description)
                        db_category.seo_keywords = record_data.get("seo_keywords", db_category.seo_keywords)
                        db_category.seo_title = record_data.get("seo_title", db_category.seo_title)
                        db_category.url = record_data.get("url", db_category.url)
                        db_category.position_on_site = record_data.get("position_on_site", db_category.position_on_site)
                else: # New category level, create it
                    logger.info(f"Creating new category level: Name='{current_level_name}', Parent DB ID='{current_parent_db_id}' for path '{current_level_full_path}'")
                    orm_fields = {
                        "name": current_level_name,
                        "parent_id": current_parent_db_id,
                        "business_details_id": business_details_id,
                        # Default description for intermediate categories if not the target of the row
                        "description": f"Category: {current_level_name}",
                        "enabled": True, # Default for new categories
                    }
                    if is_last_level: # Populate all metadata only for the target category of this CSV row
                        orm_fields.update({
                            "description": record_data.get("description"), # Override default if provided
                            "enabled": record_data.get("enabled", True),
                            "image_name": record_data.get("image_name"),
                            "long_description": record_data.get("long_description"),
                            "order_type": record_data.get("order_type"),
                            "shipping_type": record_data.get("shipping_type"),
                            "active": record_data.get("active"),
                            "seo_description": record_data.get("seo_description"),
                            "seo_keywords": record_data.get("seo_keywords"),
                            "seo_title": record_data.get("seo_title"),
                            "url": record_data.get("url"),
                            "position_on_site": record_data.get("position_on_site"),
                            # Audit fields (created_by, created_date etc. are from ORM defaults or DDL)
                        })

                    new_category_orm = CategoryOrm(**orm_fields)
                    db_session.add(new_category_orm)
                    db_session.flush() # To get the new_category_orm.id
                    category_db_id = new_category_orm.id
                    if category_db_id is None:
                        logger.error(f"Failed to obtain DB ID for new category: '{current_level_name}' after flush.")
                        # This indicates a serious issue, probably should halt this record.
                        raise Exception(f"DB flush failed to return an ID for new category '{current_level_name}'.")
                    logger.info(f"Created new category '{current_level_name}' with DB ID {category_db_id}")

                # Add to Redis _db_pk map (using the full path as key)
                # This happens whether it was found in DB or newly created, to populate cache for next levels/rows.
                if db_pk_redis_pipeline is not None:
                     add_to_id_map(session_id, f"categories{DB_PK_MAP_SUFFIX}", current_level_full_path, category_db_id, pipeline=db_pk_redis_pipeline)
                else:
                     add_to_id_map(session_id, f"categories{DB_PK_MAP_SUFFIX}", current_level_full_path, category_db_id)

            current_parent_db_id = category_db_id
            current_full_path_processed = current_level_full_path
            if is_last_level:
                final_category_db_id = category_db_id

        # The db_session.commit() or rollback() is handled by the calling task (process_csv_task)
        # after processing all records in a file.
        return final_category_db_id

    except Exception as e:
        logger.error(f"Error processing category record (path: '{category_path_str}') for business {business_details_id}: {e}", exc_info=True)
        # Do not rollback here; let the caller manage the transaction for the whole file.
        return None

def load_brand_to_db(
    db_session: Session,
    business_details_id: int,
    record_data: Dict[str, Any], # Dict from BrandCsvModel
    session_id: str,
    db_pk_redis_pipeline: Any # Redis pipeline object
) -> Optional[int]:
    """
    Loads or updates a single brand record in the database.
    Manages mapping of brand name to DB PK in Redis for the current session.

    Args:
        db_session: SQLAlchemy session.
        business_details_id: The integer ID for the business.
        record_data: A dictionary representing a row from the brand CSV,
                     validated by BrandCsvModel. Expected to have 'name', 'logo',
                     and other brand-specific fields.
        session_id: The current upload session ID.
        db_pk_redis_pipeline: The Redis pipeline for storing _db_pk mappings.

    Returns:
        The database primary key (integer / bigint) of the processed brand,
        or None if processing failed for this record.
    """
    brand_name_from_csv = record_data.get("name")
    if not brand_name_from_csv:
        logger.error(f"Missing 'name' (brand name) in record_data for business {business_details_id}, session {session_id}. Record: {record_data}")
        return None

    brand_db_id: Optional[int] = None

    try:
        # Check if brand already exists for this business
        db_brand = db_session.query(BrandOrm).filter_by(
            business_details_id=business_details_id,
            name=brand_name_from_csv
        ).first()

        if db_brand:  # Existing brand, update it
            logger.info(f"Updating existing brand '{brand_name_from_csv}' (ID: {db_brand.id}) for business {business_details_id}")
            db_brand.logo = record_data.get("logo", db_brand.logo) # Keep old if CSV doesn't provide new
            db_brand.supplier_id = record_data.get("supplier_id", db_brand.supplier_id)
            db_brand.active = record_data.get("active", db_brand.active)

            # Handle BigInt audit dates: only update if provided in CSV
            # These are BigInt in ORM, matching DDL. Pydantic model provides them as Optional[int].
            if record_data.get("created_by") is not None:
                db_brand.created_by = record_data.get("created_by")
            if record_data.get("created_date") is not None:
                db_brand.created_date = record_data.get("created_date")
            if record_data.get("updated_by") is not None:
                db_brand.updated_by = record_data.get("updated_by")
            if record_data.get("updated_date") is not None:
                db_brand.updated_date = record_data.get("updated_date")

            brand_db_id = db_brand.id
        else:  # New brand, create it
            logger.info(f"Creating new brand '{brand_name_from_csv}' for business {business_details_id}")

            orm_data = {
                "business_details_id": business_details_id,
                "name": brand_name_from_csv,
                "logo": record_data.get("logo"), # Mandatory in BrandCsvModel
                "supplier_id": record_data.get("supplier_id"),
                "active": record_data.get("active"),
                "created_by": record_data.get("created_by"),
                "created_date": record_data.get("created_date"),
                "updated_by": record_data.get("updated_by"),
                "updated_date": record_data.get("updated_date"),
            }

            new_brand_orm = BrandOrm(**orm_data)
            db_session.add(new_brand_orm)
            db_session.flush()  # To get the new_brand_orm.id

            if new_brand_orm.id is None:
                logger.error(f"Failed to obtain DB ID for new brand '{brand_name_from_csv}' after flush.")
                raise Exception(f"DB flush failed to return an ID for new brand '{brand_name_from_csv}'.")
            brand_db_id = new_brand_orm.id
            logger.info(f"Created new brand '{brand_name_from_csv}' with DB ID {brand_db_id}")

        # Store/Update mapping of brand_name_from_csv -> brand_db_id in Redis for this session
        if brand_db_id is not None: # Should always be true if no exception before this
            if db_pk_redis_pipeline is not None:
                add_to_id_map(
                    session_id,
                    f"brands{DB_PK_MAP_SUFFIX}",
                    brand_name_from_csv,
                    brand_db_id,
                    pipeline=db_pk_redis_pipeline
                )
            else: # Fallback, though pipeline is expected from process_csv_task
                add_to_id_map(session_id, f"brands{DB_PK_MAP_SUFFIX}", brand_name_from_csv, brand_db_id)

        return brand_db_id

    except Exception as e:
        logger.error(f"Error processing brand record '{brand_name_from_csv}' for business {business_details_id}: {e}", exc_info=True)
        return None

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
    parent_attr_orm_instance: Optional[AttributeOrm] = None

    try:
        # 1. Upsert Parent Attribute (AttributeOrm)
        parent_attr_orm_instance = db_session.query(AttributeOrm).filter_by(
            business_details_id=business_details_id,
            name=parent_attribute_name
        ).first()

        is_color_from_csv = record_data.get('is_color', False)

        if parent_attr_orm_instance: # Existing parent attribute
            logger.info(f"Updating existing attribute '{parent_attribute_name}' (ID: {parent_attr_orm_instance.id}) for business {business_details_id}")
            parent_attr_orm_instance.is_color = is_color_from_csv
            parent_attr_orm_instance.active = record_data.get("attribute_active", parent_attr_orm_instance.active)

            if record_data.get("updated_by") is not None: parent_attr_orm_instance.updated_by = record_data.get("updated_by")
            if record_data.get("updated_date") is not None: parent_attr_orm_instance.updated_date = record_data.get("updated_date")
            # created_by and created_date are typically not updated for existing records from CSV.
            attribute_db_id = parent_attr_orm_instance.id
        else: # New parent attribute
            logger.info(f"Creating new attribute '{parent_attribute_name}' for business {business_details_id}")
            parent_orm_data = {
                "business_details_id": business_details_id,
                "name": parent_attribute_name,
                "is_color": is_color_from_csv,
                "active": record_data.get("attribute_active"),
                "created_by": record_data.get("created_by"),
                "created_date": record_data.get("created_date"),
                "updated_by": record_data.get("updated_by", record_data.get("created_by")),
                "updated_date": record_data.get("updated_date", record_data.get("created_date")),
            }
            parent_attr_orm_instance = AttributeOrm(**parent_orm_data)
            db_session.add(parent_attr_orm_instance)
            db_session.flush()
            if parent_attr_orm_instance.id is None:
                logger.error(f"Failed to obtain DB ID for new attribute '{parent_attribute_name}' after flush.")
                raise Exception(f"DB flush failed to return an ID for new attribute '{parent_attribute_name}'.")
            attribute_db_id = parent_attr_orm_instance.id
            logger.info(f"Created new attribute '{parent_attribute_name}' with DB ID {attribute_db_id}")

        if attribute_db_id is not None: # Should always be true if no exception before this
            if db_pk_redis_pipeline is not None:
                add_to_id_map(
                    session_id,
                    f"attributes{DB_PK_MAP_SUFFIX}",
                    parent_attribute_name,
                    attribute_db_id,
                    pipeline=db_pk_redis_pipeline
                )
            else:
                 add_to_id_map(session_id, f"attributes{DB_PK_MAP_SUFFIX}", parent_attribute_name, attribute_db_id)


        # 2. Process Attribute Values (AttributeValueOrm)
        values_name_str = record_data.get("values_name")
        if values_name_str and attribute_db_id is not None:
            value_display_names = [name.strip() for name in values_name_str.split('|') if name.strip()]

            raw_value_values = record_data.get("value_value")
            value_actual_values = [v.strip() for v in raw_value_values.split('|')] if raw_value_values else []

            raw_img_urls = record_data.get("img_url")
            value_image_urls = [img.strip() for img in raw_img_urls.split('|')] if raw_img_urls else []

            raw_values_active = record_data.get("values_active")
            value_active_statuses = [status.strip().upper() for status in raw_values_active.split('|')] if raw_values_active else []

            # Pydantic model should have already validated list length consistency.
            # Here, we assume they are consistent or pad shorter lists.
            num_values = len(value_display_names)

            for i, val_display_name in enumerate(value_display_names):
                actual_value_part = value_actual_values[i] if i < len(value_actual_values) else None
                image_url_part = value_image_urls[i] if i < len(value_image_urls) else None
                # Default active status for a value to "INACTIVE" if not provided or if list is shorter.
                # The ORM model has server_default="INACTIVE", so this explicit default might only be for clarity
                # or if we want to override the ORM default based on CSV presence.
                active_status_part = value_active_statuses[i] if i < len(value_active_statuses) and value_active_statuses[i] in ["ACTIVE", "INACTIVE"] else "INACTIVE"

                value_for_db: str
                if parent_attr_orm_instance.is_color: # parent_attr_orm_instance should be set from above
                    value_for_db = actual_value_part if actual_value_part else val_display_name
                else:
                    value_for_db = actual_value_part if actual_value_part else val_display_name

                if not value_for_db:
                    logger.warning(f"Skipping attribute value for '{parent_attribute_name}' due to missing value/name part at index {i}.")
                    continue

                attr_value_orm = db_session.query(AttributeValueOrm).filter_by(
                    attribute_id=attribute_db_id,
                    name=val_display_name
                ).first()

                if attr_value_orm:
                    logger.debug(f"Updating existing attribute value '{val_display_name}' for attribute ID {attribute_db_id}")
                    attr_value_orm.value = value_for_db
                    attr_value_orm.attribute_image_url = image_url_part if image_url_part is not None else attr_value_orm.attribute_image_url
                    attr_value_orm.active = active_status_part
                    # Audit fields for values are not typically updated from this CSV structure directly
                else:
                    logger.debug(f"Creating new attribute value '{val_display_name}' for attribute ID {attribute_db_id}")
                    new_val_orm_data = {
                        "attribute_id": attribute_db_id,
                        "name": val_display_name,
                        "value": value_for_db,
                        "attribute_image_url": image_url_part,
                        "active": active_status_part,
                        # logo_name is from DDL, default is None or handled by DB
                        # created_by, created_date for attribute values are not sourced from this CSV structure
                    }
                    attr_value_orm = AttributeValueOrm(**new_val_orm_data)
                    db_session.add(attr_value_orm)
            # Rely on process_csv_task's final commit for attribute values of this parent attribute.
            # If a mix of new/updated values, flush might be needed if new value IDs were referenced elsewhere.
            # db_session.flush() # If needed to get IDs for AttributeValueOrm immediately

        return attribute_db_id

    except Exception as e:
        logger.error(f"Error processing attribute record '{parent_attribute_name}' for business {business_details_id}: {e}", exc_info=True)
        return None

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
    db_policy: Optional[ReturnPolicyOrm] = None

    try:
        # Prepare data for ORM, applying conditional logic first
        policy_data_for_orm = {
            "policy_name": record_data.get("policy_name"),
            "return_type": return_policy_type,
            "return_days": record_data.get("time_period_return"), # Mapped to return_days
            "business_details_id": business_details_id,
            # grace_period_return is not directly mapped as ORM has no such field
        }
        if record_data.get('created_date') is not None: # Pydantic model makes it datetime
            policy_data_for_orm['created_date_ts'] = record_data.get('created_date') # Changed key
        if record_data.get('updated_date') is not None:
            policy_data_for_orm['updated_date_ts'] = record_data.get('updated_date') # Changed key


        if return_policy_type == "SALES_ARE_FINAL":
            policy_data_for_orm["policy_name"] = None
            policy_data_for_orm["return_days"] = None # Changed from time_period_return
            # grace_period_return is already not in policy_data_for_orm

        # Upsert logic
        if csv_policy_id is not None:
            db_policy = db_session.query(ReturnPolicyOrm).filter_by(
                id=csv_policy_id,
                business_details_id=business_details_id # Ensure it belongs to this business
            ).first()

        if db_policy:  # Existing policy, update it
            logger.info(f"Updating existing return policy ID '{csv_policy_id}' for business {business_details_id}")
            for key, value in policy_data_for_orm.items():
                setattr(db_policy, key, value)
            # db_policy.updated_date = datetime.utcnow() # Handled by onupdate=func.now() in ORM
            db_pk = db_policy.id
        else:  # New policy or ID from CSV not found (treat as new)
            if csv_policy_id is not None:
                logger.warning(f"Return policy ID '{csv_policy_id}' provided in CSV not found for business {business_details_id}. Creating as new.")

            new_policy_orm = ReturnPolicyOrm(**policy_data_for_orm)
            db_session.add(new_policy_orm)
            db_session.flush()

            if new_policy_orm.id is None:
                logger.error(f"Failed to obtain DB ID for new return policy after flush. Name: {policy_data_for_orm.get('policy_name')}")
                return None
            db_pk = new_policy_orm.id
            logger.info(f"Created new return policy with DB ID {db_pk}. Name: {policy_data_for_orm.get('policy_name')}")

        return db_pk

    except Exception as e:
        logger.error(f"Error processing return policy record for business {business_details_id}: {record_data} - {e}", exc_info=True)
        return None

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
        # Pydantic model PriceCsv has already done primary validation.
        # Here we focus on DB interaction and foreign key integrity.
        price_type_csv_enum = PriceCsvTypeEnum(record_data["price_type"]) # Convert string to Enum

        target_product_id: Optional[int] = None
        target_sku_id: Optional[int] = None

        if price_type_csv_enum == PriceCsvTypeEnum.PRODUCT:
            csv_product_id_str = record_data.get("product_id")
            if not csv_product_id_str:
                logger.error(f"Price load: product_id missing for PRODUCT type. Business: {business_details_id}, Data: {record_data}")
                return None
            # Assuming product_id from CSV is the DB PK of the product.
            # Product IDs in ProductOrm are BigInteger.
            try:
                # ProductOrm.id is BigInteger, PriceOrm.product_id is BigInteger
                target_product_id = int(csv_product_id_str)
            except ValueError:
                logger.error(f"Price load: Invalid product_id format '{csv_product_id_str}'. Must be integer. Business: {business_details_id}")
                return None

            # Verify product exists and belongs to this business
            product_check = db_session.query(ProductOrm.id).filter(
                ProductOrm.id == target_product_id,
                ProductOrm.business_details_id == business_details_id # business_details_id is BigInteger
            ).first()
            if not product_check:
                logger.warning(f"Price load: Product ID {target_product_id} not found or not associated with business {business_details_id}.")
                return None

        elif price_type_csv_enum == PriceCsvTypeEnum.SKU:
            csv_sku_id_str = record_data.get("sku_id")
            if not csv_sku_id_str:
                logger.error(f"Price load: sku_id missing for SKU type. Business: {business_details_id}, Data: {record_data}")
                return None
            # SKU IDs in ProductItemOrm are Integer. PriceOrm.sku_id is Integer.
            try:
                target_sku_id = int(csv_sku_id_str)
            except ValueError:
                logger.error(f"Price load: Invalid sku_id format '{csv_sku_id_str}'. Must be integer. Business: {business_details_id}")
                return None

            # Verify SKU exists and belongs to this business
            sku_check = db_session.query(ProductItemOrm.id).filter(
                ProductItemOrm.id == target_sku_id,
                ProductItemOrm.business_details_id == business_details_id # business_details_id is BigInteger
            ).first()
            if not sku_check:
                logger.warning(f"Price load: SKU ID {target_sku_id} not found or not associated with business {business_details_id}.")
                return None
        else:
            # This case should ideally be caught by Pydantic validation if PriceTypeEnum was used there.
            logger.error(f"Price load: Unknown price_type '{record_data['price_type']}'. Business: {business_details_id}")
            return None

        # Upsert logic for PriceOrm
        existing_price_query = db_session.query(PriceOrm).filter(
            PriceOrm.business_details_id == business_details_id
        )
        if target_product_id:
            existing_price_query = existing_price_query.filter(PriceOrm.product_id == target_product_id)
        elif target_sku_id:
            existing_price_query = existing_price_query.filter(PriceOrm.sku_id == target_sku_id)

        existing_price: Optional[PriceOrm] = existing_price_query.first()

        price_orm_instance: PriceOrm
        if existing_price:
            logger.info(f"Updating price for business {business_details_id}, {'product ' + str(target_product_id) if target_product_id else 'SKU ' + str(target_sku_id)}")
            existing_price.price = record_data["price"]
            existing_price.discount_price = record_data.get("discount_price")
            existing_price.cost_price = record_data.get("cost_price")
            existing_price.currency = record_data.get("currency", "USD") # Default if not provided
            # updated_at is handled by ORM default onupdate
            price_orm_instance = existing_price
        else:
            logger.info(f"Creating new price for business {business_details_id}, {'product ' + str(target_product_id) if target_product_id else 'SKU ' + str(target_sku_id)}")
            new_price_data = {
                "business_details_id": business_details_id,
                "product_id": target_product_id,
                "sku_id": target_sku_id,
                "price": record_data["price"],
                "discount_price": record_data.get("discount_price"),
                "cost_price": record_data.get("cost_price"),
                "currency": record_data.get("currency", "USD"),
                # created_at and updated_at handled by ORM server_default
            }
            price_orm_instance = PriceOrm(**new_price_data)
            db_session.add(price_orm_instance)

        db_session.flush() # To get ID if new, or ensure instance is tracked for commit.
        if price_orm_instance.id is None:
             logger.error(f"Failed to obtain DB ID for price record after flush. Data: {record_data}")
             raise Exception("DB flush failed to return an ID for price record.")

        return price_orm_instance.id

    except ValueError as ve: # Catch specific value errors e.g. from ID conversion
        logger.error(f"Value error processing price record for business {business_details_id}: {record_data} - {ve}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Generic error processing price record for business {business_details_id}: {record_data} - {e}", exc_info=True)
        # Do not rollback here; let the calling task manage the transaction.
        return None

# ... other loader functions for products, attributes, etc. ...
