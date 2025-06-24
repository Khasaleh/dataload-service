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
from sqlalchemy.exc import IntegrityError # Added for bulk operations

# Import ORM models as they are needed by specific loader functions.
from app.db.models import (
    CategoryOrm,
    BrandOrm,
    AttributeOrm,
    AttributeValueOrm,
    ReturnPolicyOrm,
    PriceOrm,
    ProductOrm,
    ProductItemOrm,
)
from datetime import datetime
from app.dataload.models.price_csv import PriceCsv, PriceTypeEnum as PriceCsvTypeEnum

from app.utils.redis_utils import add_to_id_map, get_from_id_map, DB_PK_MAP_SUFFIX
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType

logger = logging.getLogger(__name__)


def load_category_to_db(
    db_session: Session,
    business_details_id: int,
    record_data: Dict[str, Any],
    session_id: str,
    db_pk_redis_pipeline: Any
) -> Optional[int]:
    category_path_str = record_data.get("category_path")
    if not category_path_str:
        # This should be caught by Pydantic if mandatory. Raising here defensively.
        msg = "Missing 'category_path' in record_data."
        logger.error(f"{msg} Business: {business_details_id}, Session: {session_id}, Record: {record_data}")
        raise DataLoaderError(message=msg, error_type=ErrorType.VALIDATION, field_name="category_path")

    path_levels = [level.strip() for level in category_path_str.split('/') if level.strip()]
    if not path_levels:
        msg = f"Empty or invalid 'category_path' after splitting: '{category_path_str}'."
        logger.error(f"{msg} Record: {record_data}")
        raise DataLoaderError(message=msg, error_type=ErrorType.VALIDATION, field_name="category_path", offending_value=category_path_str)

    current_parent_db_id: Optional[int] = None
    current_full_path_processed = ""
    final_category_db_id: Optional[int] = None

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
                        existing_category_orm.url = record_data.get("url", existing_category_orm.url)
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
                elif category_db_id is not None:
                     add_to_id_map(session_id, f"categories{DB_PK_MAP_SUFFIX}", current_level_full_path, category_db_id)

            current_parent_db_id = category_db_id
            current_full_path_processed = current_level_full_path
            if is_last_level:
                final_category_db_id = category_db_id

        return final_category_db_id

    except DataLoaderError:
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
    records_data: List[Dict[str, Any]],
    session_id: str,
    db_pk_redis_pipeline: Any
) -> Dict[str, int]:
    if not records_data:
        return {"inserted": 0, "updated": 0, "errors": 0}

    brand_names_from_csv = {record['name'] for record in records_data if record.get('name')}
    if not brand_names_from_csv:
        raise DataLoaderError(message="No brand names found in records_data list.", error_type=ErrorType.VALIDATION)

    summary = {"inserted": 0, "updated": 0, "errors": 0}

    try:
        existing_brands_query = db_session.query(BrandOrm).filter(
            BrandOrm.business_details_id == business_details_id,
            BrandOrm.name.in_(brand_names_from_csv)
        )
        existing_brands_map = {brand.name: brand for brand in existing_brands_query.all()}

        new_brands_mappings = []
        updated_brands_mappings = []
        processed_brand_names_for_redis = {}

        for record in records_data:
            brand_name = record.get("name")
            if not brand_name:
                logger.warning(f"Skipping record due to missing brand name: {record}")
                summary["errors"] += 1
                continue
            db_brand = existing_brands_map.get(brand_name)
            if db_brand:
                update_mapping = record.copy()
                update_mapping['id'] = db_brand.id
                updated_brands_mappings.append(update_mapping)
                processed_brand_names_for_redis[brand_name] = db_brand.id
                summary["updated"] += 1
            else:
                insert_mapping = record.copy()
                insert_mapping['business_details_id'] = business_details_id
                new_brands_mappings.append(insert_mapping)
        if updated_brands_mappings:
            logger.info(f"Bulk updating {len(updated_brands_mappings)} brands for business {business_details_id}.")
            db_session.bulk_update_mappings(BrandOrm, updated_brands_mappings)
        if new_brands_mappings:
            logger.info(f"Bulk inserting {len(new_brands_mappings)} new brands for business {business_details_id}.")
            db_session.bulk_insert_mappings(BrandOrm, new_brands_mappings)
            summary["inserted"] = len(new_brands_mappings)
            if summary["inserted"] > 0:
                db_session.flush()
                for brand_mapping_dict in new_brands_mappings: # Attempt to get IDs
                    if 'id' in brand_mapping_dict and brand_mapping_dict['id'] is not None:
                         processed_brand_names_for_redis[brand_mapping_dict['name']] = brand_mapping_dict['id']
                    else:
                        logger.warning(f"ID not populated after bulk insert for brand: {brand_mapping_dict['name']}. Redis map might be incomplete for this new brand.")
                        summary["errors"] += 1
        if processed_brand_names_for_redis:
            redis_pipe_to_use = db_pk_redis_pipeline if db_pk_redis_pipeline else db_session.get_bind().pool._redis_client.pipeline() if hasattr(db_session.get_bind().pool, '_redis_client') else None # Simplified
            for brand_name, brand_id in processed_brand_names_for_redis.items():
                add_to_id_map(session_id, f"brands{DB_PK_MAP_SUFFIX}", brand_name, brand_id, pipeline=redis_pipe_to_use)
            if redis_pipe_to_use and not db_pk_redis_pipeline : redis_pipe_to_use.execute()


        return summary
    except IntegrityError as e:
        logger.error(f"Bulk database integrity error processing brands for business {business_details_id}: {e.orig}", exc_info=False)
        raise DataLoaderError(message=f"Bulk database integrity error for brands: {str(e.orig)}",error_type=ErrorType.DATABASE,original_exception=e)
    except Exception as e:
        logger.error(f"Unexpected error during bulk processing of brands for business {business_details_id}: {e}", exc_info=True)
        raise DataLoaderError(message=f"Unexpected error during bulk brand processing: {str(e)}", error_type=ErrorType.UNEXPECTED_ROW_ERROR, original_exception=e)

def load_attribute_to_db(
    db_session: Session,
    business_details_id: int,
    record_data: Dict[str, Any],
    session_id: str,
    db_pk_redis_pipeline: Any
) -> Optional[int]:
    parent_attribute_name = record_data.get("attribute_name")
    if not parent_attribute_name:
        msg = "Missing 'attribute_name' in record_data."
        logger.error(f"{msg} Business: {business_details_id}, Session: {session_id}, Record: {record_data}")
        raise DataLoaderError(message=msg, error_type=ErrorType.VALIDATION, field_name="attribute_name")

    attribute_db_id: Optional[int] = None
    parent_attr_orm_instance: Optional[AttributeOrm] = None

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
            parent_attr_orm_instance = AttributeOrm(**parent_orm_data)
            db_session.add(parent_attr_orm_instance)
            db_session.flush()
            if parent_attr_orm_instance.id is None:
                msg = f"DB flush failed to return an ID for new attribute '{parent_attribute_name}'."
                logger.error(msg)
                raise DataLoaderError(message=msg, error_type=ErrorType.DATABASE, field_name="attribute_name", offending_value=parent_attribute_name)
            attribute_db_id = parent_attr_orm_instance.id
            logger.info(f"Created new attribute '{parent_attribute_name}' with DB ID {attribute_db_id}")

        if attribute_db_id is not None:
            redis_pipe_to_use = db_pk_redis_pipeline if db_pk_redis_pipeline else db_session.get_bind().pool._redis_client.pipeline() if hasattr(db_session.get_bind().pool, '_redis_client') else None # Simplified
            add_to_id_map(session_id, f"attributes{DB_PK_MAP_SUFFIX}", parent_attribute_name, attribute_db_id, pipeline=redis_pipe_to_use)
            if redis_pipe_to_use and not db_pk_redis_pipeline : redis_pipe_to_use.execute()


        values_name_str = record_data.get("values_name")
        if values_name_str and attribute_db_id is not None and parent_attr_orm_instance is not None:
            value_display_names = [name.strip() for name in values_name_str.split('|') if name.strip()]
            raw_value_values = record_data.get("value_value")
            value_actual_values = [v.strip() for v in raw_value_values.split('|')] if raw_value_values else []
            raw_img_urls = record_data.get("img_url")
            value_image_urls = [img.strip() for img in raw_img_urls.split('|')] if raw_img_urls else []
            raw_values_active = record_data.get("values_active")
            value_active_statuses = [status.strip().upper() for status in raw_values_active.split('|')] if raw_values_active else []

            for i, val_display_name in enumerate(value_display_names):
                actual_value_part = value_actual_values[i] if i < len(value_actual_values) else None
                image_url_part = value_image_urls[i] if i < len(value_image_urls) else None
                active_status_part = value_active_statuses[i] if i < len(value_active_statuses) and value_active_statuses[i] in ["ACTIVE", "INACTIVE"] else "INACTIVE"
                value_for_db = actual_value_part if parent_attr_orm_instance.is_color and actual_value_part else val_display_name

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
    except DataLoaderError:
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
    business_details_id: int,
    records_data: List[Dict[str, Any]],
    session_id: str,
    db_pk_redis_pipeline: Any
) -> Dict[str, int]:
    if not records_data:
        return {"inserted": 0, "updated": 0, "errors": 0}

    summary = {"inserted": 0, "updated": 0, "errors": 0}

    processed_records_for_bulk = []
    for record in records_data:
        processed_record = record.copy()
        if record.get("return_policy_type") == "SALES_ARE_FINAL":
            processed_record["policy_name"] = None
            processed_record["time_period_return"] = None
        if "time_period_return" in processed_record: # Map to ORM field name
            processed_record["return_days"] = processed_record.pop("time_period_return")
        if "created_date" in processed_record:
            processed_record["created_date_ts"] = processed_record.pop("created_date")
        if "updated_date" in processed_record:
            processed_record["updated_date_ts"] = processed_record.pop("updated_date")
        processed_records_for_bulk.append(processed_record)

    records_with_id_in_csv = [r for r in processed_records_for_bulk if r.get("id") is not None]
    records_without_id_in_csv = [r for r in processed_records_for_bulk if r.get("id") is None]

    updates_list_of_dicts = []
    inserts_list_of_dicts = []

    try:
        if records_with_id_in_csv:
            ids_from_csv = [r['id'] for r in records_with_id_in_csv]
            existing_policies_by_id_query = db_session.query(ReturnPolicyOrm).filter(
                ReturnPolicyOrm.business_details_id == business_details_id,
                ReturnPolicyOrm.id.in_(ids_from_csv)
            )
            existing_policies_by_id_map = {p.id: p for p in existing_policies_by_id_query.all()}

            for record in records_with_id_in_csv:
                csv_id = record['id']
                if csv_id in existing_policies_by_id_map:
                    update_data = record.copy()
                    updates_list_of_dicts.append(update_data)
                else:
                    logger.warning(f"Return policy ID '{csv_id}' provided in CSV not found for business {business_details_id}. Will attempt to process as new if name matches or insert.")
                    # Add to records_without_id_in_csv to be processed by name or as new insert
                    records_without_id_in_csv.append(record)
                    summary["errors"] +=1 # Consider this an error or a specific handling case

        if records_without_id_in_csv:
            policy_names_to_check = {r['policy_name'] for r in records_without_id_in_csv if r.get('policy_name')}
            existing_policies_by_name_map = {}
            if policy_names_to_check:
                existing_policies_by_name_query = db_session.query(ReturnPolicyOrm).filter(
                    ReturnPolicyOrm.business_details_id == business_details_id,
                    ReturnPolicyOrm.policy_name.in_(policy_names_to_check)
                )
                existing_policies_by_name_map = {p.policy_name: p for p in existing_policies_by_name_query.all()}

            for record in records_without_id_in_csv:
                policy_name = record.get("policy_name")
                existing_policy_by_name = existing_policies_by_name_map.get(policy_name) if policy_name else None

                is_already_targeted_for_update_by_id = any(u['id'] == record.get('id') for u in updates_list_of_dicts if record.get('id') is not None)

                if existing_policy_by_name and not is_already_targeted_for_update_by_id:
                    update_data = record.copy()
                    update_data['id'] = existing_policy_by_name.id
                    # Avoid adding duplicate updates if already matched by ID
                    if not any(u['id'] == update_data['id'] for u in updates_list_of_dicts):
                        updates_list_of_dicts.append(update_data)
                elif not is_already_targeted_for_update_by_id :
                    insert_data = record.copy()
                    insert_data['business_details_id'] = business_details_id
                    if 'id' in insert_data: del insert_data['id']
                    inserts_list_of_dicts.append(insert_data)

        if updates_list_of_dicts:
            logger.info(f"Bulk updating {len(updates_list_of_dicts)} return policies for business {business_details_id}.")
            db_session.bulk_update_mappings(ReturnPolicyOrm, updates_list_of_dicts)
            summary["updated"] = len(updates_list_of_dicts)

        if inserts_list_of_dicts:
            logger.info(f"Bulk inserting {len(inserts_list_of_dicts)} new return policies for business {business_details_id}.")
            db_session.bulk_insert_mappings(ReturnPolicyOrm, inserts_list_of_dicts)
            summary["inserted"] = len(inserts_list_of_dicts)
            # No need to fetch IDs back as these are not typically mapped by name in Redis for FKs

        return summary

    except IntegrityError as e:
        logger.error(f"Bulk database integrity error processing return policies for business {business_details_id}: {e.orig}", exc_info=False)
        raise DataLoaderError(message=f"Bulk database integrity error for return policies: {str(e.orig)}", error_type=ErrorType.DATABASE, original_exception=e)
    except Exception as e:
        logger.error(f"Unexpected error during bulk processing of return policies for business {business_details_id}: {e}", exc_info=True)
        raise DataLoaderError(message=f"Unexpected error during bulk return policy processing: {str(e)}", error_type=ErrorType.UNEXPECTED_ROW_ERROR, original_exception=e)

def load_price_to_db(
    db_session: Session,
    business_details_id: int,
    record_data: Dict[str, Any],
    session_id: str,
    db_pk_redis_pipeline: Any
) -> Optional[int]:
    try:
        price_type_csv_enum = PriceCsvTypeEnum(record_data["price_type"])
        target_product_id: Optional[int] = None
        target_sku_id: Optional[int] = None
        target_id_field_name = "product_id"

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

    except DataLoaderError:
        raise
    except ValueError as ve:
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
