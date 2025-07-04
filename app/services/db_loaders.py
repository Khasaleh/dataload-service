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
from sqlalchemy.exc import IntegrityError, DataError # Added DataError
from app.utils.date_utils import ServerDateTime
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
    except IntegrityError as e:
        logger.error(f"DB integrity error processing category path '{category_path_str}': {e.orig}", exc_info=False)
        raise DataLoaderError(
            message=f"Database integrity error for category path '{category_path_str}': {str(e.orig)}",
            error_type=ErrorType.DATABASE,
            field_name="category_path",
            offending_value=category_path_str,
            original_exception=e
        )
    except DataError as e:
        logger.error(f"DB data error processing category path '{category_path_str}': {e.orig}", exc_info=False)
        raise DataLoaderError(
            message=f"Database data error for category path '{category_path_str}': {str(e.orig)}", # e.g. value too long for column
            error_type=ErrorType.DATABASE, # Could also be VALIDATION if data is simply wrong type/length
            field_name="category_path", # Or more specific if determinable
            offending_value=category_path_str, # Or specific field's value if known
            original_exception=e
        )
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
    db_pk_redis_pipeline: Any = None,
    user_id: int = None
) -> Dict[str, int]:
    if not records_data:
        return {"inserted": 0, "updated": 0, "errors": 0}

    summary = {"inserted": 0, "updated": 0, "errors": 0}
    ts_now = ServerDateTime.now_epoch_ms()

    # normalize flags and attach business_details_id
    for rec in records_data:
        rec["business_details_id"] = business_details_id
        flag = str(rec.get("active", "")).strip().upper()
        rec["active"] = "ACTIVE" if flag in ("TRUE", "1", "ACTIVE") else "INACTIVE"

    # fetch existing by name
    names = [r["name"] for r in records_data if r.get("name")]
    existing = db_session.query(BrandOrm).filter(
        BrandOrm.business_details_id == business_details_id,
        BrandOrm.name.in_(names)
    ).all()
    existing_map = {b.name: b for b in existing}

    to_insert, to_update = [], []

    for rec in records_data:
        name = rec.get("name")
        if not name:
            summary["errors"] += 1
            continue

        if name in existing_map:
            # update only updated_* fields
            upd = {
                "id": existing_map[name].id,
                "updated_by": user_id,
                "updated_date": ts_now,
                # any other columns from CSV to update:
                "logo": rec.get("logo"),
                "supplier_id": rec.get("supplier_id"),
                "active": rec["active"],
            }
            to_update.append(upd)
            summary["updated"] += 1
        else:
            # insert with both created_* and updated_*
            ins = {
                "business_details_id": business_details_id,
                "name": name,
                "logo": rec.get("logo"),
                "supplier_id": rec.get("supplier_id"),
                "active": rec["active"],
                "created_by": user_id,
                "created_date": ts_now,
                "updated_by": user_id,
                "updated_date": ts_now,
            }
            to_insert.append(ins)
            summary["inserted"] += 1

    try:
        if to_update:
            db_session.bulk_update_mappings(BrandOrm, to_update)
        if to_insert:
            db_session.bulk_insert_mappings(BrandOrm, to_insert)
            db_session.flush()
            # register new IDs in Redis
            for ins in to_insert:
                bid = ins.get("id")
                if bid:
                    add_to_id_map(
                        session_id,
                        f"brands{DB_PK_MAP_SUFFIX}",
                        ins["name"],
                        bid,
                        pipeline=db_pk_redis_pipeline
                    )
    except IntegrityError as e:
        raise DataLoaderError(
            message=f"Database integrity error for brands: {e.orig}",
            error_type=ErrorType.DATABASE,
            original_exception=e
        )

    return summary

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
    except IntegrityError as e:
        logger.error(f"DB integrity error processing attribute '{parent_attribute_name}': {e.orig}", exc_info=False)
        raise DataLoaderError(
            message=f"Database integrity error for attribute '{parent_attribute_name}' or its values: {str(e.orig)}",
            error_type=ErrorType.DATABASE,
            field_name="attribute_name", # Or specific value if parsable from e.orig
            offending_value=parent_attribute_name,
            original_exception=e
        )
    except DataError as e:
        logger.error(f"DB data error processing attribute '{parent_attribute_name}': {e.orig}", exc_info=False)
        raise DataLoaderError(
            message=f"Database data error for attribute '{parent_attribute_name}' or its values: {str(e.orig)}",
            error_type=ErrorType.DATABASE,
            field_name="attribute_name", # Or specific value field
            offending_value=parent_attribute_name, # Or specific problematic value
            original_exception=e
        )
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
        if "time_period_return" in processed_record:
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
                    records_without_id_in_csv.append(record)
                    summary["errors"] +=1

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
    records_data: List[Dict[str, Any]],
    session_id: str,
    db_pk_redis_pipeline: Any
) -> Dict[str, Any]: # Returns a summary like {"inserted": count, "updated": count, "error_details": List[ErrorDetailModel]}
    if not records_data:
        return {"inserted": 0, "updated": 0, "errors_list": []} # Changed "errors" to "errors_list"

    summary = {"inserted": 0, "updated": 0} # errors will be the list of ErrorDetailModel
    error_details_list: List[ErrorDetailModel] = []

    product_price_records_data = []
    sku_price_records_data = []

    # Initial pass to categorize and validate price_type, and ensure target_id presence
    for i, record in enumerate(records_data):
        row_num_for_error = i + 2 # Assuming CSV row 2 is the first data row
        try:
            price_type_str = record.get("price_type")
            if not price_type_str:
                raise ValueError("price_type is missing.")
            price_type = PriceCsvTypeEnum(price_type_str) # Validate enum value

            if price_type == PriceCsvTypeEnum.PRODUCT:
                if not record.get("product_id"):
                    raise ValueError("product_id is required for PRODUCT price type.")
                product_price_records_data.append(record)
            elif price_type == PriceCsvTypeEnum.SKU:
                if not record.get("sku_id"):
                    raise ValueError("sku_id is required for SKU price type.")
                sku_price_records_data.append(record)
        except ValueError as ve:
            logger.warning(f"Skipping price record (row approx {row_num_for_error}) due to validation error: {ve}. Record: {record}")
            error_details_list.append(ErrorDetailModel(
                row_number=row_num_for_error,
                field_name="price_type" if "price_type" in str(ve).lower() else ("product_id" if "product_id" in str(ve).lower() else "sku_id"),
                error_message=str(ve),
                error_type=ErrorType.VALIDATION,
                offending_value=str(record.get("price_type") or record.get("product_id") or record.get("sku_id"))
            ))
            continue

    all_inserts = []
    all_updates = []

    try:
        # Process Product Prices
        if product_price_records_data:
            product_ids_from_csv = {int(r['product_id']) for r in product_price_records_data if r.get('product_id') and r['product_id'].isdigit()}

            valid_db_product_ids = {
                pid[0] for pid in db_session.query(ProductOrm.id).filter(
                    ProductOrm.business_details_id == business_details_id,
                    ProductOrm.id.in_(product_ids_from_csv)
                ).all()
            }

            existing_prices_for_products_map = {
                p.product_id: p for p in db_session.query(PriceOrm).filter(
                    PriceOrm.business_details_id == business_details_id,
                    PriceOrm.product_id.in_(valid_db_product_ids)
                ).all()
            }

            for i, record in enumerate(product_price_records_data): # Use original index for row number if needed
                row_num_for_error = records_data.index(record) + 2 if record in records_data else None # Approx row num
                try:
                    product_id_str = record['product_id']
                    product_id = int(product_id_str)
                    if product_id not in valid_db_product_ids:
                        msg = f"Product ID {product_id} not found or not associated with business {business_details_id}."
                        logger.warning(f"{msg} Skipping price record: {record}")
                        error_details_list.append(ErrorDetailModel(row_number=row_num_for_error, field_name="product_id", error_message=msg, error_type=ErrorType.LOOKUP, offending_value=product_id_str))
                        continue

                    orm_data = {
                        "business_details_id": business_details_id, "product_id": product_id, "sku_id": None,
                        "price": record["price"], "discount_price": record.get("discount_price"),
                        "cost_price": record.get("cost_price"), "currency": record.get("currency", "USD"),
                    }
                    existing_price = existing_prices_for_products_map.get(product_id)
                    if existing_price:
                        orm_data['id'] = existing_price.id
                        all_updates.append(orm_data)
                    else:
                        all_inserts.append(orm_data)
                except ValueError:
                    msg = f"Invalid product_id format '{record.get('product_id')}' in record."
                    logger.warning(f"{msg} Skipping: {record}")
                    error_details_list.append(ErrorDetailModel(row_number=row_num_for_error, field_name="product_id", error_message=msg, error_type=ErrorType.VALIDATION, offending_value=record.get('product_id')))


        # Process SKU Prices
        if sku_price_records_data:
            sku_ids_from_csv = {int(r['sku_id']) for r in sku_price_records_data if r.get('sku_id') and r['sku_id'].isdigit()}
            valid_db_sku_ids = {
                sid[0] for sid in db_session.query(ProductItemOrm.id).filter(
                    ProductItemOrm.business_details_id == business_details_id,
                    ProductItemOrm.id.in_(sku_ids_from_csv)
                ).all()
            }
            existing_prices_for_skus_map = {
                p.sku_id: p for p in db_session.query(PriceOrm).filter(
                    PriceOrm.business_details_id == business_details_id,
                    PriceOrm.sku_id.in_(valid_db_sku_ids)
                ).all()
            }
            for i, record in enumerate(sku_price_records_data):
                row_num_for_error = records_data.index(record) + 2 if record in records_data else None
                try:
                    sku_id_str = record['sku_id']
                    sku_id = int(sku_id_str)
                    if sku_id not in valid_db_sku_ids:
                        msg = f"SKU ID {sku_id} not found or not associated with business {business_details_id}."
                        logger.warning(f"{msg} Skipping price record: {record}")
                        error_details_list.append(ErrorDetailModel(row_number=row_num_for_error, field_name="sku_id", error_message=msg, error_type=ErrorType.LOOKUP, offending_value=sku_id_str))
                        continue

                    orm_data = {
                        "business_details_id": business_details_id, "product_id": None, "sku_id": sku_id,
                        "price": record["price"], "discount_price": record.get("discount_price"),
                        "cost_price": record.get("cost_price"), "currency": record.get("currency", "USD"),
                    }
                    existing_price = existing_prices_for_skus_map.get(sku_id)
                    if existing_price:
                        orm_data['id'] = existing_price.id
                        all_updates.append(orm_data)
                    else:
                        all_inserts.append(orm_data)
                except ValueError:
                    msg = f"Invalid sku_id format '{record.get('sku_id')}' in record."
                    logger.warning(f"{msg} Skipping: {record}")
                    error_details_list.append(ErrorDetailModel(row_number=row_num_for_error, field_name="sku_id", error_message=msg, error_type=ErrorType.VALIDATION, offending_value=record.get('sku_id')))

        if all_updates:
            logger.info(f"Bulk updating {len(all_updates)} prices for business {business_details_id}.")
            db_session.bulk_update_mappings(PriceOrm, all_updates)
            summary["updated"] = len(all_updates)

        if all_inserts:
            logger.info(f"Bulk inserting {len(all_inserts)} new prices for business {business_details_id}.")
            db_session.bulk_insert_mappings(PriceOrm, all_inserts)
            summary["inserted"] = len(all_inserts)

        summary["errors_list"] = error_details_list # Attach collected pre-check errors
        return summary

    except IntegrityError as e:
        logger.error(f"Bulk database integrity error processing prices for business {business_details_id}: {e.orig}", exc_info=False)
        # This error applies to the batch; specific row is hard to determine from bulk error.
        # Add the collected pre-check errors to the exception if any.
        raise DataLoaderError(message=f"Bulk database integrity error for prices: {str(e.orig)}", error_type=ErrorType.DATABASE, original_exception=e) # errors_list can be passed to process_csv_task to merge
    except Exception as e:
        logger.error(f"Unexpected error during bulk processing of prices for business {business_details_id}: {e}", exc_info=True)
        raise DataLoaderError(message=f"Unexpected error during bulk price processing: {str(e)}", error_type=ErrorType.UNEXPECTED_ROW_ERROR, original_exception=e)
