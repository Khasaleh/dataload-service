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
from app.utils.slug import generate_slug
from app.utils.redis_utils import add_to_id_map, get_from_id_map, DB_PK_MAP_SUFFIX
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType

logger = logging.getLogger(__name__)

def load_category_to_db(
    db_session,
    business_details_id: int,
    record_data: Dict[str, Any],
    session_id: str,
    db_pk_redis_pipeline: Any = None,
    user_id: int = None
) -> int:
    """
    Upsert a hierarchical category path (e.g. "Electronics/Computers/Laptops").
    - New rows: set created_by/created_date, updated_by/updated_date, business_details_id, enabled, active, url slug.
    - If CSV explicitly provides order_type or shipping_type (even blank), convert blank→NULL; if omitted, leave None.
    - Existing leaf: update only mutable fields + updated_by/updated_date.
    Returns the final category ID.
    """
    path = record_data.get("category_path", "").strip()
    if not path:
        raise DataLoaderError(
            message="Missing or empty 'category_path'",
            error_type=ErrorType.VALIDATION,
            field_name="category_path"
        )

    # helpers
    def _bool(val: Any) -> bool:
        return str(val or "").strip().lower() in ("true", "1", "yes")
    def _active(val: Any) -> str:
        return "INACTIVE" if isinstance(val, str) and val.strip().lower()=="inactive" else "ACTIVE"

    # extract & normalize CSV fields
    name             = record_data["name"].strip()
    description      = record_data.get("description")
    enabled          = _bool(record_data.get("enabled", True))
    image_name       = record_data.get("image_name")
    long_description = record_data.get("long_description")
    order_type_raw   = record_data.get("order_type")   # may be absent
    order_type       = order_type_raw.strip() if order_type_raw is not None and order_type_raw.strip() else None
    shipping_raw     = record_data.get("shipping_type")
    shipping_type    = shipping_raw.strip() if shipping_raw is not None and shipping_raw.strip() else None
    active_flag      = _active(record_data.get("active"))
    seo_description  = record_data.get("seo_description")
    seo_keywords     = record_data.get("seo_keywords")
    seo_title        = record_data.get("seo_title")
    position         = record_data.get("position_on_site")
    url              = record_data.get("url") or f"/{ServerDateTime.now_epoch_ms()}"  # fallback, but CSV-model should fill

    segments = [seg.strip() for seg in path.split("/") if seg.strip()]
    parent_id: Optional[int] = None
    full_path = ""
    final_id: Optional[int] = None

    try:
        for idx, seg in enumerate(segments):
            full_path = f"{full_path}/{seg}" if full_path else seg
            is_leaf = (idx == len(segments)-1)

            # lookup existing at this level
            orm_name = name if is_leaf else seg
            cat = (
                db_session.query(CategoryOrm)
                          .filter_by(
                              business_details_id=business_details_id,
                              parent_id=parent_id,
                              name=orm_name
                          )
                          .first()
            )

            if cat:
                # update only leaf‐level metadata
                if is_leaf:
                    logger.info(f"Updating category '{full_path}' (ID={cat.id})")
                    cat.description      = description      or cat.description
                    cat.enabled          = enabled
                    cat.image_name       = image_name       or cat.image_name
                    cat.long_description = long_description or cat.long_description
                    if "order_type" in record_data:
                        cat.order_type    = order_type
                    if "shipping_type" in record_data:
                        cat.shipping_type = shipping_type
                    cat.active           = active_flag
                    cat.seo_description  = seo_description  or cat.seo_description
                    cat.seo_keywords     = seo_keywords     or cat.seo_keywords
                    cat.seo_title        = seo_title        or cat.seo_title
                    cat.url              = url              or cat.url
                    cat.position_on_site = position or cat.position_on_site
                    cat.updated_by       = user_id
                    cat.updated_date     = ServerDateTime.now_epoch_ms()
                final_id = cat.id

            else:
                # create new
                now = ServerDateTime.now_epoch_ms()
                payload: Dict[str, Any] = {
                    "business_details_id": business_details_id,
                    "parent_id": parent_id,
                    "name": orm_name,
                    "created_by": user_id,
                    "created_date": now,
                    "updated_by": user_id,
                    "updated_date": now,
                    "enabled": enabled if is_leaf else True,
                    "active": active_flag,
                    "description": description if is_leaf else seg,
                    "url": url,
                }
                if is_leaf:
                    payload.update({
                        "image_name":       image_name,
                        "long_description": long_description,
                        "order_type":       order_type,
                        "shipping_type":    shipping_type,
                        "seo_description":  seo_description,
                        "seo_keywords":     seo_keywords,
                        "seo_title":        seo_title,
                        "position_on_site": position,
                    })
                new_cat = CategoryOrm(**payload)
                db_session.add(new_cat)
                db_session.flush()
                final_id = new_cat.id
                logger.info(f"Created category '{full_path}' (ID={final_id})")

            # cache in Redis
            add_to_id_map(
                session_id,
                f"categories{DB_PK_MAP_SUFFIX}",
                full_path,
                final_id,
                pipeline=db_pk_redis_pipeline
            )

            parent_id = final_id  # type: ignore

        return final_id  # type: ignore

    except IntegrityError as e:
        logger.error(f"DB integrity error for '{path}': {e.orig}")
        raise DataLoaderError(
            message=f"Integrity error for '{path}': {e.orig}",
            error_type=ErrorType.DATABASE,
            field_name="category_path",
            offending_value=path,
            original_exception=e
        )
    except DataError as e:
        logger.error(f"DB data error for '{path}': {e.orig}")
        raise DataLoaderError(
            message=f"Data error for '{path}': {e.orig}",
            error_type=ErrorType.DATABASE,
            field_name="category_path",
            offending_value=path,
            original_exception=e
        )
    except Exception as e:
        logger.exception(f"Unexpected error for '{path}'")
        raise DataLoaderError(
            message=f"Unexpected error for category path '{path}': {str(e)}",
            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
            field_name="category_path",
            offending_value=path,
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
    db_pk_redis_pipeline: Any = None
) -> Optional[int]:
    parent_attribute_name = record_data.get("attribute_name")
    if not parent_attribute_name:
        msg = "Missing 'attribute_name' in record_data."
        logger.error(f"{msg} Business: {business_details_id}, Session: {session_id}, Record: {record_data}")
        raise DataLoaderError(message=msg, error_type=ErrorType.VALIDATION, field_name="attribute_name")

    try:
        # 1) Upsert parent Attribute
        parent = (
            db_session.query(AttributeOrm)
                      .filter_by(business_details_id=business_details_id, name=parent_attribute_name)
                      .first()
        )
        is_color_flag = record_data.get("is_color", False)
        if parent:
            logger.info(f"Updating existing attribute '{parent_attribute_name}' (ID: {parent.id})")
            parent.is_color   = is_color_flag
            parent.active     = record_data.get("attribute_active", parent.active)
            parent.updated_by = record_data.get("updated_by", parent.updated_by)
            parent.updated_date = record_data.get("updated_date", parent.updated_date)
            attribute_db_id = parent.id
        else:
            logger.info(f"Creating new attribute '{parent_attribute_name}'")
            parent = AttributeOrm(
                business_details_id=business_details_id,
                name=parent_attribute_name,
                is_color=is_color_flag,
                active=record_data.get("attribute_active"),
                created_by=record_data.get("created_by"),
                created_date=record_data.get("created_date"),
                updated_by=record_data.get("updated_by", record_data.get("created_by")),
                updated_date=record_data.get("updated_date", record_data.get("created_date")),
            )
            db_session.add(parent)
            db_session.flush()
            if parent.id is None:
                raise DataLoaderError(
                    message=f"Failed to flush new attribute '{parent_attribute_name}'",
                    error_type=ErrorType.DATABASE,
                    field_name="attribute_name",
                    offending_value=parent_attribute_name
                )
            attribute_db_id = parent.id
            logger.info(f"Created attribute '{parent_attribute_name}' with ID {attribute_db_id}")

        # 2) Cache in Redis
        add_to_id_map(
            session_id,
            f"attributes{DB_PK_MAP_SUFFIX}",
            parent_attribute_name,
            attribute_db_id,
            pipeline=db_pk_redis_pipeline
        )

        # 3) Process values (zip‐longest semantics)
        values_name_str  = record_data.get("values_name")  or ""
        values_value_str = record_data.get("value_value")  or ""
        img_url_str      = record_data.get("img_url")      or ""
        values_active_str= record_data.get("values_active")or ""

        names = [n.strip() for n in values_name_str.split("|")]
        vals  = [v.strip() for v in values_value_str.split("|")]
        imgs  = [u.strip() for u in img_url_str.split("|")]
        acts  = [s.strip().upper() for s in values_active_str.split("|")]

        max_len = max(len(names), len(vals))
        for i in range(max_len):
            disp = names[i] if i < len(names) and names[i] else (vals[i] if i < len(vals) else None)
            actual = vals[i]  if i < len(vals) and vals[i] else disp
            url   = imgs[i]  if i < len(imgs) and imgs[i] else None
            st    = acts[i]  if i < len(acts) and acts[i] in ("ACTIVE","INACTIVE") else "INACTIVE"

            # choose stored value
            value_for_db = actual if parent.is_color and actual else disp
            if not value_for_db:
                logger.warning(f"Skipping empty attribute‐value at index {i} for '{parent_attribute_name}'")
                continue

            # upsert AttributeValue
            val_orm = (
                db_session.query(AttributeValueOrm)
                          .filter_by(attribute_id=attribute_db_id, name=disp)
                          .first()
            )
            if val_orm:
                logger.debug(f"Updating value '{disp}' for attribute ID {attribute_db_id}")
                val_orm.value               = value_for_db
                val_orm.attribute_image_url = url or val_orm.attribute_image_url
                val_orm.active              = st
                val_orm.updated_by          = record_data.get("updated_by", val_orm.updated_by)
                val_orm.updated_date        = record_data.get("updated_date", val_orm.updated_date)
            else:
                logger.debug(f"Creating value '{disp}' for attribute ID {attribute_db_id}")
                new_val = AttributeValueOrm(
                    attribute_id        = attribute_db_id,
                    name                = disp,
                    value               = value_for_db,
                    attribute_image_url = url,
                    active              = st,
                    created_by          = record_data.get("created_by"),
                    created_date        = record_data.get("created_date"),
                    updated_by          = record_data.get("updated_by", record_data.get("created_by")),
                    updated_date        = record_data.get("updated_date", record_data.get("created_date")),
                )
                db_session.add(new_val)

        return attribute_db_id

    except IntegrityError as e:
        db_session.rollback()
        logger.error(f"Integrity error for attribute '{parent_attribute_name}': {e.orig}")
        raise DataLoaderError(
            message=f"Database integrity error: {e.orig}",
            error_type=ErrorType.DATABASE,
            field_name="attribute_name",
            offending_value=parent_attribute_name,
            original_exception=e
        )
    except DataError as e:
        db_session.rollback()
        logger.error(f"Data error for attribute '{parent_attribute_name}': {e.orig}")
        raise DataLoaderError(
            message=f"Database data error: {e.orig}",
            error_type=ErrorType.DATABASE,
            field_name="attribute_name",
            offending_value=parent_attribute_name,
            original_exception=e
        )
    except DataLoaderError:
        db_session.rollback()
        raise
    except Exception as e:
        db_session.rollback()
        logger.exception(f"Unexpected error processing attribute '{parent_attribute_name}'")
        raise DataLoaderError(
            message=f"Unexpected error: {str(e)}",
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
