import logging
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, NoResultFound, DataError
from sqlalchemy import func

from app.db.models import (
    ProductOrm,
    SkuOrm,
    MainSkuOrm,
    ProductVariantOrm,
    ProductImageOrm,
    AttributeOrm,
    AttributeValueOrm,
)
from app.dataload.models.item_csv import ItemCsvModel
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
    ItemParserError,
    parse_images_string
)
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType
from app.utils.date_utils import now_epoch_ms
from app.utils import barcode_helper

logger = logging.getLogger(__name__)

def _lookup_attribute_ids(db: Session, business_details_id: int, attribute_names: List[str]) -> Dict[str, int]:
    attr_id_map: Dict[str, int] = {}
    missing_attrs = []
    for name in set(attribute_names):
        try:
            attr = db.query(AttributeOrm).filter(
                func.lower(AttributeOrm.name) == func.lower(name),
                AttributeOrm.business_details_id == business_details_id
            ).one()
            attr_id_map[name] = attr.id
        except NoResultFound:
            missing_attrs.append(name)
    if missing_attrs:
        raise DataLoaderError(
            message=f"Attributes not found for business {business_details_id}: {', '.join(missing_attrs)}",
            error_type=ErrorType.LOOKUP,
            field_name="attributes",
            offending_value=str(missing_attrs)
        )
    return attr_id_map

def _lookup_attribute_value_ids(
    db: Session, 
    attr_id_map: Dict[str, int], 
    attr_value_pairs_to_lookup: List[Tuple[str, str]]
) -> Dict[Tuple[str, str], int]:
    attr_val_id_map: Dict[Tuple[str, str], int] = {}
    missing_vals = []
    for attr_name, val_name in set(attr_value_pairs_to_lookup):
        attr_id = attr_id_map.get(attr_name)
        if attr_id is None:
            continue
        try:
            attr_val = db.query(AttributeValueOrm).filter(
                AttributeValueOrm.attribute_id == attr_id,
                func.lower(AttributeValueOrm.name) == func.lower(val_name)
            ).one()
            attr_val_id_map[(attr_name, val_name)] = attr_val.id
        except NoResultFound:
            missing_vals.append(f"{attr_name} -> {val_name}")
    if missing_vals:
        raise DataLoaderError(
            message=f"Attribute values not found: {', '.join(missing_vals)}",
            error_type=ErrorType.LOOKUP,
            field_name="attribute_combination",
            offending_value=str(missing_vals)
        )
    return attr_val_id_map

def find_existing_sku_by_attributes(
    db: Session, 
    product_id: int, 
    target_attribute_value_ids: List[int],
    log_prefix: str = "" 
) -> Optional[SkuOrm]:
    if not target_attribute_value_ids:
        return None

    num_target_attributes = len(target_attribute_value_ids)

    subquery = (
        db.query(ProductVariantOrm.sku_id)
        .filter(ProductVariantOrm.attribute_value_id.in_(target_attribute_value_ids))
        .group_by(ProductVariantOrm.sku_id)
        .having(func.count(ProductVariantOrm.attribute_value_id) == num_target_attributes)
    ).subquery()

    subquery_count = (
        db.query(ProductVariantOrm.sku_id, func.count(ProductVariantOrm.id).label("attributes_count"))
        .group_by(ProductVariantOrm.sku_id)
    ).subquery()

    existing_sku = (
        db.query(SkuOrm)
        .join(subquery, SkuOrm.id == subquery.c.sku_id)
        .join(subquery_count, SkuOrm.id == subquery_count.c.sku_id)
        .filter(SkuOrm.product_id == product_id)
        .filter(subquery_count.c.attributes_count == num_target_attributes)
        .first()
    )
    
    return existing_sku

def load_item_record_to_db(
    db: Session, 
    business_details_id: int, 
    item_csv_row: ItemCsvModel, 
    user_id: int,
) -> List[int]:
    log_prefix = f"[ItemCSV Product: {item_csv_row.product_name}]"
    logger.info(f"{log_prefix} Starting processing of item CSV row.")

    processed_main_sku_ids = []

    try:
        product_orm = db.query(ProductOrm).filter(
            ProductOrm.name == item_csv_row.product_name,
            ProductOrm.business_details_id == business_details_id
        ).one()
        product_id = product_orm.id

        parsed_attributes = parse_attributes_string(item_csv_row.attributes)
        parsed_attribute_values = parse_attribute_combination_string(
            item_csv_row.attribute_combination, parsed_attributes
        )
        all_sku_variants = generate_sku_variants(
            parsed_attribute_values, parsed_attributes
        )

        attr_names = [attr['name'] for attr in parsed_attributes]
        attr_id_map = _lookup_attribute_ids(db, business_details_id, attr_names)

        attr_value_pairs = []
        for variant in all_sku_variants:
            for attr_detail in variant:
                attr_value_pairs.append((attr_detail['attribute_name'], attr_detail['value']))
        
        attr_val_id_map = _lookup_attribute_value_ids(db, attr_id_map, attr_value_pairs)

        first_main_sku_id = None

        for sku_variant in all_sku_variants:
            is_default_sku = any(v.get('is_default_sku_value', False) for v in sku_variant)
            
            main_sku = MainSkuOrm(
                product_id=product_id,
                is_default=is_default_sku,
                active="ACTIVE",
                created_by=user_id,
                updated_by=user_id,
                created_date=now_epoch_ms(),
                updated_date=now_epoch_ms(),
            )
            db.add(main_sku)
            db.flush()

            if first_main_sku_id is None:
                first_main_sku_id = main_sku.id

            sku = SkuOrm(
                product_id=product_id,
                main_sku_id=main_sku.id,
                name=f"{item_csv_row.product_name} - {'/'.join(v['value'] for v in sku_variant)}",
                price=get_price_for_combination(item_csv_row.price, parsed_attributes, parsed_attribute_values, sku_variant),
                quantity=get_quantity_for_combination(item_csv_row.quantity, parsed_attributes, parsed_attribute_values, sku_variant),
                active=get_status_for_combination(item_csv_row.status, parsed_attributes, parsed_attribute_values, sku_variant),
                order_limit=get_order_limit_for_combination(item_csv_row.order_limit, parsed_attributes, parsed_attribute_values, sku_variant),
                package_size_length=get_package_size_length_for_combination(item_csv_row.package_size_length, parsed_attributes, parsed_attribute_values, sku_variant),
                package_size_width=get_package_size_width_for_combination(item_csv_row.package_size_width, parsed_attributes, parsed_attribute_values, sku_variant),
                package_size_height=get_package_size_height_for_combination(item_csv_row.package_size_height, parsed_attributes, parsed_attribute_values, sku_variant),
                package_weight=get_package_weight_for_combination(item_csv_row.package_weight, parsed_attributes, parsed_attribute_values, sku_variant),
                created_by=user_id,
                updated_by=user_id,
                created_date=now_epoch_ms(),
                updated_date=now_epoch_ms(),
            )
            db.add(sku)
            db.flush()

            sku.part_number = str(sku.id).zfill(9)
            sku.mobile_barcode = f"S{sku.id}P{product_id}"
            sku.barcode = barcode_helper.encode_barcode_to_base64(barcode_helper.generate_barcode_image(sku.mobile_barcode))

            main_sku.part_number = sku.part_number
            main_sku.mobile_barcode = sku.mobile_barcode
            main_sku.barcode = sku.barcode
            main_sku.price = sku.price
            main_sku.quantity = sku.quantity

            for attr_detail in sku_variant:
                product_variant = ProductVariantOrm(
                    sku_id=sku.id,
                    main_sku_id=main_sku.id,
                    attribute_id=attr_id_map[attr_detail['attribute_name']],
                    attribute_value_id=attr_val_id_map[(attr_detail['attribute_name'], attr_detail['value'])],
                    active="ACTIVE",
                    created_by=user_id,
                    updated_by=user_id,
                    created_date=now_epoch_ms(),
                    updated_date=now_epoch_ms(),
                )
                db.add(product_variant)

            processed_main_sku_ids.append(main_sku.id)

        if item_csv_row.images and first_main_sku_id:
            parsed_images = parse_images_string(item_csv_row.images)
            for img_data in parsed_images:
                image = ProductImageOrm(
                    name=img_data['url'],
                    product_id=product_id,
                    main_sku_id=first_main_sku_id,
                    main_image=img_data['main_image'],
                    active="ACTIVE",
                    created_by=user_id,
                    updated_by=user_id,
                    created_date=now_epoch_ms(),
                    updated_date=now_epoch_ms(),
                )
                db.add(image)

    except (NoResultFound, ItemParserError, DataLoaderError) as e:
        logger.error(f"Error processing item row: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise DataLoaderError(message=str(e), error_type=ErrorType.UNEXPECTED_ROW_ERROR)

    return processed_main_sku_ids

def load_items_to_db(
    db: Session,
    business_details_id: int,
    item_records_data: List[Dict[str, Any]], 
    session_id: str, 
    user_id: int
) -> Dict[str, int]:
    summary = {
        "csv_rows_processed": 0,
        "csv_rows_with_errors": 0,
        "total_main_skus_created_or_updated": 0
    }
    from pydantic import ValidationError

    for idx, raw_record_dict in enumerate(item_records_data):
        summary["csv_rows_processed"] += 1
        try:
            item_csv_model = ItemCsvModel(**raw_record_dict)
            
            savepoint = db.begin_nested()
            try:
                processed_ids = load_item_record_to_db(
                    db=db,
                    business_details_id=business_details_id,
                    item_csv_row=item_csv_model,
                    user_id=user_id
                )
                savepoint.commit()
                summary["total_main_skus_created_or_updated"] += len(processed_ids)
            except (DataLoaderError, ItemParserError, IntegrityError, DataError) as e:
                savepoint.rollback()
                logger.error(f"Row {idx+2}: Error processing row: {e}")
                summary["csv_rows_with_errors"] += 1
            except Exception as e:
                savepoint.rollback()
                logger.error(f"Row {idx+2}: Unexpected error: {e}")
                summary["csv_rows_with_errors"] += 1

        except ValidationError as e:
            logger.error(f"Row {idx+2}: Validation error: {e}")
            summary["csv_rows_with_errors"] += 1

    return summary
