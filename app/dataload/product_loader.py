import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, NoResultFound, DataError

from app.utils.slug import generate_slug
try:
    from app.utils.date_utils import now_epoch_ms
except ImportError:
    def now_epoch_ms() -> int:
        return int(datetime.utcnow().timestamp() * 1000)

from app.db.models import (
    ProductOrm,
    ProductImageOrm,
    ProductSpecificationOrm,
    ProductsPriceHistoryOrm,
    BrandOrm,
    CategoryOrm,
    ReturnPolicyOrm,
)
from app.db.connection import get_session
from app.models.shopping_category import ShoppingCategoryOrm
from app.dataload.models.product_csv import ProductCsvModel
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType
from app.utils.redis_utils import add_to_id_map, DB_PK_MAP_SUFFIX, get_from_id_map

logger = logging.getLogger(__name__)


def parse_specifications(spec_str: Optional[str]) -> List[Dict[str, str]]:
    specs: List[Dict[str, str]] = []
    if not spec_str:
        return specs
    for pair in spec_str.split('|'):
        if ':' not in pair:
            logger.warning(f"Skipping malformed specification pair: '{pair}'")
            continue
        name, value = pair.split(':', 1)
        if name.strip() and value.strip():
            specs.append({"name": name.strip(), "value": value.strip()})
        else:
            logger.warning(f"Skipping empty spec name or value in '{pair}'")
    return specs


def parse_images(image_str: Optional[str]) -> List[Dict[str, Any]]:
    images: List[Dict[str, Any]] = []
    if not image_str:
        return images
    parts = image_str.split('|')
    if len(parts) % 2 != 0:
        logger.warning(f"Malformed images string: '{image_str}'")
        return images
    for i in range(0, len(parts), 2):
        url = parts[i].strip()
        flag = parts[i+1].strip().lower()
        if not url.startswith(('http://', 'https://')):
            logger.warning(f"Invalid image URL: '{url}'")
            continue
        if flag == 'main_image:true':
            is_main = True
        elif flag == 'main_image:false':
            is_main = False
        else:
            logger.warning(f"Invalid main_image flag: '{flag}'")
            continue
        images.append({"url": url, "main_image": is_main})
    return images


def load_products_to_db(
    db_session: Session,
    business_details_id: int,
    records_data: List[Dict[str, Any]],
    session_id: str,
    db_pk_redis_pipeline: Any = None,
    user_id: int = None,
) -> Dict[str, int]:
    logger.info(f"Starting load_products_to_db for {len(records_data)} records for business_id {business_details_id}, session_id {session_id}.")
    summary = {"inserted": 0, "updated": 0, "errors": 0}

    for idx, raw in enumerate(records_data, start=2):
        product_identifier_for_log = raw.get('self_gen_product_id', f"row_index_{idx-2}")
        try:
            logger.debug(f"[Product {product_identifier_for_log}] Raw data: {raw}")
            model = ProductCsvModel(**raw)
            logger.debug(f"[Product {model.self_gen_product_id}] Pydantic model created: {model.model_dump_json(indent=2)}")
            # Use the refactored function
            prod_id = load_product_record_to_db_refactored( 
                db_session,
                business_details_id,
                model,
                session_id,
                user_id
            )
            prev = add_to_id_map(
                session_id,
                f"products{DB_PK_MAP_SUFFIX}",
                model.self_gen_product_id,
                prod_id,
                pipeline=db_pk_redis_pipeline,
                read_only=True
            )
            if prev is None:
                summary["inserted"] += 1
            else:
                summary["updated"] += 1

            add_to_id_map(
                session_id,
                f"products{DB_PK_MAP_SUFFIX}",
                model.self_gen_product_id,
                prod_id,
                pipeline=db_pk_redis_pipeline
            )
        except DataLoaderError as e:
            logger.error(f"[Product {product_identifier_for_log}] DataLoaderError during processing: {e}", exc_info=True)
            summary["errors"] += 1
        except Exception as e:
            logger.error(f"[Product {product_identifier_for_log}] Unexpected exception during processing row: {raw}. Error: {e}", exc_info=True)
            summary["errors"] += 1
    logger.info(f"Finished load_products_to_db for business_id {business_details_id}, session_id {session_id}. Summary: {summary}")
    return summary


def load_product_record_to_db_refactored( # Renamed for clarity during refactor
    db: Session,
    business_details_id: int,
    product_data: ProductCsvModel,
    session_id: str,
    user_id: int
) -> int:
    log_prefix = f"[Product {product_data.self_gen_product_id}]"
    logger.info(f"{log_prefix} Starting 3-step processing for business_id {business_details_id}.")
    now_ms = now_epoch_ms()

    # --- Step 1: Lookups & Validations First ---
    try:
        logger.debug(f"{log_prefix} Step 1: Performing lookups and validations.")

        # 1a. Brand lookup
        logger.debug(f"{log_prefix} Looking up brand: {product_data.brand_name}")
        brand = db.query(BrandOrm).filter_by(
            name=product_data.brand_name,
            business_details_id=business_details_id
        ).one_or_none()
        if not brand:
            raise DataLoaderError(
                message=f"Brand '{product_data.brand_name}' not found.",
                error_type=ErrorType.LOOKUP, field_name="brand_name", offending_value=product_data.brand_name
            )
        logger.debug(f"{log_prefix} Brand found: ID {brand.id}")

        # 1b. Category lookup (Redis cache then DB, including leaf node check)
        logger.debug(f"{log_prefix} Looking up category_id for path '{product_data.category_path}' from Redis cache.")
        category_id_from_cache = get_from_id_map(session_id, f"categories{DB_PK_MAP_SUFFIX}", product_data.category_path)
        if not category_id_from_cache:
            raise DataLoaderError(
                message=f"Category ID for path '{product_data.category_path}' not found in Redis cache. Ensure categories are loaded first or path is correct.",
                error_type=ErrorType.LOOKUP, field_name="category_path", offending_value=product_data.category_path
            )
        
        logger.debug(f"{log_prefix} Category ID from cache: {category_id_from_cache}. Fetching CategoryOrm object from DB.")
        category = db.query(CategoryOrm).filter_by(id=category_id_from_cache, business_details_id=business_details_id).one_or_none()
        if not category:
            raise DataLoaderError(
                message=f"Category with ID '{category_id_from_cache}' (from path '{product_data.category_path}') not found in DB for business {business_details_id}.",
                error_type=ErrorType.LOOKUP, field_name="category_path", offending_value=product_data.category_path
            )
        logger.debug(f"{log_prefix} CategoryOrm object fetched: ID {category.id}, Name: {category.name}")

        # Leaf node check for category
        logger.debug(f"{log_prefix} Checking if category '{category.name}' (ID: {category.id}) is a leaf node.")
        is_parent_category = db.query(CategoryOrm.id).filter(CategoryOrm.parent_id == category.id, CategoryOrm.business_details_id == business_details_id).first()
        if is_parent_category:
            raise DataLoaderError(
                message=f"Category '{category.name}' (Path: '{product_data.category_path}') is not a leaf node. Products can only be assigned to leaf categories.",
                error_type=ErrorType.VALIDATION, field_name="category_path", offending_value=product_data.category_path
            )
        logger.debug(f"{log_prefix} Category '{category.name}' is a leaf node.")

        # 1c. Shopping category lookup (optional)
        shopping_cat_id: Optional[int] = None
        if product_data.shopping_category_name:
            logger.debug(f"{log_prefix} Looking up shopping category: {product_data.shopping_category_name}")
            sc = db.query(ShoppingCategoryOrm).filter_by(name=product_data.shopping_category_name).one_or_none()
            if sc:
                shopping_cat_id = sc.id
                logger.debug(f"{log_prefix} Shopping category found: {sc.id}")
            else:
                logger.warning(f"{log_prefix} ShoppingCategory '{product_data.shopping_category_name}' not found.")
        else:
            logger.debug(f"{log_prefix} No shopping category provided.")

        # 1d. Return policy lookup (from DB2)
        return_policy_orm_from_db2: Optional[ReturnPolicyOrm] = None
        if product_data.return_policy:
            logger.debug(f"{log_prefix} Looking up return policy '{product_data.return_policy}' in DB2.")
            db2_session = None
            try:
                db2_session = get_session(business_id=business_details_id, db_key="DB2")
                logger.debug(f"{log_prefix} Obtained DB2 session for return policy lookup.")
                return_policy_orm_from_db2 = db2_session.query(ReturnPolicyOrm).filter(
                    ReturnPolicyOrm.business_details_id == business_details_id,
                    ReturnPolicyOrm.policy_name == product_data.return_policy
                ).one_or_none()
                logger.debug(f"{log_prefix} Return policy query executed. Found: {return_policy_orm_from_db2.id if return_policy_orm_from_db2 else 'None'}")
                if not return_policy_orm_from_db2:
                    raise DataLoaderError(
                        message=f"Return policy '{product_data.return_policy}' not found in secondary database for business ID {business_details_id}.",
                        error_type=ErrorType.LOOKUP, field_name="return_policy", offending_value=product_data.return_policy
                    )
            finally:
                if db2_session:
                    logger.debug(f"{log_prefix} Closing DB2 session for return policy lookup.")
                    db2_session.close()
        else:
            logger.info(f"{log_prefix} No return_policy name provided in CSV. Product will not have a return policy linked.")
        
        logger.debug(f"{log_prefix} Step 1 completed successfully.")

        # --- Step 2: Product Core Data Load ---
        logger.debug(f"{log_prefix} Step 2: Upserting ProductOrm and populating core fields.")
        
        prod = db.query(ProductOrm).filter_by(
            self_gen_product_id=product_data.self_gen_product_id,
            business_details_id=business_details_id
        ).one_or_none()
        logger.debug(f"{log_prefix} Existing product found by self_gen_product_id: {prod.id if prod else 'None'}")

        is_new = prod is None
        old_actual_price: Optional[float] = None
        old_sale_price: Optional[float] = None

        if is_new:
            logger.info(f"{log_prefix} Creating new product.")
            prod = ProductOrm(
                self_gen_product_id=product_data.self_gen_product_id,
                business_details_id=business_details_id,
                created_by=user_id, created_date=now_ms,
                updated_by=user_id, updated_date=now_ms,
                barcode=generate_slug(product_data.self_gen_product_id)
            )
            db.add(prod)
            logger.debug(f"{log_prefix} New ProductOrm added to session.")
        else:
            logger.info(f"{log_prefix} Updating existing product ID: {prod.id}")
            old_actual_price = prod.price
            old_sale_price = prod.sale_price
            logger.debug(f"{log_prefix} Captured old prices for history - Actual: {old_actual_price}, Sale: {old_sale_price}")
            prod.updated_by = user_id
            prod.updated_date = now_ms

        # Populate core fields
        prod.name = product_data.product_name
        prod.description = product_data.description
        prod.brand_name = product_data.brand_name # Uses brand_name string as per requirement
        prod.category_id = category.id # From Step 1b
        prod.shopping_category_id = shopping_cat_id # From Step 1c
        prod.price = product_data.price
        prod.sale_price = product_data.sale_price
        prod.cost_price = product_data.cost_price
        prod.quantity = product_data.quantity
        prod.package_size_length = product_data.package_size_length
        prod.package_size_width = product_data.package_size_width
        prod.package_size_height = product_data.package_size_height
        prod.product_weights = product_data.product_weights
        prod.size_unit = product_data.size_unit.upper() if product_data.size_unit else None
        prod.weight_unit = product_data.weight_unit.upper() if product_data.weight_unit else None
        
        if return_policy_orm_from_db2: # From Step 1d
            prod.return_policy_id = return_policy_orm_from_db2.id
        elif product_data.return_policy: # Should have failed in Step 1d if name provided but not found
             pass # This case implies error logic in Step 1d was bypassed or name was empty
        elif is_new: # No policy name provided, and it's a new product
             prod.return_policy_id = None
        # If not new and no policy name given, existing prod.return_policy_id is preserved.

        prod.return_type = product_data.return_type
        prod.return_fee_type = product_data.return_fee_type
        prod.return_fee = product_data.return_fee
        prod.url = product_data.url or generate_slug(product_data.product_name)
        prod.video_url = product_data.video_url
        prod.thumbnail_url = product_data.video_thumbnail_url
        prod.active = product_data.active
        prod.ean = product_data.ean
        prod.isbn = product_data.isbn
        prod.keywords = product_data.keywords
        prod.mpn = product_data.mpn
        prod.seo_description = product_data.seo_description
        prod.seo_title = product_data.seo_title
        prod.upc = product_data.upc
        prod.is_child_item = product_data.is_child_item
        
        logger.debug(f"{log_prefix} Core product fields populated. Attempting flush to get/confirm product ID.")
        db.flush()
        logger.info(f"{log_prefix} Product ID after flush: {prod.id}")
        logger.debug(f"{log_prefix} Step 2 completed successfully.")

        # --- Step 3: Load Dependent Data ---
        logger.debug(f"{log_prefix} Step 3: Loading dependent data for product ID {prod.id}.")

        # ProductSpecifications
        logger.debug(f"{log_prefix} Processing product specifications.")
        if not is_new: # Delete existing only if product was not new
            logger.debug(f"{log_prefix} Deleting existing specifications for updated product.")
            db.query(ProductSpecificationOrm).filter_by(product_id=prod.id).delete()
        
        # Warehouse Location
        if product_data.warehouse_location is not None:
            db.add(ProductSpecificationOrm(product_id=prod.id, name="Warehouse Location", value=product_data.warehouse_location, active='ACTIVE', created_by=user_id, created_date=now_ms, updated_by=user_id, updated_date=now_ms))
            logger.debug(f"{log_prefix} Added Warehouse Location spec: {product_data.warehouse_location}")
        # Store Location
        if product_data.store_location is not None:
            db.add(ProductSpecificationOrm(product_id=prod.id, name="Store Location", value=product_data.store_location, active='ACTIVE', created_by=user_id, created_date=now_ms, updated_by=user_id, updated_date=now_ms))
            logger.debug(f"{log_prefix} Added Store Location spec: {product_data.store_location}")
        # CSV specifications
        parsed_csv_specs = parse_specifications(product_data.specifications)
        logger.debug(f"{log_prefix} Parsed {len(parsed_csv_specs)} specs from CSV column.")
        for spec in parsed_csv_specs:
            db.add(ProductSpecificationOrm(product_id=prod.id, name=spec["name"], value=spec["value"], active='ACTIVE', created_by=user_id, created_date=now_ms, updated_by=user_id, updated_date=now_ms))
            logger.debug(f"{log_prefix} Added CSV spec: {spec['name']} = {spec['value']}")

        # ProductImages
        logger.debug(f"{log_prefix} Processing product images.")
        if not is_new: # Delete existing only if product was not new
            logger.debug(f"{log_prefix} Deleting existing images for updated product.")
            db.query(ProductImageOrm).filter_by(product_id=prod.id).delete()
        
        main_img_url: Optional[str] = None
        parsed_imgs = parse_images(product_data.images)
        logger.debug(f"{log_prefix} Parsed {len(parsed_imgs)} images from CSV column.")
        for img_data in parsed_imgs:
            db.add(ProductImageOrm(product_id=prod.id, name=img_data["url"], main_image=img_data["main_image"], active='ACTIVE', created_by=user_id, created_date=now_ms, updated_by=user_id, updated_date=now_ms))
            if img_data["main_image"]:
                main_img_url = img_data["url"]
            logger.debug(f"{log_prefix} Added image: {img_data['url']}, main: {img_data['main_image']}")
        prod.main_image_url = main_img_url
        logger.debug(f"{log_prefix} Set main_image_url to: {main_img_url}")

        # Price History
        logger.debug(f"{log_prefix} Processing price history. is_new: {is_new}, old_actual: {old_actual_price}, old_sale: {old_sale_price}, new_actual: {prod.price}, new_sale: {prod.sale_price}")
        needs_price_history_entry = False
        final_old_price_for_history: Optional[float] = None
        if is_new:
            needs_price_history_entry = True
            final_old_price_for_history = None
        else:
            price_changed = old_actual_price != prod.price 
            sale_price_changed = old_sale_price != prod.sale_price
            if price_changed or sale_price_changed:
                needs_price_history_entry = True
                final_old_price_for_history = old_actual_price if price_changed else old_sale_price
        
        if needs_price_history_entry:
            current_time = datetime.utcnow()
            price_history_entry = ProductsPriceHistoryOrm(
                product_id=prod.id, price=prod.price, sale_price=prod.sale_price,
                old_price=final_old_price_for_history,
                month=current_time.strftime("%B"), year=current_time.year
            )
            logger.debug(f"{log_prefix} Adding price history entry: {price_history_entry.__dict__}")
            db.add(price_history_entry)
        else:
            logger.debug(f"{log_prefix} No price change detected or not applicable, skipping price history.")
        
        logger.debug(f"{log_prefix} Step 3 completed successfully.")
        logger.info(f"{log_prefix} Successfully processed product ID {prod.id}.")
        return prod.id

    except (IntegrityError, DataError) as e:
        logger.error(f"{log_prefix} Database integrity or data error: {e}", exc_info=True)
        db.rollback()
        raise DataLoaderError(message=str(e.orig), error_type=ErrorType.DATABASE, offending_value=product_data.self_gen_product_id, original_exception=e)
    except NoResultFound as e:
        logger.error(f"{log_prefix} Lookup error (NoResultFound): {e}", exc_info=True)
        db.rollback()
        raise DataLoaderError(message=str(e), error_type=ErrorType.LOOKUP, offending_value=product_data.self_gen_product_id, original_exception=e)
    except DataLoaderError as e:
        logger.error(f"{log_prefix} DataLoaderError occurred: {e.message}", exc_info=True) # Log full error
        db.rollback()
        raise
    except Exception as e:
        logger.error(f"{log_prefix} Unexpected error: {e}", exc_info=True)
        db.rollback()
        raise DataLoaderError(message=f"Unexpected error for product {product_data.self_gen_product_id}: {str(e)}", error_type=ErrorType.UNEXPECTED_ROW_ERROR, offending_value=product_data.self_gen_product_id, original_exception=e)

# Remove or comment out the old function if the refactored one is complete and tested.
# For now, I'm keeping the old one commented out below for reference.
# def load_product_record_to_db(...): ...
