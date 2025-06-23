import csv # Not strictly needed here anymore but kept for parse_images/specs context if they were more complex
from typing import List, Dict, Optional, Any # Added Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, NoResultFound
import logging
from datetime import datetime

from app.db.models import (
    ProductOrm,
    ProductImageOrm,
    ProductSpecificationOrm,
    BrandOrm,
    CategoryOrm,
    ReturnPolicyOrm,
    PUBLIC_SCHEMA, # Used in ORM sequence defaults
    CATALOG_SCHEMA # Used in ORM FKs if any point there
)
from app.models.shopping_category import ShoppingCategoryOrm
from app.dataload.models.product_csv import ProductCsvModel # generate_url_slug is part of this model

logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO) # Configuration should be handled at application entry point

SYSTEM_USER_ID = 0 # Placeholder for created_by/updated_by

def parse_specifications(spec_str: Optional[str]) -> List[Dict[str, str]]:
    specs = []
    if not spec_str:
        return specs
    pairs = spec_str.split('|')
    for pair in pairs:
        if ':' in pair:
            name, value = pair.split(':', 1)
            if name.strip() and value.strip():
                specs.append({"name": name.strip(), "value": value.strip()})
            else:
                logger.warning(f"Skipping malformed specification pair: '{pair}'")
        else:
            logger.warning(f"Skipping malformed specification pair (no colon): '{pair}'")
    return specs

def parse_images(image_str: Optional[str]) -> List[Dict[str, any]]:
    images = []
    if not image_str:
        return images
    parts = image_str.split('|')
    if len(parts) % 2 != 0:
        logger.warning(f"Malformed images string: '{image_str}'. Odd number of parts.")
        return images

    for i in range(0, len(parts), 2):
        url = parts[i].strip()
        flag_part = parts[i+1].strip()

        if not url.startswith(('http://', 'https://')):
            logger.warning(f"Invalid image URL: '{url}' in images string.")
            continue

        if flag_part.lower() == "main_image:true":
            is_main = True
        elif flag_part.lower() == "main_image:false":
            is_main = False
        else:
            logger.warning(f"Malformed main_image flag: '{flag_part}' in images string.")
            continue

        images.append({"name": url, "main_image": is_main})
    return images

def load_product_record_to_db(
    db: Session,
    business_details_id: int,
    product_data: ProductCsvModel, # Accepts a validated Pydantic model instance
    session_id: str # For logging context
) -> Optional[int]: # Returns ProductOrm.id or None
    """
    Loads or updates a single product record in the database.
    Handles lookups, ORM object creation/update for one product.
    """
    log_prefix = f"Product self_gen_id '{product_data.self_gen_product_id}' (Session: {session_id}, Business: {business_details_id}):"

    try:
        # 1. Lookups (using data from the validated Pydantic model)
        # Brand Lookup
        brand = db.query(BrandOrm).filter(
            BrandOrm.name == product_data.brand_name,
            BrandOrm.business_details_id == business_details_id
        ).one_or_none()
        if not brand:
            logger.error(f"{log_prefix} Brand '{product_data.brand_name}' not found.")
            return None
        # brand_id = brand.id # Not stored on ProductOrm directly per its DDL

        # Category Lookup
        category = db.query(CategoryOrm).filter(
            CategoryOrm.id == product_data.category_id,
            CategoryOrm.business_details_id == business_details_id
        ).one_or_none()
        if not category:
            logger.error(f"{log_prefix} Category with id '{product_data.category_id}' not found.")
            return None

        # Shopping Category Lookup
        shopping_category_orm_id: Optional[int] = None
        if product_data.shopping_category_name:
            shopping_cat = db.query(ShoppingCategoryOrm).filter(
                ShoppingCategoryOrm.name == product_data.shopping_category_name
            ).one_or_none()
            if not shopping_cat:
                logger.warning(f"{log_prefix} Shopping Category '{product_data.shopping_category_name}' not found. Proceeding without it.")
            else:
                shopping_category_orm_id = shopping_cat.id

        # Return Policy Lookup
        return_policy_orm_id: Optional[int] = None
        if product_data.return_type: # return_type is mandatory from Pydantic model
            query_filters = [
                ReturnPolicyOrm.business_details_id == business_details_id,
                ReturnPolicyOrm.return_type == product_data.return_type
            ]
            if product_data.return_type == "SALES_RETURN_ALLOWED":
                # Pydantic model ensures return_fee_type is not None here
                query_filters.append(ReturnPolicyOrm.return_fee_type == product_data.return_fee_type)
                # Pydantic model normalizes return_fee for 'FREE' to 0.0
                query_filters.append(ReturnPolicyOrm.return_fee == product_data.return_fee)
            elif product_data.return_type == "SALES_ARE_FINAL":
                # Pydantic model ensures return_fee_type and return_fee are None here
                query_filters.append(ReturnPolicyOrm.return_fee_type.is_(None))
                query_filters.append(ReturnPolicyOrm.return_fee.is_(None))

            matched_policy = db.query(ReturnPolicyOrm).filter(*query_filters).one_or_none()
            if not matched_policy:
                logger.error(
                    f"{log_prefix} No matching ReturnPolicy found for "
                    f"return_type='{product_data.return_type}', "
                    f"fee_type='{product_data.return_fee_type}', fee='{product_data.return_fee}'."
                )
                return None # Error out if no matching policy
            return_policy_orm_id = matched_policy.id

        # 2. Upsert Product
        # Product uniqueness for upsert is product_name + business_details_id
        product_orm_instance = db.query(ProductOrm).filter(
            ProductOrm.name == product_data.product_name, # Changed from self_gen_product_id
            ProductOrm.business_details_id == business_details_id
        ).one_or_none()

        current_time_epoch = int(datetime.utcnow().timestamp() * 1000)
        is_new_product = False

        if product_orm_instance: # Update
            product_orm_instance.updated_by = SYSTEM_USER_ID
            product_orm_instance.updated_date = current_time_epoch
            logger.debug(f"{log_prefix} Updating existing product ID: {product_orm_instance.id}")
        else: # Create
            is_new_product = True
            product_orm_instance = ProductOrm(
                self_gen_product_id=product_data.self_gen_product_id,
                business_details_id=business_details_id,
                created_by=SYSTEM_USER_ID,
                created_date=current_time_epoch,
                barcode=f"BARCODE-{product_data.self_gen_product_id}" # Placeholder for barcode
            )
            db.add(product_orm_instance)
            logger.debug(f"{log_prefix} Creating new product.")

        # Populate/Update common fields
        product_orm_instance.name = product_data.product_name
        product_orm_instance.description = product_data.description
        product_orm_instance.brand_name = product_data.brand_name # Storing brand_name on ProductOrm
        product_orm_instance.category_id = product_data.category_id
        product_orm_instance.shopping_category_id = shopping_category_orm_id
        product_orm_instance.price = product_data.price
        product_orm_instance.sale_price = product_data.sale_price
        product_orm_instance.cost_price = product_data.cost_price
        product_orm_instance.quantity = product_data.quantity
        product_orm_instance.package_size_length = product_data.package_size_length
        product_orm_instance.package_size_width = product_data.package_size_width
        product_orm_instance.package_size_height = product_data.package_size_height
        product_orm_instance.product_weights = product_data.product_weights
        product_orm_instance.size_unit = product_data.size_unit
        product_orm_instance.weight_unit = product_data.weight_unit
        product_orm_instance.active = product_data.active
        product_orm_instance.return_type = product_data.return_type
        product_orm_instance.return_fee_type = product_data.return_fee_type
        product_orm_instance.return_fee = product_data.return_fee
        product_orm_instance.return_policy_id = return_policy_orm_id
        product_orm_instance.url = product_data.url # From Pydantic model (auto-generated or validated)
        product_orm_instance.video_url = product_data.video_url
        # thumbnail_url will be handled after image processing, potentially using video_thumbnail_url
        product_orm_instance.is_child_item = product_data.is_child_item
        product_orm_instance.ean = product_data.ean
        product_orm_instance.isbn = product_data.isbn
        product_orm_instance.keywords = product_data.keywords
        product_orm_instance.mpn = product_data.mpn
        product_orm_instance.seo_description = product_data.seo_description
        product_orm_instance.seo_title = product_data.seo_title
        product_orm_instance.upc = product_data.upc

        # Other ProductOrm fields (e.g. product_type_status) are left to DB defaults or current values if not in CSV model

        db.flush() # Ensure product_orm_instance.id is available for relations

        # 3. Handle Product Specifications
        if not is_new_product: # Clear old ones on update
            db.query(ProductSpecificationOrm).filter(ProductSpecificationOrm.product_id == product_orm_instance.id).delete(synchronize_session='fetch')

        parsed_specs = parse_specifications(product_data.specifications)
        for spec_data in parsed_specs:
            db.add(ProductSpecificationOrm(
                product_id=product_orm_instance.id, name=spec_data["name"], value=spec_data["value"],
                active="ACTIVE", created_by=SYSTEM_USER_ID, created_date=current_time_epoch # Assuming new specs are active
            ))

        # 4. Handle Product Images (only if is_child_item == 0) and Thumbnail
        # Reset main_image_url and thumbnail_url on product.
        # thumbnail_url will be populated by video_thumbnail_url if provided,
        # otherwise by main_image_url if available.
        product_orm_instance.main_image_url = None
        product_orm_instance.thumbnail_url = product_data.video_thumbnail_url # Prioritize dedicated video thumbnail

        if not is_new_product: # Clear old product images on update
            db.query(ProductImageOrm).filter(ProductImageOrm.product_id == product_orm_instance.id).delete(synchronize_session='fetch')

        if product_data.is_child_item == 0 and product_data.images:
            parsed_imgs = parse_images(product_data.images)
            found_main_image_url = None
            for img_data in parsed_imgs:
                db.add(ProductImageOrm(
                    product_id=product_orm_instance.id, name=img_data["name"], main_image=img_data["main_image"],
                    active="ACTIVE", created_by=SYSTEM_USER_ID, created_date=current_time_epoch # Assuming new images are active
                ))
                if img_data["main_image"]:
                    found_main_image_url = img_data["name"]

            product_orm_instance.main_image_url = found_main_image_url

            # If video_thumbnail_url was not provided from CSV, and a main product image exists,
            # use the main product image URL as the fallback for thumbnail_url.
            if not product_orm_instance.thumbnail_url and found_main_image_url:
                product_orm_instance.thumbnail_url = found_main_image_url

        # The calling function (process_csv_task) will handle session commit/rollback.
        # db.flush() # Called earlier to get ID, further flushes might occur before commit by caller.

        logger.info(f"{log_prefix} Successfully processed. DB ID: {product_orm_instance.id}")
        return product_orm_instance.id

    except IntegrityError as e:
        logger.error(f"{log_prefix} DB integrity error: {e}", exc_info=True)
        # Rollback will be handled by the caller (process_csv_task)
        return None
    except NoResultFound as e: # Should be caught by specific lookups, but as a safeguard
        logger.error(f"{log_prefix} DB lookup error (NoResultFound): {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"{log_prefix} Unexpected error: {e}", exc_info=True)
        # Rollback will be handled by the caller (process_csv_task)
        return None

# Example of how this would be called by process_csv_task (conceptual):
# for validated_row_dict in validated_records:
#     product_csv_model_instance = ProductCsvModel(**validated_row_dict) # Pydantic validation done by validate_csv
#     product_db_id = load_product_record_to_db(
#         db_session, business_id, product_csv_model_instance, session_id
#     )
#     if product_db_id:
#         # Add to Redis _db_pk map: product_data.self_gen_product_id -> product_db_id
#         add_to_id_map(session_id, f"products{DB_PK_MAP_SUFFIX}", product_csv_model_instance.self_gen_product_id, product_db_id, pipeline=db_pk_redis_pipeline)
#         processed_db_count += 1
#     else:
#         db_error_count += 1
