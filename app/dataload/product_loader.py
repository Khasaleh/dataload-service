
import csv
from typing import List, Dict, Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, NoResultFound, DataError
import logging

# Fix missing now_epoch_ms import: fallback to local definition
try:
    from app.utils.date_utils import now_epoch_ms
except ImportError:
    from datetime import datetime
    def now_epoch_ms() -> int:
        return int(datetime.utcnow().timestamp() * 1000)

from app.utils.slug import generate_slug

from app.db.models import (
    ProductOrm,
    ProductImageOrm,
    ProductSpecificationOrm,
    BrandOrm,
    CategoryOrm,
    ReturnPolicyOrm,
)
from app.models.shopping_category import ShoppingCategoryOrm
from app.dataload.models.product_csv import ProductCsvModel
from app.exceptions import DataLoaderError
from app.models.schemas import ErrorType
from app.core.context import get_current_user_id  # extracts user ID from request context

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
    db_pk_redis_pipeline: Any = None
) -> Dict[str, int]:
    """
    Upsert all products from the CSV into products (+ specs + images).
    """
    summary = {"inserted": 0, "updated": 0, "errors": 0}
    user_id = get_current_user_id()

    for idx, raw in enumerate(records_data, start=2):
        try:
            model = ProductCsvModel(**raw)
            prod_id = load_product_record_to_db(
                db_session,
                business_details_id,
                model,
                session_id,
                user_id
            )
            from app.utils.redis_utils import add_to_id_map, DB_PK_MAP_SUFFIX
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
        except DataLoaderError:
            summary["errors"] += 1
        except Exception:
            summary["errors"] += 1
    return summary


def load_product_record_to_db(
    db: Session,
    business_details_id: int,
    product_data: ProductCsvModel,
    session_id: str,
    user_id: int
) -> int:
    """
    Loads or updates a single product.
    """
    log_prefix = f"[Product {product_data.self_gen_product_id}]"
    now_ms = now_epoch_ms()
    try:
        # 1) Lookups
        brand = db.query(BrandOrm).filter_by(
            name=product_data.brand_name,
            business_details_id=business_details_id
        ).one_or_none()
        if not brand:
            raise DataLoaderError(
                message=f"Brand '{product_data.brand_name}' not found.",
                error_type=ErrorType.LOOKUP,
                field_name="brand_name",
                offending_value=product_data.brand_name
            )
        category = db.query(CategoryOrm).filter_by(
            id=product_data.category_id,
            business_details_id=business_details_id
        ).one_or_none()
        if not category:
            raise DataLoaderError(
                message=f"Category ID '{product_data.category_id}' not found.",
                error_type=ErrorType.LOOKUP,
                field_name="category_id",
                offending_value=product_data.category_id
            )
        shopping_cat_id: Optional[int] = None
        if product_data.shopping_category_name:
            sc = db.query(ShoppingCategoryOrm).filter_by(
                name=product_data.shopping_category_name
            ).one_or_none()
            if sc:
                shopping_cat_id = sc.id
            else:
                logger.warning(f"{log_prefix} ShoppingCategory '{product_data.shopping_category_name}' not found.")
        # return policy
        rp_filters = [
            ReturnPolicyOrm.business_details_id == business_details_id,
            ReturnPolicyOrm.return_policy_type == product_data.return_type
        ]
        if product_data.return_type == "SALES_RETURN_ALLOWED":
            rp_filters += [
                ReturnPolicyOrm.return_fee_type == product_data.return_fee_type,
                ReturnPolicyOrm.time_period_return == product_data.return_fee
            ]
        else:
            rp_filters += [ReturnPolicyOrm.time_period_return.is_(None)]
        rp = db.query(ReturnPolicyOrm).filter(*rp_filters).one_or_none()
        if not rp:
            raise DataLoaderError(
                message="Matching return policy not found.",
                error_type=ErrorType.LOOKUP,
                field_name="return_type",
                offending_value=product_data.return_type
            )
        # 2) Upsert product
        prod = db.query(ProductOrm).filter_by(
            self_gen_product_id=product_data.self_gen_product_id,
            business_details_id=business_details_id
        ).one_or_none()
        is_new = prod is None
        if is_new:
            prod = ProductOrm(
                self_gen_product_id=product_data.self_gen_product_id,
                business_details_id=business_details_id,
                created_by=user_id,
                created_date=now_ms,
                barcode=generate_slug(product_data.self_gen_product_id)
            )
            db.add(prod)
        else:
            prod.updated_by = user_id
            prod.updated_date = now_ms
        # common fields
        prod.name                 = product_data.product_name
        prod.description          = product_data.description
        prod.brand_name           = product_data.brand_name
        prod.category_id          = product_data.category_id
        prod.shopping_category_id = shopping_cat_id
        prod.price                = product_data.price
        prod.sale_price           = product_data.sale_price
        prod.cost_price           = product_data.cost_price
        prod.quantity             = product_data.quantity
        prod.package_size_length  = product_data.package_size_length
        prod.package_size_width   = product_data.package_size_width
        prod.package_size_height  = product_data.package_size_height
        prod.product_weights      = product_data.product_weights
        prod.size_unit            = product_data.size_unit.upper() if product_data.size_unit else None
        prod.weight_unit          = product_data.weight_unit.upper() if product_data.weight_unit else None
        prod.return_policy_id     = rp.id
        prod.return_type          = product_data.return_type
        prod.return_fee_type      = product_data.return_fee_type
        prod.time_period_return   = product_data.return_fee
        prod.url                  = product_data.url or generate_slug(product_data.product_name)
        prod.video_url            = product_data.video_url
        prod.video_thumbnail_url  = product_data.video_thumbnail_url
        prod.active               = product_data.active
        prod.ean                  = product_data.ean
        prod.isbn                 = product_data.isbn
        prod.keywords             = product_data.keywords
        prod.mpn                  = product_data.mpn
        prod.seo_description      = product_data.seo_description
        prod.seo_title            = product_data.seo_title
        prod.upc                  = product_data.upc
        prod.is_child_item        = product_data.is_child_item
        db.flush()
        # 3) Specs
        if not is_new:
            db.query(ProductSpecificationOrm).filter_by(product_id=prod.id).delete()
        for name, val in [
            ("Warehouse_Location", getattr(product_data, 'warehouse_location', None)),
            ("Store_Location", getattr(product_data, 'store_location', None))
        ]:
            if val is not None:
                db.add(ProductSpecificationOrm(
                    product_id=prod.id,
                    name=name,
                    value=val,
                    active='ACTIVE',
                    created_by=user_id if is_new else None,
                    created_date=now_ms if is_new else None
                ))
        for spec in parse_specifications(product_data.specifications):
            db.add(ProductSpecificationOrm(
                product_id=prod.id,
                name=spec['name'],
                value=spec['value'],
                active='ACTIVE',
                created_by=user_id if is_new else None,
                created_date=now_ms if is_new else None
            ))
        # 4) Images
        if not is_new:
            db.query(ProductImageOrm).filter_by(product_id=prod.id).delete()
        main_img: Optional[str] = None
        for img in parse_images(product_data.images):
            db.add(ProductImageOrm(
                product_id=prod.id,
                name=img['url'],
                main_image=img['main_image'],
                active='ACTIVE',
                created_by=user_id if is_new else None,
                created_date=now_ms if is_new else None
            ))
            if img['main_image']:
                main_img = img['url']
        prod.main_image_url = main_img
        return prod.id
    except (IntegrityError, DataError) as e:
        db.rollback()
        raise DataLoaderError(
            message=str(e.orig),
            error_type=ErrorType.DATABASE,
            offending_value=product_data.self_gen_product_id,
            original_exception=e
        )
    except NoResultFound as e:
        db.rollback()
        raise DataLoaderError(
            message=str(e),
            error_type=ErrorType.LOOKUP,
            offending_value=product_data.self_gen_product_id,
            original_exception=e
        )
    except DataLoaderError:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise DataLoaderError(
            message=str(e),
            error_type=ErrorType.UNEXPECTED_ROW_ERROR,
            offending_value=product_data.self_gen_product_id,
            original_exception=e
        )
