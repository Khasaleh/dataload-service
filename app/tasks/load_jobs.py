
from celery import shared_task
from app.db.connection import get_session
from sqlalchemy import text

@shared_task
def load_product_data(business_id: str, product: dict):
    session = get_session(business_id)
    try:
        stmt = text("""
            INSERT INTO products (product_name, product_url, brand_id, category_id, return_policy_id, length, width, height, weight, status)
            VALUES (:product_name, :product_url, :brand_id, :category_id, :return_policy_id, :package_length, :package_width, :package_height, :package_weight, :status)
            ON CONFLICT (product_url) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                brand_id = EXCLUDED.brand_id,
                category_id = EXCLUDED.category_id,
                return_policy_id = EXCLUDED.return_policy_id,
                length = EXCLUDED.package_length,
                width = EXCLUDED.package_width,
                height = EXCLUDED.package_height,
                weight = EXCLUDED.package_weight,
                status = EXCLUDED.status;
        """)
        session.execute(stmt, product)
        session.commit()
        return {"status": "success", "product_url": product["product_url"]}
    except Exception as e:
        session.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        session.close()

@shared_task
def load_item_data(business_id: str, item: dict):
    session = get_session(business_id)
    try:
        stmt = text("""
            INSERT INTO product_items (product_name, variant_sku, attribute_combination, status, published, default_sku, quantity, image_urls)
            VALUES (:product_name, :variant_sku, :attribute_combination, :status, :published, :default_sku, :quantity, :image_urls)
            ON CONFLICT (variant_sku) DO UPDATE SET
                attribute_combination = EXCLUDED.attribute_combination,
                status = EXCLUDED.status,
                published = EXCLUDED.published,
                default_sku = EXCLUDED.default_sku,
                quantity = EXCLUDED.quantity,
                image_urls = EXCLUDED.image_urls;
        """)
        session.execute(stmt, item)
        session.commit()
        return {"status": "success", "variant_sku": item["variant_sku"]}
    except Exception as e:
        session.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        session.close()

@shared_task
def load_price_data(business_id: str, price: dict):
    session = get_session(business_id)
    try:
        stmt = text("""
            INSERT INTO product_prices (product_name, price, offer_price, cost_per_item)
            VALUES (:product_name, :price, :offer_price, :cost_per_item)
            ON CONFLICT (product_name) DO UPDATE SET
                price = EXCLUDED.price,
                offer_price = EXCLUDED.offer_price,
                cost_per_item = EXCLUDED.cost_per_item;
        """)
        session.execute(stmt, price)
        session.commit()
        return {"status": "success", "product_name": price["product_name"]}
    except Exception as e:
        session.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        session.close()

@shared_task
def load_meta_data(business_id: str, meta: dict):
    session = get_session(business_id)
    try:
        stmt = text("""
            INSERT INTO product_meta (product_name, meta_title, meta_keywords, meta_description)
            VALUES (:product_name, :meta_title, :meta_keywords, :meta_description)
            ON CONFLICT (product_name) DO UPDATE SET
                meta_title = EXCLUDED.meta_title,
                meta_keywords = EXCLUDED.meta_keywords,
                meta_description = EXCLUDED.meta_description;
        """)
        session.execute(stmt, meta)
        session.commit()
        return {"status": "success", "product_name": meta["product_name"]}
    except Exception as e:
        session.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        session.close()
@shared_task
def load_brand_data(business_id: str, brand: dict):
    session = get_session(business_id)
    try:
        stmt = text("""
            INSERT INTO product_brands (brand_id, brand_name)
            VALUES (:brand_id, :brand_name)
            ON CONFLICT (brand_id) DO UPDATE SET
                brand_name = EXCLUDED.brand_name;
        """)
        session.execute(stmt, brand)
        session.commit()
        return {"status": "success", "brand_id": brand["brand_id"]}
    except Exception as e:
        session.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        session.close()


@shared_task
def load_attribute_data(business_id: str, attr: dict):
    session = get_session(business_id)
    try:
        stmt = text("""
            INSERT INTO product_attributes (attribute_name, attribute_type)
            VALUES (:attribute_name, :attribute_type)
            ON CONFLICT (attribute_name) DO UPDATE SET
                attribute_type = EXCLUDED.attribute_type;
        """)
        session.execute(stmt, attr)
        session.commit()
        return {"status": "success", "attribute_name": attr["attribute_name"]}
    except Exception as e:
        session.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        session.close()


@shared_task
def load_return_policy_data(business_id: str, rp: dict):
    session = get_session(business_id)
    try:
        stmt = text("""
            INSERT INTO return_policies (policy_id, policy_name, policy_description)
            VALUES (:policy_id, :policy_name, :policy_description)
            ON CONFLICT (policy_id) DO UPDATE SET
                policy_name = EXCLUDED.policy_name,
                policy_description = EXCLUDED.policy_description;
        """)
        session.execute(stmt, rp)
        session.commit()
        return {"status": "success", "policy_id": rp["policy_id"]}
    except Exception as e:
        session.rollback()
        return {"status": "error", "error": str(e)}
    finally:
        session.close()
