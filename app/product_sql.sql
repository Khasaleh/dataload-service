DO
$$
DECLARE
    target_product_id BIGINT;
BEGIN
    -- 1️⃣ Find the product ID
    SELECT id INTO target_product_id
    FROM public.products
    WHERE name = 'Adidas Performance Tee'
      AND business_details_id = 11;

    -- Exit if not found
    IF target_product_id IS NULL THEN
        RAISE NOTICE 'Product not found. Nothing to delete.';
        RETURN;
    END IF;

    -- 2️⃣ Null out variant_id in main_skus (to break FK)
    UPDATE public.main_skus
    SET variant_id = NULL
    WHERE product_id = target_product_id
      AND variant_id IS NOT NULL;

    -- 3️⃣ Delete product_variant entries for SKUs
    DELETE FROM public.product_variant
    WHERE sku_id IN (
        SELECT id FROM public.sku WHERE product_id = target_product_id
    );

    -- 4️⃣ Delete product_variant entries for MainSKUs
    DELETE FROM public.product_variant
    WHERE main_sku_id IN (
        SELECT id FROM public.main_skus WHERE product_id = target_product_id
    );

    -- 5️⃣ Delete images linked to MainSKUs
    DELETE FROM public.product_images
    WHERE main_sku_id IN (
        SELECT id FROM public.main_skus WHERE product_id = target_product_id
    );

    -- 6️⃣ Delete images linked to the Product
    DELETE FROM public.product_images
    WHERE product_id = target_product_id;

    -- 7️⃣ Delete SKUs
    DELETE FROM public.sku
    WHERE product_id = target_product_id;

    -- 8️⃣ Delete MainSKUs
    DELETE FROM public.main_skus
    WHERE product_id = target_product_id;

    -- 9️⃣ Delete Product Specifications
    DELETE FROM public.product_specification
    WHERE product_id = target_product_id;

    -- 10️⃣ Delete Product Price History
    DELETE FROM public.products_price_history
    WHERE product_id = target_product_id;

    -- 11️⃣ Finally, delete the Product itself
    DELETE FROM public.products
    WHERE id = target_product_id;

    RAISE NOTICE '✅ Deleted product and related data for product ID %', target_product_id;
END
$$ LANGUAGE plpgsql;
