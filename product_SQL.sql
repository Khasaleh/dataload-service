-- SQL to delete a product and its related SKUs by product name and business_details_id
-- WARNING: Always back up your database before running destructive SQL commands.
-- Test thoroughly in a non-production environment first.
-- Replace 'YOUR_PRODUCT_NAME_HERE' and YOUR_BUSINESS_ID_HERE with actual values.

-- Start a transaction
BEGIN;

-- Step 1: (Optional but Recommended) Verify the product_id(s) you are about to affect.
-- SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE;

-- Step 2: Delete from ProductVariantOrm (links SKUs to attribute values)
-- These link SkuOrm and MainSkuOrm to attributes.
DELETE FROM public.product_variant
WHERE sku_id IN (
    SELECT id FROM public.sku
    WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE)
);

DELETE FROM public.product_variant
WHERE main_sku_id IN (
    SELECT id FROM public.main_skus
    WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE)
);

-- Step 3: Delete from ProductImageOrm (images linked to MainSkuOrm or ProductOrm)
-- Images linked to Main SKUs of the product
DELETE FROM public.product_images
WHERE main_sku_id IN (
    SELECT id FROM public.main_skus
    WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE)
);
-- Images linked directly to the Product
DELETE FROM public.product_images
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);

-- Step 4: Delete from SkuOrm
-- Note: SkuOrm.main_sku_id is an FK to MainSkuOrm.id.
-- If MainSkuOrm deletion cascades, this might be handled there. Explicit deletion is safer if unsure.
DELETE FROM public.sku
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);

-- Step 5: Delete from MainSkuOrm
DELETE FROM public.main_skus
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);

-- Step 6: Delete from ProductSpecificationOrm (specifications linked to ProductOrm)
DELETE FROM public.product_specification
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);

-- Step 7: Delete from ProductsPriceHistoryOrm (price history linked to ProductOrm)
DELETE FROM public.products_price_history
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);

-- Step 8: Delete from PriceOrm (catalog_management.prices)
-- Prices linked directly to the Product
DELETE FROM catalog_management.prices
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);
-- Prices linked to SKUs: These should have been deleted if SkuOrm deletion cascaded to PriceOrm (PriceOrm.sku_id FK SkuOrm.id).
-- If not, and you need to be certain, you would have had to capture the sku_ids before deleting them in Step 4
-- and then run: DELETE FROM catalog_management.prices WHERE sku_id IN (SELECT ... captured sku_ids ...);

-- Step 9: Delete from other direct product dependencies (e.g., MetaTagOrm, ProductPriceOrm - legacy)
DELETE FROM catalog_management.meta_tags
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);

DELETE FROM catalog_management.product_prices -- (Legacy prices table)
WHERE product_id IN (SELECT id FROM public.products WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE);

-- Step 10: Finally, delete the product itself from ProductOrm
DELETE FROM public.products
WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE;

-- Commit the transaction
COMMIT;

-- ---
-- Simplified version IF `ON DELETE CASCADE` is extensively used for all relationships:
-- ---
-- BEGIN;
--
-- DELETE FROM public.products
-- WHERE name = 'YOUR_PRODUCT_NAME_HERE' AND business_details_id = YOUR_BUSINESS_ID_HERE;
--
-- COMMIT;
-- ---
