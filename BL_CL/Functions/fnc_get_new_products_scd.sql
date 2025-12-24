CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE FUNCTION BL_CL.FN_GET_NEW_PRODUCTS_SCD()
RETURNS TABLE (
    fn_product_src_id VARCHAR(250),
    fn_product_name VARCHAR(100),
    fn_subcategory_id BIGINT,
    fn_brand_id BIGINT,
    fn_product_rating DECIMAL(3,2),
    fn_source_system VARCHAR(50),
    fn_source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
	    pf.product_src_id AS fn_product_src_id,
        pf.product_name AS fn_product_name,
        sub.subcategory_id,
        br.brand_id,
        pf.product_rating,
        pf.source_system,
        pf.source_entity
    FROM (
        SELECT DISTINCT ON (COALESCE(product_name, 'n. a.'))
		    COALESCE(product_id, 'n. a.') AS product_src_id,
            COALESCE(product_name, 'n. a.') AS product_name,
            COALESCE(subcategory, 'n. a.') AS subcategory_name,
            COALESCE(brand, 'n. a.') AS brand_name,
            COALESCE(rating::DECIMAL, 0.0) AS product_rating,
            'SA_OFFLINE_SALES'::VARCHAR(50) AS source_system,
            'SRC_OFFLINE_SALES'::VARCHAR(50) AS source_entity
        FROM sa_offline_sales.src_offline_sales

        UNION ALL

        SELECT DISTINCT ON (COALESCE(product_name, 'n. a.'))
		    COALESCE(product_id, 'n. a.') AS product_src_id,
            COALESCE(product_name, 'n. a.') AS product_name,
            COALESCE(subcategory, 'n. a.') AS subcategory_name,
            COALESCE(brand, 'n. a.') AS brand_name,
            COALESCE(rating::DECIMAL, 0.0) AS product_rating,
            'SA_ONLINE_SALES'::VARCHAR(50) AS source_system,
            'SRC_ONLINE_SALES'::VARCHAR(50) AS source_entity
        FROM sa_online_sales.src_online_sales
    ) pf
    LEFT JOIN BL_3NF.CE_Subcategories sub
        ON sub.subcategory_src_id = pf.subcategory_name
       AND sub.source_system = pf.source_system
       AND sub.source_entity = pf.source_entity
    LEFT JOIN BL_3NF.CE_Brands br
        ON br.brand_src_id = pf.brand_name
       AND br.source_system = pf.source_system
       AND br.source_entity = pf.source_entity
   WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_Products_SCD p
    WHERE p.product_src_id = pf.product_src_id
      AND p.source_system = pf.source_system
      AND p.source_entity = pf.source_entity
      AND p.is_active = 'Y'
      AND p.product_name = pf.product_name
      AND p.subcategory_id = sub.subcategory_id
      AND p.brand_id = br.brand_id
      AND p.product_rating = pf.product_rating
);
END;
$$;
