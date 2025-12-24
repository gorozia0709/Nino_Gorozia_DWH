CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE TYPE bl_cl.product_rec_type AS (
    product_src_id            VARCHAR,
    product_name             VARCHAR,
    subcategory_id           BIGINT,
    product_subcategory_name VARCHAR,
    category_id              BIGINT,
    product_category_name    VARCHAR,
    brand_id                 BIGINT,
    product_brand_name       VARCHAR,
    product_rating           DECIMAL,
    start_dt                 TIMESTAMP,
    end_dt                   TIMESTAMP,
    ta_insert_dt             TIMESTAMP,
    source_system            VARCHAR,
    source_entity            VARCHAR
);



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_dim_products_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    rec bl_cl.product_rec_type;
    cur_products CURSOR FOR
    SELECT
        COALESCE(p.product_id::varchar, 'n. a.') AS product_src_id,
        COALESCE(p.product_name, 'n. a.') AS product_name,
        COALESCE(p.subcategory_id, -1) AS subcategory_id,
        COALESCE(sc.subcategory_name, 'n. a.') AS product_subcategory_name,
        COALESCE(c.category_id, -1) AS category_id,
        COALESCE(c.category_name, 'n. a.') AS product_category_name,
        COALESCE(b.brand_id, -1) AS brand_id,
        COALESCE(b.brand_name, 'n. a.') AS product_brand_name,
        COALESCE(p.product_rating, -1) AS product_rating,
        COALESCE(p.start_dt, NOW()) AS start_dt,
        COALESCE(p.end_dt, NOW()) AS end_dt,
        clock_timestamp() AS ta_insert_dt,
        'BL_3NF' AS source_system,
        'BL_3NF.CE_PRODUCTS_SCD' AS source_entity
    FROM BL_3NF.CE_Products_SCD p
    LEFT JOIN BL_3NF.CE_Subcategories sc ON p.subcategory_id = sc.subcategory_id
    LEFT JOIN BL_3NF.CE_Categories c ON sc.category_id = c.category_id
    LEFT JOIN BL_3NF.CE_Brands b ON p.brand_id = b.brand_id
    WHERE p.is_active = 'Y' AND p.product_id != -1;


    v_rows_inserted INT := 0;
    v_rows_updated INT := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
	
    OPEN cur_products;
    LOOP
        FETCH cur_products INTO rec;
        EXIT WHEN NOT FOUND;

        IF EXISTS (
            SELECT 1
            FROM BL_DM.DIM_Products_SCD dp
            WHERE dp.product_src_id = rec.product_src_id
              AND dp.source_system = rec.source_system
              AND dp.source_entity = rec.source_entity
              AND dp.is_active = 'Y'
              AND (
                    dp.product_name IS DISTINCT FROM rec.product_name OR
                    dp.subcategory_id IS DISTINCT FROM rec.subcategory_id OR
                    dp.product_subcategory_name IS DISTINCT FROM rec.product_subcategory_name OR
                    dp.category_id IS DISTINCT FROM rec.category_id OR
                    dp.product_category_name IS DISTINCT FROM rec.product_category_name OR
                    dp.brand_id IS DISTINCT FROM rec.brand_id OR
                    dp.product_brand_name IS DISTINCT FROM rec.product_brand_name OR
                    dp.product_rating IS DISTINCT FROM rec.product_rating))
					
        THEN UPDATE BL_DM.DIM_Products_SCD  SET end_dt = rec.start_dt - INTERVAL '1 second', is_active = 'N'
            WHERE product_src_id = rec.product_src_id AND source_system = rec.source_system AND source_entity = rec.source_entity AND is_active = 'Y';
             
            INSERT INTO BL_DM.DIM_Products_SCD (
                product_surr_id,
                product_src_id,
                product_name,
                subcategory_id,
                product_subcategory_name,
                category_id,
                product_category_name,
                brand_id,
                product_brand_name,
                product_rating,
                start_dt,
                end_dt,
                is_active,
                ta_insert_dt,
                source_system,
                source_entity)
            VALUES (
                NEXTVAL('bl_dm.seq_product_surr_id'),
                rec.product_src_id,
                rec.product_name,
                rec.subcategory_id,
                rec.product_subcategory_name,
                rec.category_id,
                rec.product_category_name,
                rec.brand_id,
                rec.product_brand_name,
                rec.product_rating,
                rec.start_dt,
                rec.end_dt,
                'Y',
                rec.ta_insert_dt,
                rec.source_system,
                rec.source_entity);
            v_rows_updated := v_rows_updated + 1;

        ELSIF NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Products_SCD dp WHERE dp.product_src_id = rec.product_src_id AND dp.source_system = rec.source_system AND dp.source_entity = rec.source_entity AND dp.start_dt = rec.start_dt)
           
        THEN
            INSERT INTO BL_DM.DIM_Products_SCD (
                product_surr_id,
                product_src_id,
                product_name,
                subcategory_id,
                product_subcategory_name,
                category_id,
                product_category_name,
                brand_id,
                product_brand_name,
                product_rating,
                start_dt,
                end_dt,
                is_active,
                ta_insert_dt,
                source_system,
                source_entity)
            VALUES (
                NEXTVAL('bl_dm.seq_product_surr_id'),
                rec.product_src_id,
                rec.product_name,
                rec.subcategory_id,
                rec.product_subcategory_name,
                rec.category_id,
                rec.product_category_name,
                rec.brand_id,
                rec.product_brand_name,
                rec.product_rating,
                rec.start_dt,
                rec.end_dt,
                'Y',
                rec.ta_insert_dt,
                rec.source_system,
                rec.source_entity);
            v_rows_inserted := v_rows_inserted + 1;
        END IF;
    END LOOP;
    CLOSE cur_products;

    v_end_time := clock_timestamp();

    CALL BL_CL.prc_log_insert('prc_load_dim_products_scd', v_rows_inserted + v_rows_updated, CASE WHEN (v_rows_inserted + v_rows_updated) > 0 THEN 'Loaded successfully' ELSE 'No new or changed data to load' END, v_end_time - v_start_time );  

EXCEPTION WHEN OTHERS THEN
   v_end_time := clock_timestamp(); CALL BL_CL.prc_log_insert( 'prc_load_dim_products_scd', v_rows_inserted + v_rows_updated, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;




