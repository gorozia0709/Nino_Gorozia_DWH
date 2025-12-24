CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_products_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_rows_affected INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_existing_record RECORD;
    v_current_ts TIMESTAMP := clock_timestamp();
    v_max_ts TIMESTAMP := '9999-12-31 00:00:00';
    v_product_id BIGINT;
BEGIN

    FOR rec IN SELECT * FROM BL_CL.FN_GET_NEW_PRODUCTS_SCD()
    LOOP
        SELECT product_id INTO v_product_id
        FROM BL_3NF.CE_Products_SCD
        WHERE product_src_id = rec.fn_product_src_id
          AND source_system = rec.fn_source_system
          AND source_entity = rec.fn_source_entity
        ORDER BY ta_insert_dt
        LIMIT 1;

        SELECT * INTO v_existing_record
        FROM BL_3NF.CE_Products_SCD
        WHERE product_src_id = rec.fn_product_src_id
          AND source_system = rec.fn_source_system
          AND source_entity = rec.fn_source_entity
          AND is_active = 'Y'
          AND end_dt = v_max_ts;

        IF v_existing_record IS NOT NULL THEN
            IF v_existing_record.product_name   IS DISTINCT FROM rec.fn_product_name OR
               v_existing_record.subcategory_id IS DISTINCT FROM rec.fn_subcategory_id OR
               v_existing_record.brand_id       IS DISTINCT FROM rec.fn_brand_id OR
               v_existing_record.product_rating IS DISTINCT FROM rec.fn_product_rating THEN

                UPDATE BL_3NF.CE_Products_SCD
                SET end_dt = v_current_ts - INTERVAL '1 second',
                    is_active = 'N'
                WHERE product_id = v_existing_record.product_id
                  AND start_dt = v_existing_record.start_dt;

                INSERT INTO BL_3NF.CE_Products_SCD (
                    product_id, product_name, subcategory_id, brand_id,
                    product_rating, start_dt, end_dt, is_active,
                    ta_insert_dt, product_src_id, source_system, source_entity
                )
                VALUES (
                    v_existing_record.product_id,  
                    rec.fn_product_name,
                    rec.fn_subcategory_id,
                    rec.fn_brand_id,
                    rec.fn_product_rating,
                    v_current_ts,
                    v_max_ts,
                    'Y',
                    v_current_ts,
                    rec.fn_product_src_id,
                    rec.fn_source_system,
                    rec.fn_source_entity
                );

                v_rows_affected := v_rows_affected + 1;
            END IF;

        ELSE
            IF v_product_id IS NULL THEN
                v_product_id := NEXTVAL('BL_3NF.seq_product_id');
            END IF;

            INSERT INTO BL_3NF.CE_Products_SCD (
                product_id, product_name, subcategory_id, brand_id,
                product_rating, start_dt, end_dt, is_active,
                ta_insert_dt, product_src_id, source_system, source_entity
            )
            VALUES (
                v_product_id,
                rec.fn_product_name,
                rec.fn_subcategory_id,
                rec.fn_brand_id,
                rec.fn_product_rating,
                v_current_ts,
                v_max_ts,
                'Y',
                v_current_ts,
                rec.fn_product_src_id,
                rec.fn_source_system,
                rec.fn_source_entity
            );

            v_rows_affected := v_rows_affected + 1;
        END IF;
    END LOOP;

    v_end_time := clock_timestamp();

    CALL BL_CL.prc_log_insert(
        'prc_load_ce_products_scd',
        v_rows_affected,
        CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END,
        v_end_time - v_start_time
    );

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
    CALL BL_CL.prc_log_insert(
        'prc_load_ce_products_scd',
        v_rows_affected,
        'Error: ' || SQLERRM,
        v_end_time - v_start_time
    );
    RAISE;
END;
$$;