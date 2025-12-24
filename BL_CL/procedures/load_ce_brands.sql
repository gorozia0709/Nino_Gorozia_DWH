CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_brands()
LANGUAGE plpgsql
AS $$
DECLARE
   v_rows_affected INTEGER := 0;
   v_start_time TIMESTAMP := clock_timestamp();
   v_end_time TIMESTAMP;
BEGIN

    WITH combined_brands AS (
        SELECT DISTINCT brand AS brand_value,
                        'SA_OFFLINE_SALES' AS source_system, 
                        'SRC_OFFLINE_SALES' AS source_entity
        FROM sa_offline_sales.src_offline_sales
        WHERE brand IS NOT NULL

        UNION ALL

        SELECT DISTINCT brand AS brand_value, 
                        'SA_ONLINE_SALES' AS source_system, 
                        'SRC_ONLINE_SALES' AS source_entity
        FROM sa_online_sales.src_online_sales
        WHERE brand IS NOT NULL
    )

    INSERT INTO BL_3NF.CE_Brands (
        brand_id, brand_name, TA_INSERT_DT, TA_UPDATE_DT, brand_src_id, source_system, source_entity
    )
    SELECT 
        NEXTVAL('BL_3NF.seq_brand_id'),
        COALESCE(brand_value, 'n. a.') AS brand_name,
        clock_timestamp(),
        clock_timestamp(),
        COALESCE(brand_value, 'n. a.'),
        COALESCE(source_system, 'MANUAL'),
        COALESCE(source_entity, 'MANUAL')
    FROM combined_brands cb
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_Brands tgt
        WHERE tgt.brand_src_id = cb.brand_value
          AND tgt.source_system = cb.source_system
          AND tgt.source_entity = cb.source_entity
    );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    v_end_time := clock_timestamp();

CALL BL_CL.prc_log_insert( 'prc_load_ce_brands', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert( 'prc_load_ce_brands', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;
