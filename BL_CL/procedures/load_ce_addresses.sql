CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_addresses()
LANGUAGE plpgsql
AS $$
DECLARE
   v_rows_affected INTEGER := 0;
   v_start_time TIMESTAMP := clock_timestamp();
   v_end_time TIMESTAMP;
BEGIN

INSERT INTO bl_3nf.ce_addresses (
    address_id,
    address_name,
    city_id,  
    ta_insert_dt,
    ta_update_dt,
    address_src_id,
    source_system,
    source_entity
)
SELECT
    NEXTVAL('bl_3nf.seq_address_id'),
    COALESCE(combined.address_value, 'n. a.') AS address_name,
    COALESCE(ci.city_id, -1) AS city_id,
    clock_timestamp(),
    clock_timestamp(),
    COALESCE(combined.address_value, 'n. a.'),
    COALESCE(combined.source_system, 'MANUAL'),
    COALESCE(combined.source_entity, 'MANUAL')
FROM (
    SELECT DISTINCT 
        store_address AS address_value,
        store_city AS city_src_id,
        'SA_OFFLINE_SALES' AS source_system, 
        'SRC_OFFLINE_SALES' AS source_entity
    FROM sa_offline_sales.src_offline_sales
    WHERE store_address IS NOT NULL

    UNION ALL

    SELECT DISTINCT 
        shipping_address AS address_value,
        shipping_city AS city_src_id,
        'SA_ONLINE_SALES' AS source_system, 
        'SRC_ONLINE_SALES' AS source_entity
    FROM sa_online_sales.src_online_sales
    WHERE shipping_address IS NOT NULL
) combined
LEFT JOIN bl_3nf.ce_cities ci 
    ON ci.city_src_id = combined.city_src_id 
    AND ci.source_system = combined.source_system 
    AND ci.source_entity = combined.source_entity
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_addresses tgt
    WHERE tgt.address_src_id = combined.address_value
      AND tgt.source_system = combined.source_system
      AND tgt.source_entity = combined.source_entity
);

GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

v_end_time := clock_timestamp();

CALL BL_CL.prc_log_insert( 'prc_load_ce_addresses', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert( 'prc_load_ce_addresses', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;


