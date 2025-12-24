CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_shippings()
LANGUAGE plpgsql
AS $$
DECLARE
   v_rows_affected INTEGER := 0;
   v_start_time TIMESTAMP := clock_timestamp();
   v_end_time TIMESTAMP;
BEGIN

INSERT INTO BL_3NF.CE_Shippings ( 
    shipping_id,
    shipping_src_id,
    shipping_method,
    shipping_price,
    address_id,
    TA_INSERT_DT,
    TA_UPDATE_DT,
    source_system,
    source_entity
)
SELECT 
    NEXTVAL('BL_3NF.seq_shipping_id'),
    COALESCE(src.shipping_src_id, 'n. a.'),
    COALESCE(src.shipping_method, 'n. a.'),
    COALESCE(src.shipping_price::NUMERIC, 0.0),
    COALESCE(a.address_id,-1),
    now(),
    now(),
    'SA_ONLINE_SALES',
    'SRC_ONLINE_SALES'
FROM (
    SELECT DISTINCT ON (shipping_method, shipping_address)
        CONCAT(shipping_method, '|', shipping_address) AS shipping_src_id,
        shipping_method,
        shipping_price,
        shipping_address
    FROM sa_online_sales.src_online_sales
) src
LEFT JOIN BL_3NF.CE_Addresses a 
    ON a.address_src_id = src.shipping_address 
   AND a.source_system = 'SA_ONLINE_SALES' 
   AND a.source_entity = 'SRC_ONLINE_SALES'
WHERE NOT EXISTS (
    SELECT 1 
    FROM BL_3NF.CE_Shippings s
    WHERE s.shipping_src_id = src.shipping_src_id
      AND s.source_system = 'SA_ONLINE_SALES'
      AND s.source_entity = 'SRC_ONLINE_SALES'
);
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    v_end_time := clock_timestamp();
	
   CALL BL_CL.prc_log_insert( 'prc_load_ce_shippings', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert( 'prc_load_ce_shippings', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;

