CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_subcategories()
LANGUAGE plpgsql
AS $$
DECLARE
   v_rows_affected INTEGER := 0;
   v_start_time TIMESTAMP := clock_timestamp();
   v_end_time TIMESTAMP;
BEGIN

WITH combined AS (
    SELECT DISTINCT ON (subcategory)
        subcategory AS subcategory_value,
        category AS category_src_id,
        'SA_OFFLINE_SALES' AS source_system, 
        'SRC_OFFLINE_SALES' AS source_entity
    FROM sa_offline_sales.src_offline_sales
    WHERE subcategory IS NOT NULL

    UNION ALL

    SELECT DISTINCT ON (subcategory)
        subcategory AS subcategory_value,
        category AS category_src_id,
        'SA_ONLINE_SALES' AS source_system, 
        'SRC_ONLINE_SALES' AS source_entity
    FROM sa_online_sales.src_online_sales
    WHERE subcategory IS NOT NULL
)

INSERT INTO bl_3nf.ce_subcategories (
    subcategory_id,
    subcategory_name,
    category_id,  
    ta_insert_dt,
    ta_update_dt,
    subcategory_src_id,
    source_system,
    source_entity
)
SELECT
    NEXTVAL('bl_3nf.seq_subcategory_id'),
    COALESCE(c.subcategory_value, 'n. a.') AS subcategory_name,
    COALESCE(cc.category_id, -1) AS category_id,
    clock_timestamp(),
    clock_timestamp(),
    COALESCE(c.subcategory_value, 'n. a.'),
    COALESCE(c.source_system, 'MANUAL'),
    COALESCE(c.source_entity, 'MANUAL')
FROM combined c
LEFT JOIN bl_3nf.ce_categories cc 
    ON cc.category_src_id = c.category_src_id 
    AND cc.source_system = c.source_system 
    AND cc.source_entity = c.source_entity
WHERE NOT EXISTS (
    SELECT 1 
    FROM bl_3nf.ce_subcategories tgt
    WHERE tgt.subcategory_src_id = c.subcategory_value
      AND tgt.source_system = c.source_system
      AND tgt.source_entity = c.source_entity
);

GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

v_end_time := clock_timestamp();

CALL BL_CL.prc_log_insert( 'prc_load_ce_subcategories', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
    CALL BL_CL.prc_log_insert( 'prc_load_ce_subcategories', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;

