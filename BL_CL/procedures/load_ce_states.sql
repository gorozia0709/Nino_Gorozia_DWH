CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_states()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
BEGIN

    INSERT INTO BL_3NF.CE_States (
        state_id, state_name, TA_INSERT_DT, TA_UPDATE_DT, state_src_id, source_system, source_entity
    )
    SELECT 
        NEXTVAL('BL_3NF.seq_state_id'),
        COALESCE(state_value, 'n. a.') AS state_name,
        clock_timestamp(),
        clock_timestamp(),
        COALESCE(state_value, 'n. a'),
        COALESCE(source_system, 'MANUAL'),
        COALESCE(source_entity, 'MANUAL')
    FROM (
        SELECT DISTINCT store_state AS state_value, 			                    
                        'SA_OFFLINE_SALES' AS source_system, 
                        'SRC_OFFLINE_SALES' AS source_entity
        FROM sa_offline_sales.src_offline_sales
        WHERE store_state IS NOT NULL

        UNION

        SELECT DISTINCT shipping_state AS state_value, 
                        'SA_ONLINE_SALES' AS source_system, 
                        'SRC_ONLINE_SALES' AS source_entity
        FROM sa_online_sales.src_online_sales
        WHERE shipping_state IS NOT NULL
    ) AS src_combined
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_States tgt
        WHERE tgt.state_src_id = src_combined.state_value
          AND tgt.source_system = src_combined.source_system
          AND tgt.source_entity = src_combined.source_entity
    );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := clock_timestamp();
	
   CALL BL_CL.prc_log_insert( 'prc_load_ce_states', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
    CALL BL_CL.prc_log_insert( 'prc_load_ce_states', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;

