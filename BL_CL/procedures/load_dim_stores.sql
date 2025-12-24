CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE BL_CL.prc_load_dim_stores()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
BEGIN

    MERGE INTO BL_DM.DIM_Stores tgt
    USING (
        SELECT 
             COALESCE(st.store_id, -1) AS store_id,
             COALESCE(st.store_name, 'n. a.') AS store_name,
             COALESCE(st.store_type, 'n. a.') AS store_type,
             COALESCE(st.store_website, 'n. a.') AS store_website,
             COALESCE(ad.address_id, -1) AS address_id,
             COALESCE(ad.address_name, 'n. a.') AS address_name,
             COALESCE(ct.city_id, -1) AS city_id,
             COALESCE(ct.city_name, 'n. a.') AS city_name,
             COALESCE(s.state_id, -1) AS state_id,
             COALESCE(s.state_name, 'n. a.') AS state_name,
             COALESCE(st.ta_insert_dt, NOW()) AS ta_insert_dt,
             COALESCE(st.ta_update_dt, NOW()) AS ta_update_dt,
            'BL_3NF' AS source_system,
            'BL_3NF.CE_STORES' AS source_entity
        FROM BL_3NF.CE_Stores st
        LEFT JOIN BL_3NF.CE_Addresses ad ON st.address_id = ad.address_id
        LEFT JOIN BL_3NF.CE_Cities ct ON ad.city_id = ct.city_id
        LEFT JOIN BL_3NF.CE_States s ON ct.state_id = s.state_id
		WHERE st.store_id != -1
    ) src
    ON (tgt.store_src_id = src.store_id::varchar
        AND tgt.source_system = src.source_system
        AND tgt.source_entity = src.source_entity)
    WHEN MATCHED AND (
        tgt.store_name IS DISTINCT FROM src.store_name OR
        tgt.store_type IS DISTINCT FROM src.store_type OR
        tgt.store_website IS DISTINCT FROM src.store_website OR
        tgt.store_address_id IS DISTINCT FROM src.address_id OR
        tgt.store_address_name IS DISTINCT FROM src.address_name OR
        tgt.store_city_id IS DISTINCT FROM src.city_id OR
        tgt.store_city_name IS DISTINCT FROM src.city_name OR
        tgt.store_state_id IS DISTINCT FROM src.state_id OR
        tgt.store_state_name IS DISTINCT FROM src.state_name)
	THEN
        UPDATE SET
            store_name = src.store_name,
            store_type = src.store_type,
            store_website = src.store_website,
            store_address_id = src.address_id,
            store_address_name = src.address_name,
            store_city_id = src.city_id,
            store_city_name = src.city_name,
            store_state_id = src.state_id,
            store_state_name = src.state_name,
            ta_update_dt = clock_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (
            store_surr_id, store_src_id, store_name, store_type, store_website,
            store_address_id, store_address_name, store_city_id, store_city_name,
            store_state_id, store_state_name, ta_insert_dt, ta_update_dt,
            source_system, source_entity
        )
        VALUES (
            NEXTVAL('BL_DM.seq_store_surr_id'), src.store_id, src.store_name, src.store_type, src.store_website,
            src.address_id, src.address_name, src.city_id, src.city_name,
            src.state_id, src.state_name, clock_timestamp(), clock_timestamp(),
            src.source_system, src.source_entity
        );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := clock_timestamp();

    CALL BL_CL.prc_log_insert(
        'prc_load_dim_stores',
        v_rows_affected,
        CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END,
        v_end_time - v_start_time
    );

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
    CALL BL_CL.prc_log_insert(
        'prc_load_dim_stores',
        v_rows_affected,
        'Error: ' || SQLERRM,
        v_end_time - v_start_time
    );
    RAISE;
END;
$$;

