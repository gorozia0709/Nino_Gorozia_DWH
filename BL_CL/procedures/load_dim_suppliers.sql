CREATE SCHEMA IF NOT EXISTS BL_CL;




CREATE OR REPLACE PROCEDURE BL_CL.prc_load_dim_suppliers()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
BEGIN
    MERGE INTO BL_DM.DIM_Suppliers tgt
    USING (
        SELECT 
            COALESCE(s.supplier_id,-1) AS supplier_id,
            COALESCE(s.supplier_name, 'n. a.') AS supplier_name,
            COALESCE(s.supplier_email, 'n. a.') AS supplier_email,
            COALESCE(s.supplier_contract_start_dt, current_date) AS supplier_contract_start_dt,
            COALESCE(s.supplier_contract_end_dt, current_date) AS supplier_contract_end_dt,
            COALESCE(s.ta_insert_dt, NOW()) AS ta_insert_dt,
            COALESCE(s.ta_update_dt, NOW()) AS ta_update_dt,
            'BL_3NF' AS source_system,
            'BL_3NF.CE_SUPPLIERS' AS source_entity
        FROM BL_3NF.CE_Suppliers s
		WHERE s.supplier_id != -1
    ) src
    ON (
        tgt.Supplier_SRC_ID = src.supplier_id::varchar
        AND tgt.source_system = src.source_system
        AND tgt.source_entity = src.source_entity
    )
    WHEN MATCHED AND (
        tgt.Supplier_Name IS DISTINCT FROM src.supplier_name OR
        tgt.Supplier_Email IS DISTINCT FROM src.supplier_email OR
        tgt.Supplier_Contract_Start_DT IS DISTINCT FROM src.supplier_contract_start_dt OR
        tgt.Supplier_Contract_End_DT IS DISTINCT FROM src.supplier_contract_end_dt
    )
    THEN UPDATE SET
        Supplier_Name = src.supplier_name,
        Supplier_Email = src.supplier_email,
        Supplier_Contract_Start_DT = src.supplier_contract_start_dt,
        Supplier_Contract_End_DT = src.supplier_contract_end_dt,
        TA_Update_DT = clock_timestamp()

    WHEN NOT MATCHED THEN
        INSERT (
            Supplier_SURR_ID, Supplier_SRC_ID, Supplier_Name, Supplier_Email,
            Supplier_Contract_Start_DT, Supplier_Contract_End_DT,
            TA_Insert_DT, TA_Update_DT, source_system, source_entity
        )
        VALUES (
            NEXTVAL('BL_DM.seq_supplier_surr_id'), src.supplier_id, src.supplier_name, src.supplier_email,
            src.supplier_contract_start_dt, src.supplier_contract_end_dt,
            clock_timestamp(), clock_timestamp(),
            src.source_system, src.source_entity
        );

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := clock_timestamp();

    CALL BL_CL.prc_log_insert(
        'prc_load_dim_suppliers',
        v_rows_affected,
        CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END,
        v_end_time - v_start_time
    );

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
    CALL BL_CL.prc_log_insert(
        'prc_load_dim_suppliers',
        v_rows_affected,
        'Error: ' || SQLERRM,
        v_end_time - v_start_time
    );
    RAISE;
END;
$$;





