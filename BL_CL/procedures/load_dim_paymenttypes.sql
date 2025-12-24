CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE BL_CL.prc_load_dim_paymenttypes()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_rows_affected INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_sql TEXT;
BEGIN

    FOR rec IN
        SELECT 
            COALESCE(pt.payment_type_id,-1) AS payment_type_id,
            COALESCE(pt.payment_type_method, 'n. a.') AS payment_type_method,
            COALESCE(pt.ta_insert_dt, NOW()) AS ta_insert_dt,
            COALESCE(pt.ta_update_dt, NOW()) AS ta_update_dt, 
            'BL_3NF' AS source_system, 
            'BL_3NF.CE_PaymentTypes' AS source_entity     
        FROM BL_3NF.CE_PaymentTypes pt
		where pt.payment_type_id != -1 AND
        NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PaymentTypes dpt WHERE dpt.Payment_Type_SRC_ID = pt.payment_type_id::varchar AND dpt.source_system = 'BL_3NF' AND dpt.source_entity = 'BL_3NF.CE_PaymentTypes')           
    LOOP
        v_sql := 
            'INSERT INTO BL_DM.DIM_PaymentTypes (
                Payment_Type_SURR_ID,
                Payment_Type_SRC_ID,
                Payment_Type_Method,
                TA_Insert_DT,
                TA_Update_DT,
                source_system,
                source_entity
            ) VALUES (
                NEXTVAL(''BL_DM.seq_payment_type_surr_id''),
                $1, $2, $3, $4, $5, $6)';

        EXECUTE v_sql USING rec.payment_type_id,rec.payment_type_method,clock_timestamp(),clock_timestamp(),rec.source_system,rec.source_entity;
            
        v_rows_affected := v_rows_affected + 1;
    END LOOP;

    v_end_time := clock_timestamp();

    CALL BL_CL.prc_log_insert('prc_load_dim_paymenttypes', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );
       
EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert( 'prc_load_dim_paymenttypes', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;
