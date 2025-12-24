CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE BL_CL.prc_load_dim_employees_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_rows_inserted INT := 0;
    v_rows_updated INT := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
	
    FOR rec IN
        SELECT 
            COALESCE(e.employee_id::varchar, 'n. a.') AS employee_src_id,
            COALESCE(e.employee_firstname, 'n. a.') AS employee_firstname,
            COALESCE(e.employee_lastname, 'n. a.') AS employee_lastname,
            COALESCE(e.employee_gender, 'n. a.') AS employee_gender,
            COALESCE(e.employee_email, 'n. a.') AS employee_email,
            COALESCE(e.employee_hire_dt, current_date) AS employee_hire_dt,
            COALESCE(e.store_id, -1) AS store_id,  
            e.start_dt,
            e.end_dt,
            clock_timestamp() AS ta_insert_dt,
            'BL_3NF' AS source_system,
            'BL_3NF.CE_Employees_SCD' AS source_entity
        FROM BL_3NF.CE_Employees_SCD e where e.employee_id != -1
    LOOP
        IF EXISTS (
            SELECT 1
            FROM BL_DM.DIM_Employees_SCD de
            WHERE de.employee_src_id = rec.employee_src_id
              AND de.source_system = rec.source_system
              AND de.source_entity = rec.source_entity
              AND de.is_active = 'Y'
              AND (
                    de.employee_firstname IS DISTINCT FROM rec.employee_firstname OR
                    de.employee_lastname IS DISTINCT FROM rec.employee_lastname OR
                    de.employee_gender IS DISTINCT FROM rec.employee_gender OR
                    de.employee_email IS DISTINCT FROM rec.employee_email OR
                    de.employee_hire_dt IS DISTINCT FROM rec.employee_hire_dt 
                    )) 
			THEN
            UPDATE BL_DM.DIM_Employees_SCD SET end_dt = rec.start_dt - INTERVAL '1 day', is_active = 'N'
            WHERE employee_src_id = rec.employee_src_id AND source_system = rec.source_system AND source_entity = rec.source_entity AND is_active = 'Y';

            INSERT INTO BL_DM.DIM_Employees_SCD (
                employee_surr_id,
                employee_src_id,
                employee_firstname,
                employee_lastname,
                employee_gender,
                employee_email,
                employee_hire_dt,
                start_dt,
                end_dt,
                is_active,
                ta_insert_dt,
                source_system,
                source_entity)
            VALUES (
                NEXTVAL('bl_dm.seq_employee_surr_id'),
                rec.employee_src_id,
                rec.employee_firstname,
                rec.employee_lastname,
                rec.employee_gender,
                rec.employee_email,
                rec.employee_hire_dt,
                rec.start_dt,
                rec.end_dt,
                'Y',
                rec.ta_insert_dt,
                rec.source_system,
                rec.source_entity);
            v_rows_updated := v_rows_updated + 1;

        ELSIF NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Employees_SCD de WHERE de.employee_src_id = rec.employee_src_id AND de.source_system = rec.source_system AND de.source_entity = rec.source_entity AND de.start_dt = rec.start_dt )
           THEN
            INSERT INTO BL_DM.DIM_Employees_SCD (
                employee_surr_id,
                employee_src_id,
                employee_firstname,
                employee_lastname,
                employee_gender,
                employee_email,
                employee_hire_dt,
                start_dt,
                end_dt,
                is_active,
                ta_insert_dt,
                source_system,
                source_entity)
            VALUES (
                NEXTVAL('bl_dm.seq_employee_surr_id'),
                rec.employee_src_id,
                rec.employee_firstname,
                rec.employee_lastname,
                rec.employee_gender,
                rec.employee_email,
                rec.employee_hire_dt,
                rec.start_dt,
                rec.end_dt,
                'Y',
                rec.ta_insert_dt,
                rec.source_system,
                rec.source_entity);
            v_rows_inserted := v_rows_inserted + 1;
        END IF;
    END LOOP;

    v_end_time := clock_timestamp();

  CALL BL_CL.prc_log_insert( 'prc_load_dim_employees_scd', v_rows_inserted + v_rows_updated, CASE WHEN (v_rows_inserted + v_rows_updated) > 0 THEN 'Loaded successfully' ELSE 'No new or changed data to load' END, v_end_time - v_start_time );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert( 'prc_load_dim_employees_scd', v_rows_inserted + v_rows_updated, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;
