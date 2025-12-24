CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_employees_scd() 
LANGUAGE plpgsql
AS $$
DECLARE
   rec RECORD;
   v_rows_affected INTEGER := 0;
   v_start_time TIMESTAMP := clock_timestamp();
   v_end_time TIMESTAMP;
   v_existing_record RECORD;
   v_store_id BIGINT;
   v_current_timestamp TIMESTAMP := clock_timestamp();
   v_max_timestamp TIMESTAMP := TIMESTAMP '9999-12-31 00:00:00';
   v_employee_id BIGINT;
BEGIN

   FOR rec IN SELECT * FROM BL_CL.get_employees_for_scd()
   LOOP
       v_store_id := rec.store_id;

       SELECT * INTO v_existing_record
       FROM BL_3NF.CE_Employees_SCD e
       WHERE e.employee_src_id = rec.employee_src_id 
         AND e.source_system = rec.source_system 
         AND e.source_entity = rec.source_entity 
         AND e.is_active = 'Y' 
         AND e.end_dt = v_max_timestamp;

       SELECT employee_id INTO v_employee_id
       FROM BL_3NF.CE_Employees_SCD
       WHERE employee_src_id = rec.employee_src_id
         AND source_system = rec.source_system
         AND source_entity = rec.source_entity
       ORDER BY ta_insert_dt
       LIMIT 1;

       IF v_employee_id IS NULL THEN
           v_employee_id := NEXTVAL('BL_3NF.seq_employee_id');
       END IF;

       IF v_existing_record IS NOT NULL THEN
           IF v_existing_record.employee_firstname IS DISTINCT FROM rec.employee_firstname OR
              v_existing_record.employee_lastname  IS DISTINCT FROM rec.employee_lastname  OR
              v_existing_record.employee_gender    IS DISTINCT FROM rec.employee_gender    OR
              v_existing_record.employee_email     IS DISTINCT FROM rec.employee_email     OR
              v_existing_record.store_id           IS DISTINCT FROM v_store_id             OR
              v_existing_record.employee_hire_dt   IS DISTINCT FROM rec.employee_hire_dt THEN

               UPDATE BL_3NF.CE_Employees_SCD
               SET end_dt = v_current_timestamp - INTERVAL '1 second',
                   is_active = 'N'
               WHERE employee_id = v_existing_record.employee_id
                 AND start_dt = v_existing_record.start_dt;

               INSERT INTO BL_3NF.CE_Employees_SCD (
                   employee_id,
                   employee_firstname,
                   employee_lastname,
                   employee_gender,
                   employee_email,
                   store_id,
                   employee_hire_dt,
                   start_dt,
                   end_dt,
                   is_active,
                   ta_insert_dt,
                   employee_src_id,
                   source_system,
                   source_entity
               )
               VALUES (
                   v_employee_id, 
                   rec.employee_firstname,
                   rec.employee_lastname,
                   rec.employee_gender,
                   rec.employee_email,
                   v_store_id,
                   rec.employee_hire_dt,
                   v_current_timestamp,
                   v_max_timestamp,
                   'Y',
                   v_current_timestamp,
                   rec.employee_src_id,
                   rec.source_system,
                   rec.source_entity
               );

               v_rows_affected := v_rows_affected + 1;
           END IF;

       ELSE
           INSERT INTO BL_3NF.CE_Employees_SCD (
               employee_id,
               employee_firstname,
               employee_lastname,
               employee_gender,
               employee_email,
               store_id,
               employee_hire_dt,
               start_dt,
               end_dt,
               is_active,
               ta_insert_dt,
               employee_src_id,
               source_system,
               source_entity
           )
           VALUES (
               v_employee_id,  
               rec.employee_firstname,
               rec.employee_lastname,
               rec.employee_gender,
               rec.employee_email,
               v_store_id,
               rec.employee_hire_dt,
               v_current_timestamp,
               v_max_timestamp,
               'Y',
               v_current_timestamp,
               rec.employee_src_id,
               rec.source_system,
               rec.source_entity
           );

           v_rows_affected := v_rows_affected + 1;
       END IF;
   END LOOP;

   v_end_time := clock_timestamp();

   CALL BL_CL.prc_log_insert(
       'prc_load_ce_employees_scd',
       v_rows_affected,
       CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END,
       v_end_time - v_start_time
   );

EXCEPTION WHEN OTHERS THEN
   v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert(
       'prc_load_ce_employees_scd',
       v_rows_affected,
       'Error: ' || SQLERRM,
       v_end_time - v_start_time
   );
   RAISE;
END;
$$;
