CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE BL_CL.prc_load_dim_customers_scd()
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
            COALESCE(c.customer_id::varchar, 'n. a.') AS customer_src_id,
            COALESCE(c.customer_firstname, 'n. a.') AS customer_firstname,
            COALESCE(c.customer_lastname, 'n. a.') AS customer_lastname,
            COALESCE(c.customer_gender, 'n. a') AS customer_gender,
            COALESCE(c.customer_email, 'n. a.') AS customer_email,
            COALESCE(c.customer_phone, 'n. a.') AS customer_phone,
            COALESCE(c.customer_age, -1) AS customer_age,
			COALESCE(c.customer_signup_dt, current_date) AS customer_signup_dt,
            COALESCE(c.start_dt, NOW()) AS start_dt,
            COALESCE(c.end_dt, NOW()) AS end_dt,
            NOW() AS ta_insert_dt,
            'BL_3NF' AS source_system,
            'BL_3NF.CE_CUSTOMERS_SCD' AS source_entity
        FROM BL_3NF.CE_CUSTOMERS_SCD c where c.customer_id != -1
    LOOP
        IF EXISTS (
            SELECT 1
            FROM BL_DM.DIM_Customers_SCD dc
            WHERE dc.customer_src_id = rec.customer_src_id::varchar
              AND dc.source_system = rec.source_system
              AND dc.source_entity = rec.source_entity
              AND dc.is_active = 'Y'
              AND (
                    dc.customer_firstname IS DISTINCT FROM rec.customer_firstname OR
                    dc.customer_lastname IS DISTINCT FROM  rec.customer_lastname OR
                    dc.customer_gender IS DISTINCT FROM  rec.customer_gender OR
                    dc.customer_email IS DISTINCT FROM  rec.customer_email OR
                    dc.customer_phone IS DISTINCT FROM  rec.customer_phone OR
                    dc.customer_age IS DISTINCT FROM  rec.customer_age OR
					dc.customer_signup_dt IS DISTINCT FROM  rec.customer_signup_dt)) 
			THEN
            UPDATE BL_DM.DIM_Customers_SCD
            SET end_dt = rec.start_dt - INTERVAL '1 second' , is_active = 'N'
            WHERE customer_src_id = rec.customer_src_id::varchar AND source_system = rec.source_system AND source_entity = rec.source_entity AND is_active = 'Y';

            INSERT INTO BL_DM.DIM_Customers_SCD (
                customer_surr_id,
                customer_src_id,
                customer_firstname,
                customer_lastname,
                customer_gender,
                customer_email,
                customer_phone,
                customer_age,
			    customer_signup_dt,
                start_dt,
                end_dt,
                is_active,
                ta_insert_dt,
                source_system,
                source_entity
            )
            VALUES (
                NEXTVAL('bl_dm.seq_customer_surr_id'),
                rec.customer_src_id::varchar,
                rec.customer_firstname,
                rec.customer_lastname,
                rec.customer_gender,
                rec.customer_email,
                rec.customer_phone,
                rec.customer_age,
				rec.customer_signup_dt,
                rec.start_dt,
                rec.end_dt,
                'Y',
                rec.ta_insert_dt,
                rec.source_system,
                rec.source_entity
            );
            v_rows_updated := v_rows_updated + 1;

        ELSIF NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Customers_SCD dc WHERE dc.customer_src_id= rec.customer_src_id AND dc.source_system = rec.source_system AND dc.source_entity = rec.source_entity AND dc.start_dt = rec.start_dt)
        THEN
             INSERT INTO BL_DM.DIM_Customers_SCD (
                customer_surr_id,
                customer_src_id,
                customer_firstname,
                customer_lastname,
                customer_gender,
                customer_email,
                customer_phone,
                customer_age,
			    customer_signup_dt,
                start_dt,
                end_dt,
                is_active,
                ta_insert_dt,
                source_system,
                source_entity
            )
            VALUES (
                NEXTVAL('bl_dm.seq_customer_surr_id'),
                rec.customer_src_id::varchar,
                rec.customer_firstname,
                rec.customer_lastname,
                rec.customer_gender,
                rec.customer_email,
                rec.customer_phone,
                rec.customer_age,
				rec.customer_signup_dt,
                rec.start_dt,
                rec.end_dt,
                'Y',
                rec.ta_insert_dt,
                rec.source_system,
                rec.source_entity
            );
            v_rows_inserted := v_rows_inserted + 1;
        END IF;
    END LOOP;

 v_end_time := clock_timestamp();


CALL BL_CL.prc_log_insert( 'prc_load_dim_customers_scd', v_rows_inserted + v_rows_updated, CASE WHEN (v_rows_inserted + v_rows_updated) > 0 THEN 'Loaded successfully' ELSE 'No new or changed data to load' END, v_end_time - v_start_time );
   
EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert( 'prc_load_dim_customers_scd', v_rows_inserted + v_rows_updated, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;


