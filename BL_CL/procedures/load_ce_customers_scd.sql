CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_customers_scd()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_rows_affected INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_existing_record RECORD;
    v_current_ts TIMESTAMP := clock_timestamp();
    v_max_ts TIMESTAMP := '9999-12-31 23:59:59'::TIMESTAMP;
    v_customer_id BIGINT;
BEGIN

    FOR rec IN SELECT * FROM BL_CL.fn_get_new_customers_scd()
    LOOP
        SELECT customer_id INTO v_customer_id
        FROM BL_3NF.CE_Customers_SCD
        WHERE customer_src_id = rec.customer_src_id
          AND source_system = rec.source_system
          AND source_entity = rec.source_entity
        ORDER BY ta_insert_dt
        LIMIT 1;

        SELECT * INTO v_existing_record
        FROM BL_3NF.CE_Customers_SCD
        WHERE customer_src_id = rec.customer_src_id
          AND source_system = rec.source_system
          AND source_entity = rec.source_entity
          AND is_active = 'Y'
          AND end_dt = v_max_ts;

        IF v_existing_record IS NOT NULL THEN
            IF v_existing_record.customer_firstname     IS DISTINCT FROM rec.customer_firstname OR
               v_existing_record.customer_lastname      IS DISTINCT FROM rec.customer_lastname OR
               v_existing_record.customer_gender        IS DISTINCT FROM rec.customer_gender OR
               v_existing_record.customer_email         IS DISTINCT FROM rec.customer_email OR
               v_existing_record.customer_phone         IS DISTINCT FROM rec.customer_phone OR
               v_existing_record.customer_age           IS DISTINCT FROM rec.customer_age OR
               v_existing_record.customer_signup_dt     IS DISTINCT FROM rec.customer_signup_dt THEN

                UPDATE BL_3NF.CE_Customers_SCD
                SET end_dt = v_current_ts - INTERVAL '1 second',
                    is_active = 'N'
                WHERE customer_id = v_existing_record.customer_id
                  AND start_dt = v_existing_record.start_dt;

                INSERT INTO BL_3NF.CE_Customers_SCD (
                    customer_id,
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
                    customer_src_id,
                    source_system,
                    source_entity
                ) VALUES (
                    v_existing_record.customer_id,
                    rec.customer_firstname,
                    rec.customer_lastname,
                    rec.customer_gender,
                    rec.customer_email,
                    rec.customer_phone,
                    rec.customer_age,
                    rec.customer_signup_dt,
                    v_current_ts,
                    v_max_ts,
                    'Y',
                    v_current_ts,
                    rec.customer_src_id,
                    rec.source_system,
                    rec.source_entity
                );

                v_rows_affected := v_rows_affected + 1;
            END IF;

        ELSE
            IF v_customer_id IS NULL THEN
                v_customer_id := NEXTVAL('BL_3NF.seq_customer_id');
            END IF;

            INSERT INTO BL_3NF.CE_Customers_SCD (
                customer_id,
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
                customer_src_id,
                source_system,
                source_entity
            ) VALUES (
                v_customer_id,
                rec.customer_firstname,
                rec.customer_lastname,
                rec.customer_gender,
                rec.customer_email,
                rec.customer_phone,
                rec.customer_age,
                rec.customer_signup_dt,
                v_current_ts,
                v_max_ts,
                'Y',
                v_current_ts,
                rec.customer_src_id,
                rec.source_system,
                rec.source_entity
            );

            v_rows_affected := v_rows_affected + 1;
        END IF;
    END LOOP;

    v_end_time := clock_timestamp();

CALL BL_CL.prc_log_insert( 'prc_load_ce_customers_scd', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
    CALL BL_CL.prc_log_insert( 'prc_load_ce_customers_scd', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;

