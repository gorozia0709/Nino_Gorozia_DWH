CREATE SCHEMA IF NOT EXISTS BL_CL;



--loading last 3 months data in dim facts table using partitions
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_FCT_TRANSACTIONS_DD()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_rows_inserted INTEGER := 0;
    v_rows_inserted INTEGER := 0;

    v_start_date DATE;
    v_end_date DATE;
    v_current_start DATE;
    v_current_end DATE;

    v_partition_name TEXT;
    v_quarter INTEGER;
    v_year INTEGER;
    v_log_message TEXT;
    v_current_table_name TEXT := 'FCT_TRANSACTIONS_DD';
    v_start_time TIMESTAMP := clock_timestamp();

    v_rolling_window_start DATE;
    v_rolling_window_end DATE;
    v_rolling_window_months INTEGER := 3;
    v_old_partition_exists BOOLEAN;
    rec RECORD;
    v_detached_partitions TEXT := '';
    v_detached_count INTEGER := 0;
BEGIN
    SELECT MIN(ct.EVENT_DT::DATE), MAX(ct.EVENT_DT::DATE)
    INTO v_start_date, v_end_date
    FROM BL_3NF.CE_TRANSACTIONS ct;

    IF v_start_date IS NULL THEN
        CALL BL_CL.prc_log_insert('LOAD_FCT_TRANSACTIONS_DD', 0, 'No data found in source table. Exiting.', NULL);
        RETURN;
    END IF;

    v_rolling_window_end := date_trunc('month', v_end_date) + INTERVAL '1 month' - INTERVAL '1 day';
    v_rolling_window_start := date_trunc('month', v_end_date - INTERVAL '2 month');

    v_current_start := date_trunc('quarter', v_start_date);

    WHILE v_current_start <= v_end_date LOOP
        v_current_end := (v_current_start + INTERVAL '3 month' - INTERVAL '1 day')::DATE;
        v_quarter := EXTRACT(QUARTER FROM v_current_start);
        v_year := EXTRACT(YEAR FROM v_current_start);
        v_partition_name := 'fct_transactions_' || v_year || '_q' || v_quarter;

        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'bl_dm' 
            AND table_name = v_partition_name
        ) INTO v_old_partition_exists;

        BEGIN
            IF NOT v_old_partition_exists THEN
                EXECUTE format('CREATE TABLE BL_DM.%I PARTITION OF BL_DM.FCT_TRANSACTIONS_DD ' ||
                               'FOR VALUES FROM (%L) TO (%L)',
                               v_partition_name, v_current_start, v_current_end + INTERVAL '1 day');
            ELSE
                BEGIN
                    SELECT EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        JOIN pg_inherits i ON i.inhrelid = c.oid
                        JOIN pg_class parent ON parent.oid = i.inhparent
                        WHERE n.nspname = 'bl_dm'
                        AND parent.relname = 'fct_transactions_dd'
                        AND c.relname = v_partition_name
                    ) INTO v_old_partition_exists;

                    IF NOT v_old_partition_exists THEN
                        EXECUTE format('ALTER TABLE BL_DM.FCT_TRANSACTIONS_DD ATTACH PARTITION BL_DM.%I ' ||
                                       'FOR VALUES FROM (%L) TO (%L)',
                                       v_partition_name, v_current_start, v_current_end + INTERVAL '1 day');
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                END;
            END IF;
        EXCEPTION WHEN OTHERS THEN
        END;

        INSERT INTO BL_DM.FCT_TRANSACTIONS_DD (
            Transaction_ID,
            Store_SURR_ID,
            Product_SURR_ID,
            Supplier_SURR_ID,
            Employee_SURR_ID,
            Shipping_SURR_ID,
            Payment_Type_SURR_ID,
            Customer_SURR_ID,
            EVENT_DT,
            EVENT_TIME,
            Transaction_Price,
            Quantity_CNT,
            Revenue_TOT_AMT,
            Cost_AMT,
            TA_Insert_DT,
            transaction_src_id,
            source_system,
            source_entity
        )
        SELECT
            NEXTVAL('BL_DM.seq_transaction_surr_id'),
            COALESCE(ds.Store_SURR_ID, -1),
            COALESCE(dp.Product_SURR_ID, -1),
            COALESCE(dsp.Supplier_SURR_ID, -1),
            COALESCE(de.Employee_SURR_ID, -1),
            COALESCE(dsh.Shipping_SURR_ID, -1),
            COALESCE(dpt.Payment_Type_SURR_ID, -1),
            COALESCE(dc.Customer_SURR_ID, -1),
            ct.EVENT_DT::DATE,
            ct.EVENT_DT::TIME,
            ct.transaction_price,
            ct.quantity_CNT,
            ct.revenue_TOT_AMT,
            COALESCE(ct.cost_AMT, 0),
            ct.TA_INSERT_DT,
            ct.transaction_id::varchar,
            'BL_3NF' AS source_system,
            'BL_3NF.CE_TRANSACTIONS' AS source_entity
        FROM BL_3NF.CE_TRANSACTIONS ct
        LEFT JOIN BL_DM.DIM_Stores ds ON ct.store_id::VARCHAR = ds.store_src_id
        LEFT JOIN BL_DM.DIM_Products_SCD dp ON ct.product_id::VARCHAR = dp.Product_SRC_ID AND dp.Is_Active = 'Y'
        LEFT JOIN BL_DM.DIM_Suppliers dsp ON ct.supplier_id::VARCHAR = dsp.Supplier_SRC_ID
        LEFT JOIN BL_DM.DIM_Employees_SCD de ON ct.employee_id::VARCHAR = de.Employee_SRC_ID AND de.Is_Active = 'Y'
        LEFT JOIN BL_DM.DIM_Shippings dsh ON ct.shipping_id::VARCHAR = dsh.Shipping_SRC_ID
        LEFT JOIN BL_DM.DIM_PaymentTypes dpt ON ct.payment_type_id::VARCHAR = dpt.Payment_Type_SRC_ID
        LEFT JOIN BL_DM.DIM_Customers_SCD dc ON ct.customer_id::VARCHAR = dc.Customer_SRC_ID AND dc.Is_Active = 'Y'
		LEFT JOIN BL_DM.DIM_DATES dd ON dd.event_dt=ct.event_dt::date
		LEFT JOIN BL_DM.DIM_TIMES dt ON dt.event_time=ct.event_dt::TIME
        WHERE ct.EVENT_DT::DATE BETWEEN v_current_start AND v_current_end
        AND NOT EXISTS (
            SELECT 1 FROM BL_DM.FCT_TRANSACTIONS_DD existing
            WHERE existing.transaction_src_id = ct.transaction_id::varchar
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_total_rows_inserted := v_total_rows_inserted + v_rows_inserted;

        IF v_rows_inserted > 0 THEN
            CALL BL_CL.prc_log_insert('LOAD_FCT_TRANSACTIONS_DD', v_rows_inserted,
                'Load completed for period ' || v_current_start || ' to ' || v_current_end ||
                ' into partition ' || v_partition_name, NULL);
        END IF;

        v_current_start := v_current_start + INTERVAL '3 month';
    END LOOP;

    FOR rec IN 
        SELECT 
            n.nspname as schemaname,
            c.relname as tablename
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE n.nspname = 'bl_dm'
        AND parent.relname = 'fct_transactions_dd'
        AND c.relname LIKE 'fct_transactions_%'
    LOOP
        BEGIN
            v_year := substring(rec.tablename from 'fct_transactions_(\d{4})_q\d')::INTEGER;
            v_quarter := substring(rec.tablename from 'fct_transactions_\d{4}_q(\d)')::INTEGER;
            v_current_start := make_date(v_year, (v_quarter - 1) * 3 + 1, 1);

            IF v_current_start < v_rolling_window_start THEN
                EXECUTE format('ALTER TABLE BL_DM.FCT_TRANSACTIONS_DD DETACH PARTITION BL_DM.%I', rec.tablename);
                EXECUTE format('DROP TABLE IF EXISTS BL_DM.%I', rec.tablename);

                IF v_detached_partitions = '' THEN
                    v_detached_partitions := rec.tablename || ' (period: ' || v_current_start || ')';
                ELSE
                    v_detached_partitions := v_detached_partitions || ', ' || rec.tablename || ' (period: ' || v_current_start || ')';
                END IF;
                v_detached_count := v_detached_count + 1;
            END IF;
        EXCEPTION WHEN OTHERS THEN
        END;
    END LOOP;

    IF v_detached_count > 0 THEN
        CALL BL_CL.prc_log_insert('LOAD_FCT_TRANSACTIONS_DD', 0,
            'Detached and dropped ' || v_detached_count || ' old partitions: ' || v_detached_partitions, NULL);
    END IF;

EXCEPTION WHEN OTHERS THEN
    v_log_message := 'Error in LOAD_FCT_TRANSACTIONS_DD: ' || SQLERRM;
    CALL BL_CL.prc_log_insert('LOAD_FCT_TRANSACTIONS_DD', 0, v_log_message, clock_timestamp() - v_start_time);
END;
$$;




--this is case if we want to load with partitions and we want to load all data
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_FCT_TRANSACTIONS_DD()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_rows_inserted INTEGER := 0;
    v_rows_inserted INTEGER := 0;

    v_start_date DATE;
    v_end_date DATE;
    v_current_start DATE;
    v_current_end DATE;

    v_partition_name TEXT;
    v_quarter INTEGER;
    v_year INTEGER;
    v_log_message TEXT;
    v_start_time TIMESTAMP := clock_timestamp();

    v_partition_exists BOOLEAN;
BEGIN
    SELECT MIN(ct.EVENT_DT::DATE), MAX(ct.EVENT_DT::DATE)
    INTO v_start_date, v_end_date
    FROM BL_3NF.CE_TRANSACTIONS ct;

    IF v_start_date IS NULL THEN
        CALL BL_CL.prc_log_insert('LOAD_FCT_TRANSACTIONS_DD', 0, 'No data found in source table. Exiting.', NULL);
        RETURN;
    END IF;

    v_current_start := date_trunc('quarter', v_start_date);

    WHILE v_current_start <= v_end_date LOOP
        v_current_end := (v_current_start + INTERVAL '3 month' - INTERVAL '1 day')::DATE;
        v_quarter := EXTRACT(QUARTER FROM v_current_start);
        v_year := EXTRACT(YEAR FROM v_current_start);
        v_partition_name := 'fct_transactions_' || v_year || '_q' || v_quarter;

        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'bl_dm' AND table_name = v_partition_name
        ) INTO v_partition_exists;

        IF NOT v_partition_exists THEN
            BEGIN
                EXECUTE format('CREATE TABLE BL_DM.%I PARTITION OF BL_DM.FCT_TRANSACTIONS_DD ' ||
                               'FOR VALUES FROM (%L) TO (%L)',
                               v_partition_name, v_current_start, v_current_end + INTERVAL '1 day');
            EXCEPTION WHEN OTHERS THEN
                EXECUTE format('ALTER TABLE BL_DM.FCT_TRANSACTIONS_DD ATTACH PARTITION BL_DM.%I ' ||
                               'FOR VALUES FROM (%L) TO (%L)',
                               v_partition_name, v_current_start, v_current_end + INTERVAL '1 day');
            END;
        END IF;

        INSERT INTO BL_DM.FCT_TRANSACTIONS_DD (
            Transaction_ID,
            Store_SURR_ID,
            Product_SURR_ID,
            Supplier_SURR_ID,
            Employee_SURR_ID,
            Shipping_SURR_ID,
            Payment_Type_SURR_ID,
            Customer_SURR_ID,
            EVENT_DT,
            EVENT_TIME,
            Transaction_Price,
            Quantity_CNT,
            Revenue_TOT_AMT,
            Cost_AMT,
            TA_Insert_DT,
            transaction_src_id,
            source_system,
            source_entity
        )
        SELECT
            NEXTVAL('BL_DM.seq_transaction_surr_id'),
            COALESCE(ds.Store_SURR_ID, -1),
            COALESCE(dp.Product_SURR_ID, -1),
            COALESCE(dsp.Supplier_SURR_ID, -1),
            COALESCE(de.Employee_SURR_ID, -1),
            COALESCE(dsh.Shipping_SURR_ID, -1),
            COALESCE(dpt.Payment_Type_SURR_ID, -1),
            COALESCE(dc.Customer_SURR_ID, -1),
            ct.EVENT_DT::DATE,
            ct.EVENT_DT::TIME,
            ct.transaction_price,
            ct.quantity_CNT,
            ct.revenue_TOT_AMT,
            COALESCE(ct.cost_AMT, 0),
            ct.TA_INSERT_DT,
             ct.transaction_id::varchar,
            'BL_3NF' AS source_system,
            'BL_3NF.CE_TRANSACTIONS' AS source_entity
        FROM BL_3NF.CE_TRANSACTIONS ct
        LEFT JOIN BL_DM.DIM_Stores ds ON ct.store_id::VARCHAR = ds.store_src_id
        LEFT JOIN BL_DM.DIM_Products_SCD dp ON ct.product_id::VARCHAR = dp.Product_SRC_ID AND dp.Is_Active = 'Y'
        LEFT JOIN BL_DM.DIM_Suppliers dsp ON ct.supplier_id::VARCHAR = dsp.Supplier_SRC_ID
        LEFT JOIN BL_DM.DIM_Employees_SCD de ON ct.employee_id::VARCHAR = de.Employee_SRC_ID AND de.Is_Active = 'Y'
        LEFT JOIN BL_DM.DIM_Shippings dsh ON ct.shipping_id::VARCHAR = dsh.Shipping_SRC_ID
        LEFT JOIN BL_DM.DIM_PaymentTypes dpt ON ct.payment_type_id::VARCHAR = dpt.Payment_Type_SRC_ID
        LEFT JOIN BL_DM.DIM_Customers_SCD dc ON ct.customer_id::VARCHAR = dc.Customer_SRC_ID AND dc.Is_Active = 'Y'
	    LEFT JOIN BL_DM.DIM_DATES dd ON dd.event_dt=ct.event_dt::date
		LEFT JOIN BL_DM.DIM_TIMES dt ON dt.event_time=ct.event_dt::TIME
        WHERE ct.EVENT_DT::DATE BETWEEN v_current_start AND v_current_end
        AND NOT EXISTS (
            SELECT 1 FROM BL_DM.FCT_TRANSACTIONS_DD existing
            WHERE existing.transaction_src_id = ct.transaction_id::varchar
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_total_rows_inserted := v_total_rows_inserted + v_rows_inserted;

        IF v_rows_inserted > 0 THEN
            CALL BL_CL.prc_log_insert(
                'LOAD_FCT_TRANSACTIONS_DD',
                v_rows_inserted,
                'load completed for period ' || v_current_start || ' to ' || v_current_end ||
                ' into partition ' || v_partition_name,
                NULL
            );
        END IF;

        v_current_start := v_current_start + INTERVAL '3 month';
    END LOOP;

EXCEPTION WHEN OTHERS THEN
    v_log_message := 'Error in LOAD_FCT_TRANSACTIONS_DD: ' || SQLERRM;
    CALL BL_CL.prc_log_insert('LOAD_FCT_TRANSACTIONS_DD', 0, v_log_message, clock_timestamp() - v_start_time);
END;
$$;
