CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE bl_cl.load_ce_transactions()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_inserted INT := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_execution_time INTERVAL;
BEGIN
    INSERT INTO BL_3NF.CE_Transactions (
        transaction_id,
        product_id,
        employee_id,
        shipping_id,
        store_id,
        customer_id,
        payment_type_id,
        supplier_id,
        event_dt,
        transaction_price,
        quantity_cnt,
        revenue_tot_amt,
        cost_amt,
        ta_insert_dt,
        transaction_src_id,
        source_system,
        source_entity
    )
    SELECT
        NEXTVAL('BL_3NF.seq_transaction_id'),
        COALESCE(p.product_id, -1),
        COALESCE(e.employee_id, -1),
        COALESCE(sh.shipping_id, -1),
        COALESCE(st.store_id, -1),
        COALESCE(c.customer_id, -1),
        COALESCE(pt.payment_type_id, -1),
        COALESCE(sp.supplier_id, -1),
        COALESCE(t.date::timestamp, now()),
        COALESCE(t.price::NUMERIC, -1),
        COALESCE(t.quantity_sold::INTEGER, -1),
        COALESCE(t.amount_sold::NUMERIC, -1),
        COALESCE(t.cost_amount::NUMERIC, -1),
        now(),
        t.transaction_id,
        t.source_system,
        t.source_entity
    FROM (
        SELECT
            transaction_id,
            product_id AS product_src_id,
            NULL AS employee_src_id,
            concat(shipping_method, '|', shipping_address) AS shipping_src_id,
            store_id AS store_src_id,
            customer_id AS customer_src_id,
            payment_method AS payment_type_src_id,
            supplier_id AS supplier_src_id,
            date,
            price,
            quantity_sold,
            amount_sold,
            cost_amount,
            'SA_ONLINE_SALES' AS source_system,
            'SRC_ONLINE_SALES' AS source_entity
        FROM sa_online_sales.src_online_sales

        UNION ALL

        SELECT
            transaction_id,
            product_id AS product_src_id,
            employee_id AS employee_src_id,
            NULL AS shipping_src_id,
            store_id AS store_src_id,
            NULL AS customer_src_id,
            payment_method AS payment_type_src_id,
            supplier_id AS supplier_src_id,
            date,
            price,
            quantity_sold,
            amount_sold,
            cost_amount,
            'SA_OFFLINE_SALES' AS source_system,
            'SRC_OFFLINE_SALES' AS source_entity
        FROM sa_offline_sales.src_offline_sales
    ) t
    LEFT JOIN BL_3NF.CE_Products_SCD p ON p.product_src_id = t.product_src_id AND p.source_system = t.source_system AND p.source_entity = t.source_entity AND p.is_active='Y'
    LEFT JOIN BL_3NF.CE_Employees_SCD e ON e.employee_src_id = t.employee_src_id AND e.source_system = t.source_system AND e.source_entity = t.source_entity AND e.is_active='Y'     
    LEFT JOIN BL_3NF.CE_Shippings sh ON sh.shipping_src_id = t.shipping_src_id AND sh.source_system = t.source_system AND sh.source_entity = t.source_entity     
    LEFT JOIN BL_3NF.CE_Stores st ON st.store_src_id = t.store_src_id AND st.source_system = t.source_system AND st.source_entity = t.source_entity       
    LEFT JOIN BL_3NF.CE_Customers_SCD c ON c.customer_src_id = t.customer_src_id AND c.source_system = t.source_system AND c.source_entity = t.source_entity AND c.is_active='Y'     
    LEFT JOIN BL_3NF.CE_PaymentTypes pt ON pt.payment_type_src_id = t.payment_type_src_id AND pt.source_system = t.source_system AND pt.source_entity = t.source_entity       
    LEFT JOIN BL_3NF.CE_Suppliers sp ON sp.supplier_src_id = t.supplier_src_id AND sp.source_system = t.source_system AND sp.source_entity = t.source_entity
      
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_transactions ct WHERE ct.transaction_src_id = t.transaction_id AND ct.source_system = t.source_system AND ct.source_entity = t.source_entity );
      
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    v_execution_time := clock_timestamp() - v_start_time;

    CALL bl_cl.prc_log_insert('load_ce_transactions', rows_inserted, 'Procedure completed successfully', v_execution_time);

EXCEPTION WHEN OTHERS THEN v_execution_time := clock_timestamp() - v_start_time;
     CALL bl_cl.prc_log_insert( 'load_ce_transactions', NULL, 'Error: ' || SQLERRM, v_execution_time ); RAISE;
END;
$$;
