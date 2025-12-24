CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_dim_shippings()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    v_rows_affected INTEGER := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
BEGIN

FOR rec IN
    SELECT 
        COALESCE(sh.shipping_id, -1) AS shipping_id,
        COALESCE(sh.shipping_method, 'n. a.') AS shipping_method,
        COALESCE(sh.shipping_price, -1) AS shipping_price,
        COALESCE(ad.address_id, -1) AS address_id,
        COALESCE(ad.address_name, 'n. a.') AS address_name,
        COALESCE(ct.city_id, -1) AS city_id,
        COALESCE(ct.city_name, 'n. a.') AS city_name,
        COALESCE(st.state_id, -1) AS state_id,
        COALESCE(st.state_name, 'n. a.') AS state_name,
        COALESCE(sh.ta_insert_dt, NOW()) AS ta_insert_dt,
        COALESCE(sh.ta_update_dt, NOW()) AS ta_update_dt,
        'BL_3NF' AS source_system,
        'BL_3NF.CE_SHIPPINGS' AS source_entity
    FROM BL_3NF.CE_Shippings sh
    left JOIN BL_3NF.CE_Addresses ad ON sh.address_id = ad.address_id
    left JOIN BL_3NF.CE_Cities ct ON ad.city_id = ct.city_id
    left JOIN BL_3NF.CE_States st ON ct.state_id = st.state_id
	where sh.shipping_id != -1 AND
    NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Shippings dsh WHERE dsh.Shipping_SRC_ID = sh.shipping_id::varchar AND dsh.source_system = 'BL_3NF' AND dsh.source_entity = 'BL_3NF.CE_SHIPPINGS' )
       
LOOP
    INSERT INTO BL_DM.DIM_Shippings (
        Shipping_SURR_ID,
        Shipping_SRC_ID,
        Shipping_Method,
        Shipping_Price,
        Shipping_Address_ID,
        Shipping_Address_Name,
        Shipping_City_ID,
        Shipping_City_Name,
        Shipping_State_ID,
        Shipping_State_Name,
        TA_Insert_DT,
        TA_Update_DT,
        source_system,
        source_entity)
    VALUES (
        NEXTVAL('BL_DM.seq_shipping_surr_id'),
        rec.shipping_id,
        rec.shipping_method,
        rec.shipping_price,
        rec.address_id,
        rec.address_name,
        rec.city_id,
        rec.city_name,
        rec.state_id,
        rec.state_name,
        clock_timestamp(),
        clock_timestamp(),
        rec.source_system,
        rec.source_entity);

    v_rows_affected := v_rows_affected + 1;
END LOOP;

v_end_time := clock_timestamp();

CALL BL_CL.prc_log_insert('prc_load_dim_shippings', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );
    

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
    CALL BL_CL.prc_log_insert( 'prc_load_dim_shippings', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;

