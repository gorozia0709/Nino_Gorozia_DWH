CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE FUNCTION BL_CL.fn_get_new_customers_scd()
RETURNS TABLE (
    customer_src_id VARCHAR(250),
    customer_firstname VARCHAR(50),
    customer_lastname VARCHAR(50),
    customer_gender VARCHAR(6),
    customer_email VARCHAR(200),
    customer_phone VARCHAR(50),
    customer_age INTEGER,
    customer_signup_dt DATE,
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        COALESCE(src.customer_id, 'n. a.')::VARCHAR(250) AS customer_src_id,
        COALESCE(src.customer_firstname, 'n. a.')::VARCHAR(50),
        COALESCE(src.customer_lastname, 'n. a.')::VARCHAR(50),
        COALESCE(src.customer_gender, 'n. a.')::VARCHAR(6),
        COALESCE(src.customer_email, 'n. a.')::VARCHAR(200),
        COALESCE(src.customer_phone, 'n. a.')::VARCHAR(50),
        COALESCE(src.customer_age::INTEGER, -1),
        COALESCE(src.signup_date::DATE, CURRENT_DATE),
        'SA_ONLINE_SALES'::VARCHAR(50),
        'SRC_ONLINE_SALES'::VARCHAR(50)
    FROM sa_online_sales.src_online_sales src
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_Customers_SCD c
        WHERE COALESCE(c.customer_src_id, 'n. a.') = COALESCE(src.customer_id, 'n. a.')
          AND c.source_system = 'SA_ONLINE_SALES'
          AND c.source_entity = 'SRC_ONLINE_SALES'
          AND c.is_active = 'Y'
          AND COALESCE(c.customer_firstname, 'n. a.') = COALESCE(src.customer_firstname, 'n. a.')
          AND COALESCE(c.customer_lastname, 'n. a.') = COALESCE(src.customer_lastname, 'n. a.')
          AND COALESCE(c.customer_gender, 'n. a.') = COALESCE(src.customer_gender, 'n. a.')
          AND COALESCE(c.customer_email, 'n. a.') = COALESCE(src.customer_email, 'n. a.')
          AND COALESCE(c.customer_phone, 'n. a.') = COALESCE(src.customer_phone, 'n. a.')
          AND COALESCE(c.customer_age, -1) = COALESCE(src.customer_age::INTEGER, -1)
          AND COALESCE(c.customer_signup_dt, CURRENT_DATE) = COALESCE(src.signup_date::DATE, CURRENT_DATE)
    );
END;
$$;
