CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE FUNCTION BL_CL.get_employees_for_scd() 
RETURNS TABLE(
    employee_src_id VARCHAR(250),
    employee_firstname VARCHAR(50),
    employee_lastname VARCHAR(50),
    employee_gender VARCHAR(6),
    employee_email VARCHAR(200),
    store_id BIGINT,
    employee_hire_dt DATE,
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH enriched_employees AS (
        SELECT DISTINCT ON (emp.employee_email)
            COALESCE(emp.employee_id, 'n. a.')::VARCHAR(250) AS employee_src_id,
            COALESCE(emp.employee_firstname, 'n. a.')::VARCHAR(50) AS employee_firstname,
            COALESCE(emp.employee_lastname, 'n. a.')::VARCHAR(50) AS employee_lastname,
            COALESCE(emp.gender, 'n. a.')::VARCHAR(6) AS employee_gender,
            COALESCE(emp.employee_email, 'n. a.')::VARCHAR(200) AS employee_email,
            COALESCE(st.store_id, -1) AS store_id,
            COALESCE(emp.hire_date::DATE, DATE '1900-01-01') AS employee_hire_dt,
            'SA_OFFLINE_SALES'::VARCHAR(50) AS source_system,
            'SRC_OFFLINE_SALES'::VARCHAR(50) AS source_entity
        FROM sa_offline_sales.src_offline_sales emp
        LEFT JOIN BL_3NF.CE_Stores st 
            ON st.store_name = emp.store_name  
           AND st.source_system = 'SA_OFFLINE_SALES'
           AND st.source_entity = 'SRC_OFFLINE_SALES'
        ORDER BY emp.employee_email, emp.hire_date DESC
    )
    SELECT *
    FROM enriched_employees ee
    WHERE NOT EXISTS (
        SELECT 1
        FROM BL_3NF.CE_Employees_SCD e
        WHERE e.employee_src_id = ee.employee_src_id
          AND e.source_system = ee.source_system
          AND e.source_entity = ee.source_entity
          AND e.is_active = 'Y'
          AND e.employee_firstname = ee.employee_firstname
          AND e.employee_lastname = ee.employee_lastname
          AND e.employee_gender = ee.employee_gender
          AND e.employee_email = ee.employee_email
          AND e.store_id = ee.store_id
          AND e.employee_hire_dt = ee.employee_hire_dt
    );
END;
$$;
