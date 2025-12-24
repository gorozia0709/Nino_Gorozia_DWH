CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE bl_cl.master_procedure()
LANGUAGE plpgsql
AS $$
BEGIN
CALL BL_CL.prc_load_ce_states();
CALL BL_CL.prc_load_ce_cities();
CALL BL_CL.prc_load_ce_addresses();
CALL BL_CL.prc_load_ce_shippings();
CALL BL_CL.prc_load_ce_categories();
CALL BL_CL.prc_load_ce_subcategories();
CALL BL_CL.prc_load_ce_brands();
CALL BL_CL.prc_load_ce_paymenttypes();
CALL BL_CL.prc_load_ce_stores();
CALL BL_CL.prc_load_ce_suppliers();
CALL BL_CL.prc_load_ce_customers_scd();
CALL BL_CL.prc_load_ce_products_scd();
CALL BL_CL.prc_load_ce_employees_SCD();
CALL bl_cl.load_ce_transactions();
CALL BL_CL.prc_load_dim_stores();
CALL BL_CL.prc_load_dim_suppliers();
CALL BL_CL.prc_load_dim_paymenttypes();
CALL BL_CL.prc_load_dim_shippings();
CALL BL_CL.prc_load_dim_customers_scd();
CALL BL_CL.prc_load_dim_products_scd();
CALL BL_CL.prc_load_dim_employees_scd();
CALL bl_cl.populate_dim_dates('2015-01-01', '2027-01-01');
CALL bl_cl.populate_dim_times();
CALL BL_CL.LOAD_FCT_TRANSACTIONS_DD();
END;
$$;