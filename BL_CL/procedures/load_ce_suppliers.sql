CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_suppliers()
LANGUAGE plpgsql
AS $$
DECLARE
   rec RECORD;
   v_rows_affected INTEGER := 0;
   v_start_time TIMESTAMP := clock_timestamp();
   v_end_time TIMESTAMP;
BEGIN

   FOR rec IN
       SELECT DISTINCT
           COALESCE(supplier_value, 'n. a.') AS supplier_name,
           COALESCE(supplier_email, 'n. a.') AS supplier_email,
           COALESCE(contract_start::DATE, '1900-01-01') AS supplier_contract_start_dt,
           COALESCE(contract_end::DATE, '9999-12-31') AS supplier_contract_end_dt,
           COALESCE(supplier_id, 'n. a.') as supplier_id,
           COALESCE(source_system, 'MANUAL') as source_system,
           COALESCE(source_entity, 'MANUAL') as source_entity
       FROM (
           SELECT DISTINCT 
               supplier AS supplier_value,
               supplier_email,
               contract_start,
               contract_end,
			   supplier_id,
               'SA_OFFLINE_SALES' AS source_system,
               'SRC_OFFLINE_SALES' AS source_entity
           FROM sa_offline_sales.src_offline_sales

           UNION ALL

           SELECT DISTINCT 
               supplier AS supplier_value,
               supplier_email,
               contract_start,
               contract_end,
			   supplier_id,
               'SA_ONLINE_SALES' AS source_system,
               'SRC_ONLINE_SALES' AS source_entity
           FROM sa_online_sales.src_online_sales
       ) src
   LOOP
       IF EXISTS (
           SELECT 1
           FROM BL_3NF.CE_Suppliers s
           WHERE s.supplier_src_id = rec.supplier_id
             AND s.source_system = rec.source_system
             AND s.source_entity = rec.source_entity
       ) THEN
           UPDATE BL_3NF.CE_Suppliers
           SET 
               supplier_name = rec.supplier_name,
               supplier_email = rec.supplier_email,
               supplier_contract_start_dt = rec.supplier_contract_start_dt,
               supplier_contract_end_dt = rec.supplier_contract_end_dt,
               TA_UPDATE_DT = clock_timestamp()
           WHERE supplier_src_id = rec.supplier_id
             AND source_system = rec.source_system
             AND source_entity = rec.source_entity
             AND (
                 supplier_name IS DISTINCT FROM rec.supplier_name OR
                 supplier_email IS DISTINCT FROM rec.supplier_email OR
                 supplier_contract_start_dt IS DISTINCT FROM rec.supplier_contract_start_dt OR
                 supplier_contract_end_dt IS DISTINCT FROM rec.supplier_contract_end_dt
             );

           IF FOUND THEN
               v_rows_affected := v_rows_affected + 1;
           END IF;
       ELSE
           INSERT INTO BL_3NF.CE_Suppliers(
               supplier_id,
               supplier_name,
               supplier_email,
               supplier_contract_start_dt,
               supplier_contract_end_dt,
               TA_INSERT_DT,
               TA_UPDATE_DT,
               supplier_src_id,
               source_system,
               source_entity
           )
           VALUES (
               NEXTVAL('BL_3NF.seq_supplier_id'),
               rec.supplier_name,
               rec.supplier_email,
               rec.supplier_contract_start_dt,
               rec.supplier_contract_end_dt,
               clock_timestamp(),
               clock_timestamp(),
               rec.supplier_id,
               rec.source_system,
               rec.source_entity
           );

           v_rows_affected := v_rows_affected + 1;
       END IF;
   END LOOP;

   v_end_time := clock_timestamp();

   CALL BL_CL.prc_log_insert(
       'prc_load_ce_suppliers',
       v_rows_affected,
       CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END,
       v_end_time - v_start_time
   );

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
  CALL BL_CL.prc_log_insert( 'prc_load_ce_suppliers', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;
