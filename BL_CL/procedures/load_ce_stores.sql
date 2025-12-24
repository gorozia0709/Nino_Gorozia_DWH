CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_load_ce_stores()
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
           store_name,
           store_type,
           store_website,
           address_id,
           store_id,
           source_system,
           source_entity
       FROM (
           SELECT DISTINCT ON (s.store_name)
               COALESCE(s.store_name, 'n. a.') AS store_name,
               COALESCE(s.store_type, 'n. a.') AS store_type,
               'n. a.' AS store_website,
               COALESCE(a.address_id,-1) AS address_id,
               COALESCE(s.store_id, 'n. a.') AS store_id,
               'SA_OFFLINE_SALES' AS source_system,
               'SRC_OFFLINE_SALES' AS source_entity
           FROM sa_offline_sales.src_offline_sales s
           LEFT JOIN BL_3NF.CE_Addresses a 
               ON a.address_src_id = s.store_address 
               AND a.source_system = 'SA_OFFLINE_SALES' 
               AND a.source_entity = 'SRC_OFFLINE_SALES'

           UNION ALL

           SELECT DISTINCT ON (s.store_name)
               COALESCE(s.store_name, 'n. a.') AS store_name,
               'n. a.' AS store_type,
               COALESCE(s.store_website, 'n. a.') AS store_website,
               -1 AS address_id,
               COALESCE(s.store_id, 'n. a.') AS store_id,
               'SA_ONLINE_SALES' AS source_system,
               'SRC_ONLINE_SALES' AS source_entity
           FROM sa_online_sales.src_online_sales s
       ) src
   LOOP
       IF EXISTS (
           SELECT 1 
           FROM BL_3NF.CE_Stores st 
           WHERE st.store_src_id = rec.store_id
             AND st.source_system = rec.source_system 
             AND st.source_entity = rec.source_entity
       ) THEN
           UPDATE BL_3NF.CE_Stores
           SET 
               store_name = rec.store_name,
               store_type = rec.store_type,
               store_website = rec.store_website,
               address_id = rec.address_id,
               TA_UPDATE_DT = clock_timestamp()
           WHERE store_src_id = rec.store_id
             AND source_system = rec.source_system
             AND source_entity = rec.source_entity
             AND (
                 store_name IS DISTINCT FROM rec.store_name
                 OR store_type IS DISTINCT FROM rec.store_type
                 OR store_website IS DISTINCT FROM rec.store_website
                 OR address_id IS DISTINCT FROM rec.address_id
             );

           IF FOUND THEN
               v_rows_affected := v_rows_affected + 1;
           END IF;
       ELSE
           INSERT INTO BL_3NF.CE_Stores(
               store_id,
               store_name,
               store_type,
               store_website,
               address_id,
               TA_INSERT_DT,
               TA_UPDATE_DT,
               store_src_id,
               source_system,
               source_entity
           )
           VALUES (
               NEXTVAL('BL_3NF.seq_store_id'),
               rec.store_name,
               rec.store_type,
               rec.store_website,
               rec.address_id,
               clock_timestamp(),
               clock_timestamp(),
               rec.store_id,
               rec.source_system,
               rec.source_entity
           );

           v_rows_affected := v_rows_affected + 1;
       END IF;
   END LOOP;

   v_end_time := clock_timestamp();

   CALL BL_CL.prc_log_insert('prc_load_ce_stores', v_rows_affected, CASE WHEN v_rows_affected > 0 THEN 'Loaded successfully' ELSE 'No new data to load' END, v_end_time - v_start_time );
       

EXCEPTION WHEN OTHERS THEN v_end_time := clock_timestamp();
   CALL BL_CL.prc_log_insert( 'prc_load_ce_stores', v_rows_affected, 'Error: ' || SQLERRM, v_end_time - v_start_time ); RAISE;
END;
$$;
