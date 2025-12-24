CREATE SCHEMA IF NOT EXISTS BL_CL;



CREATE OR REPLACE PROCEDURE BL_CL.prc_log_insert(
    p_procedure_name TEXT,
    p_rows_affected INTEGER,
    p_log_message TEXT,
    p_execution_time INTERVAL
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO BL_CL.LOG_TABLE(procedure_name, rows_affected, log_message, execution_time)
    VALUES (p_procedure_name, p_rows_affected, p_log_message, p_execution_time);
END;
$$;
