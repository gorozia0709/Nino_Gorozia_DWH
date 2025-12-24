CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE TABLE IF NOT EXISTS BL_CL.LOG_TABLE (
    log_id BIGSERIAL PRIMARY KEY,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    procedure_name TEXT,
    rows_affected INTEGER,
    log_message TEXT,
    execution_time INTERVAL
);