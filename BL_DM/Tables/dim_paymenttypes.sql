CREATE SCHEMA IF NOT EXISTS BL_DM;

CREATE SEQUENCE IF NOT EXISTS BL_DM.seq_payment_type_surr_id START 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS BL_DM.DIM_PaymentTypes (
    Payment_Type_SURR_ID bigint PRIMARY KEY,
    Payment_Type_Method varchar(30) NOT NULL,
    TA_Insert_DT timestamp NOT NULL,
    TA_Update_DT timestamp NOT NULL,
	Payment_Type_SRC_ID varchar(50) NOT NULL,
    source_system varchar(100) NOT NULL,
    source_entity varchar(100) NOT NULL
);


INSERT INTO BL_DM.DIM_PaymentTypes(Payment_Type_SURR_ID,Payment_Type_SRC_ID,Payment_Type_Method,TA_Insert_DT,TA_Update_DT,source_system,source_entity)
SELECT -1,'n. a.','n. a.','1900-01-01'::timestamp,'1900-01-01'::timestamp,'MANUAL','MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_PaymentTypes WHERE Payment_Type_SURR_ID=-1);