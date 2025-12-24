CREATE SCHEMA IF NOT EXISTS BL_DM;

CREATE SEQUENCE IF NOT EXISTS BL_DM.seq_customer_surr_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_DM.DIM_Customers_SCD (
    Customer_SURR_ID bigint PRIMARY KEY,
    Customer_Firstname varchar(50) NOT NULL,
    Customer_Lastname varchar(50) NOT NULL,
    Customer_Gender varchar(6),
    Customer_Email varchar(100),
    Customer_Phone varchar(20),
    Customer_Age int,
    Customer_Signup_DT date NOT NULL,
    Start_DT timestamp NOT NULL,
    End_DT timestamp NOT NULL,
    Is_Active VARCHAR(1) NOT NULL,
    TA_Insert_DT timestamp NOT NULL,
	Customer_SRC_ID varchar(250) NOT NULL,
    source_system varchar(100) NOT NULL,
    source_entity varchar(100) NOT NULL
);


INSERT INTO BL_DM.DIM_Customers_SCD(
Customer_SURR_ID,Customer_SRC_ID,Customer_Firstname,Customer_Lastname,Customer_Gender,Customer_Email,Customer_Phone,
Customer_Age,Customer_Signup_DT,Start_DT,End_DT,Is_Active,TA_Insert_DT,source_system,source_entity)
SELECT -1,'n. a.','n. a.','n. a.','n. a.','n. a.','n. a.',-1, '1900-01-01'::date, '1900-01-01'::timestamp, '9999-12-31'::timestamp,'Y','1900-01-01'::timestamp,'MANUAL','MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Customers_SCD WHERE Customer_SURR_ID=-1);