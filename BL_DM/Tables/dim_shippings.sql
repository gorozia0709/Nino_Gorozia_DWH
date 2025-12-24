CREATE SCHEMA IF NOT EXISTS BL_DM;

CREATE SEQUENCE IF NOT EXISTS BL_DM.seq_shipping_surr_id START 1 INCREMENT BY 1;



CREATE TABLE IF NOT EXISTS BL_DM.DIM_Shippings (
    Shipping_SURR_ID bigint PRIMARY KEY,
    Shipping_Method varchar(40) NOT NULL,
    Shipping_Price decimal NOT NULL,
    Shipping_Address_ID bigint NOT NULL,
    Shipping_Address_Name varchar(100) NOT NULL,
    Shipping_City_ID bigint NOT NULL,
    Shipping_City_Name varchar(50) NOT NULL,
    Shipping_State_ID bigint NOT NULL,
    Shipping_State_Name varchar(10) NOT NULL,
    TA_Insert_DT timestamp NOT NULL,
    TA_Update_DT timestamp NOT NULL,
	Shipping_SRC_ID varchar(250) NOT NULL,
    source_system varchar(100) NOT NULL,
    source_entity varchar(100) NOT NULL
);


INSERT INTO BL_DM.DIM_Shippings(
Shipping_SURR_ID,Shipping_SRC_ID,Shipping_Method,Shipping_Price,Shipping_Address_ID,Shipping_Address_Name,
Shipping_City_ID,Shipping_City_Name,Shipping_State_ID,Shipping_State_Name,TA_Insert_DT,TA_Update_DT,source_system,source_entity)
SELECT -1,'n. a.','n. a.',-1,-1,'n. a.',-1,'n. a.',-1,'n. a.','1900-01-01'::timestamp,'1900-01-01'::timestamp,'MANUAL','MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Shippings WHERE Shipping_SURR_ID=-1);