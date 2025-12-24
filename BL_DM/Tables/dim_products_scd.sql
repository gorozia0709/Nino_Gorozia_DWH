CREATE SCHEMA IF NOT EXISTS BL_DM;
CREATE SEQUENCE IF NOT EXISTS BL_DM.seq_product_surr_id START 1 INCREMENT BY 1;



CREATE TABLE IF NOT EXISTS BL_DM.DIM_Products_SCD (
    Product_SURR_ID bigint PRIMARY KEY,
    Product_Name varchar(100) NOT NULL,
    Subcategory_ID bigint NOT NULL,
    Product_Subcategory_Name varchar(100) NOT NULL,
    Category_ID bigint NOT NULL,
    Product_Category_Name varchar(70) NOT NULL,
    Brand_ID bigint NOT NULL,
    Product_Brand_Name varchar(100) NOT NULL,
    Product_Rating decimal(3,2),
    Start_DT timestamp NOT NULL,
    End_DT timestamp NOT NULL,
    Is_Active VARCHAR(1) NOT NULL,
    TA_Insert_DT timestamp NOT NULL,
	Product_SRC_ID varchar(200) NOT NULL,
    source_system varchar(100) NOT NULL,
    source_entity varchar(100) NOT NULL
);


INSERT INTO BL_DM.DIM_Products_SCD(
Product_SURR_ID,Product_SRC_ID,Product_Name,Subcategory_ID,Product_Subcategory_Name,Category_ID,Product_Category_Name,
Brand_ID,Product_Brand_Name,Product_Rating,Start_DT,End_DT,Is_Active,TA_Insert_DT,source_system,source_entity)
SELECT -1,'n. a.','n. a.',-1,'n. a.',-1,'n. a.',-1,'n. a.',-1, '1900-01-01'::timestamp,'9999-12-31'::timestamp,'Y',DATE '1900-01-01'::timestamp,'MANUAL','MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Products_SCD WHERE Product_SURR_ID=-1);