/*
--------Stored Procedure: Load Bronze Layer (Source -> Bronze)----------------------

This stored procedure loads data into the 'bronze' schema from external CSV files. 
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

*/


-- Craete Or Alter Store Procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
-- Trubcate and Load Data into Table
	TRUNCATE TABLE bronze.crm_cust_info;
		
	BULK INSERT bronze.crm_cust_info
	FROM 'E:\PROJECTS\MY SQL PROJECT\data warehouse\MY PROJECT PRACTICE\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		
	TRUNCATE TABLE bronze.crm_prd_info;

	BULK INSERT bronze.crm_prd_info
	FROM 'E:\PROJECTS\MY SQL PROJECT\data warehouse\MY PROJECT PRACTICE\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		
	TRUNCATE TABLE bronze.crm_sales_details;
		
	BULK INSERT bronze.crm_sales_details
	FROM 'E:\PROJECTS\MY SQL PROJECT\data warehouse\MY PROJECT PRACTICE\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		
	TRUNCATE TABLE bronze.erp_loc_a101;
		
	BULK INSERT bronze.erp_loc_a101
	FROM 'E:\PROJECTS\MY SQL PROJECT\data warehouse\MY PROJECT PRACTICE\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		
	TRUNCATE TABLE bronze.erp_cust_az12;
		
	BULK INSERT bronze.erp_cust_az12
	FROM 'E:\PROJECTS\MY SQL PROJECT\data warehouse\MY PROJECT PRACTICE\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'E:\PROJECTS\MY SQL PROJECT\data warehouse\MY PROJECT PRACTICE\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
END

-- Execute or Call The Store Procedure
EXEC bronze.load_bronze
