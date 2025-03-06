--------- Cleaning, Transforming and Loading Data into Silver Layer----------

/*This Code Create Store Procdure for Get data from Bronze layer , Transform it
and load it silver layer*/

----- Craete Procedure silver.load_silver-----
CREATE OR ALTER  PROCEDURE silver.load_silver AS
BEGIN

	----Truncating Loading and Transforming table silver.crm_cust_info
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info(
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr, 
		cst_create_date)
	SELECT cst_id,
		   cst_key,
		   trim(cst_firstname) AS cst_firstname,
		   trim(cst_lastname) AS cst_lastname,
		   CASE
			   WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
			   WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
			   ELSE 'n/a'
		   END AS cst_marital_status,
		   CASE
			   WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
			   WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
			   ELSE 'n/a'
		   END AS cst_gndr,
		   cst_create_date
	FROM
	  (SELECT *,
			  row_number() OVER (PARTITION BY cst_id
								 ORDER BY cst_create_date DESC) AS flag_last
	   FROM bronze.crm_cust_info)t
	WHERE flag_last = 1
	  AND cst_id IS NOT NULL;


	----Truncating Loading and Transforming table silver.crm_prd_info
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
	prd_id, 
	cat_id, 
	prd_key, 
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt, 
	prd_end_dt
	)
	SELECT prd_id,
		   replace(substring(prd_key, 1, 5), '-', '_') AS cat_id,
		   substring(prd_key, 7, len(prd_key)) AS prd_key,
		   prd_nm,
		   isnull(prd_cost, 0) AS prd_cost,
		   CASE upper(trim(prd_line))
			   WHEN 'M' THEN 'Mountain'
			   WHEN 'R' THEN 'River'
			   WHEN 'S' THEN 'Other Sales'
			   WHEN 'T' THEN 'Touring'
			   ELSE 'n/a'
		   END AS prd_line,
		   cast(prd_start_dt AS date) AS prd_start_dt,
		   cast(lead(prd_start_dt) over(PARTITION BY prd_key
										ORDER BY prd_start_dt)-1 AS date) AS prd_end_dt
	FROM bronze.crm_prd_info;

	----Truncating Loading and Transforming table silver.crm_sales_Details
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt, 
	sls_due_dt,
	sls_sales, 
	sls_quantity,
	sls_price   
	)
	SELECT sls_ord_num,
		   sls_prd_key,
		   sls_cust_id,
		   CASE
			   WHEN sls_order_dt = 0
					OR len(sls_order_dt) != 8 THEN NULL
			   ELSE cast(cast(sls_order_dt AS varchar) AS date)
		   END AS sls_order_dt,
		   CASE
			   WHEN sls_ship_dt = 0
					OR len(sls_ship_dt) != 8 THEN NULL
			   ELSE cast(cast(sls_ship_dt AS varchar) AS date)
		   END AS sls_ship_dt,
		   CASE
			   WHEN sls_due_dt = 0
					OR len(sls_due_dt) != 8 THEN NULL
			   ELSE cast(cast(sls_due_dt AS varchar) AS date)
		   END AS sls_due_dt,
		   CASE
			   WHEN sls_sales IS NULL
					OR sls_sales <= 0
					OR sls_sales != sls_quantity * abs(sls_price) THEN sls_quantity * abs(sls_price)
			   ELSE sls_sales
		   END AS sls_sales,
		   sls_quantity,
		   CASE
			   WHEN sls_price IS NULL
					OR sls_price <= 0 THEN sls_price / nullif(sls_quantity, 0)
			   ELSE sls_price
		   END AS sls_price
	FROM bronze.crm_sales_details;

	----Truncating Loading and Transforming table silver.erp_cust_az12
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12 (
	cid, 
	bdate, 
	gen
	)
	SELECT CASE
			   WHEN cid like 'NAS%' THEN substring(cid, 4, len(cid))
			   ELSE cid
		   END AS cid,
		   CASE
			   WHEN bdate > getdate() THEN NULL
			   ELSE bdate
		   END AS bdate,
		   CASE
			   WHEN upper(trim(gen)) IN ('F',
										 'FEMALE') THEN 'Female'
			   WHEN upper(trim(gen)) IN ('M',
										 'MALE') THEN 'Male'
			   ELSE 'n/a'
		   END AS gen
	FROM bronze.erp_cust_az12;

	----Truncating Loading and Transforming table silver.erp_loc_a101
	TRUNCATE TABLE silver.erp_loc_a101
	INSERT INTO silver.erp_loc_a101 (
	cid, 
	cntry
	)
	SELECT cid,
		   CASE
			   WHEN trim(cntry) = 'DE' THEN 'Germany'
			   WHEN trim(cntry) IN ('US',
									'USA') THEN 'United State'
			   WHEN trim(cntry) = ''
					OR cntry IS NULL THEN 'n/a'
			   ELSE trim(cntry)
		   END AS cntry
	FROM bronze.erp_loc_a101;

	----Truncating Loading and Transforming table silver.erp_px_cat_g1v2
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2 (
	id, 
	cat, 
	subcat, 
	maintenance
	)
	SELECT id,
		   cat,
		   subcat,
		   maintenance
	FROM bronze.erp_px_cat_g1v2;
END;

EXEC silver.load_silver;
