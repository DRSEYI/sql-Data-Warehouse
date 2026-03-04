
/*
This script performs the ETL(extract, transform, load) process to populate the 
 Silver schema tables from the bronze table.
Actions:
**Truncates the tables 
**Populate the silver table with cleansed and transformed data from the bronze tables

TO EXECUTE: Use the query below
EXEC silver.load_silver 
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME , @batch_end DATETIME
	BEGIN TRY
		SET @batch_start=GETDATE();
		SET @start_time =GETDATE();
		PRINT'>>>Truncating table:silver.crm_cust_info'

		TRUNCATE TABLE silver.crm_cust_info;

		PRINT'>>>Inserting transformed data into table: silver.crm_cust_info'

		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status))= 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'
				ELSE 'n/a'
			END cst_marital_status,--Normalise marital status values to readable format 

			CASE 
				WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
				ELSE 'n/a'
			END cst_gndr, --Normalise gender value to readale format 
			cst_create_date
			 FROM 
				(SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
				WHERE  cst_id IS NOT NULL 
				)t
			WHERE flag_last = 1  --select the most recent record per customer 
			SET @end_time =GETDATE();
			PRINT'Load_time :'+ cast(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR)
			PRINT'------------------------------------------------------------'
		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table:silver.crm_prd_info'
		TRUNCATE table silver.crm_prd_info;
		PRINT'>>>Inseting transformed data into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key ,
			prd_nm,
			prd_cost,
			prd_line ,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
			prd_id,
			REPLACE (SUBSTRING(prd_key, 1, 5 ), '-', '_') as cat_id,--extract category id 
			SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key, --extract a new product key 
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,

			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, ---map product line codes to descriptive values 
			CAST (prd_start_dt  AS DATE) AS prd_start_dt ,
			CAST(LEAD(prd_start_dt)OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 
				AS DATE 
				) AS prd_end_dt -- calculate end date as one before the next start date 
		FROM bronze.crm_prd_info
		SET @end_time=GETDATE();
		PRINT 'silver_crm_prd_info_load_time:'+ 
				CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)

		SET @start_time=GETDATE();
		PRINT '>>>Truncating data into:silver.crm_sales_details'

		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>>>Inserting transformed data into: silver.crm_sales_details'

		INSERT INTO silver.crm_sales_details (
			sls_order_num,
			sls_prd_ket,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price 
		)

		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) 
			END sls_order_dt,
			--CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ord_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE) 
			END sls_ship_dt,
			--CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE) 
			END sls_due_dt,
			--sls_sales as old_sls_sales,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0
					OR  sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
			END AS sls_sales, 
			sls_quantity,
			--sls_price as old_sls_price,
			CASE WHEN sls_price IS NULL or sls_price < = 0 THEN  sls_sales/NULLIF(sls_quantity, 0)
					ELSE sls_price 
			END sls_price 
			FROM bronze.crm_sales_details;
			SET @end_time =GETDATE();
			PRINT 'silver_crm_sales_details_load_time:'+ 
				CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)

			PRINT'-----------------------------------------------------------------' 
			SET @start_time=GETDATE();
			PRINT'>>>Truncating table:silver.erp_cust_az12'

			TRUNCATE TABLE silver.erp_cust_az12;

			PRINT'>>>Inserting table:silver.erp_cust_az12'

		INSERT INTO  silver.erp_cust_az12(
				cid,
				bdate,
				gen
			)

	
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  --remove 'NAS' prefix if present 
				ELSE cid 
			END AS cid,
			CASE WHEN bdate > GETDATE()  THEN NULL 
				ELSE bdate 
			END bdate, --set future birthdates to NULL 
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('FEMALE','F') THEN 'Female'
				ELSE 'n/a'
			END AS  gen-- normalise gender values and handle unknown cases
		FROM  bronze.erp_cust_az12
		SET @end_time=GETDATE();
		PRINT 'silver_erp_cust_az12_load_time:'+ 
				CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)

		SET @start_time=GETDATE();
		PRINT '>>Truncating Table: silver.erp_loc_a101 table';

		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT'>>Inserting transformed data into:  silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101(cid,cntry)

		SELECT
		REPLACE(cid,'-','') as cid, --handle invalid value 
		CASE 
			WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
			WHEN TRIM(cntry) IN ('Germany', 'DE') THEN 'Germany'
			WHEN TRIM(cntry) ='' OR TRIM(cntry) IS NULL THEN  'n/a'
			ELSE TRIM(cntry)--normalization, removed empty spaces 
		END cntry
		FROM bronze.erp_loc_a101
		PRINT 'Truncating table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>Inserting into:silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance 
		)
		SELECT 
			id, 
			cat, 
			subcat,
			maintenance 
		FROM bronze.erp_px_cat_g1v2
		SET @end_time=GETDATE();
		PRINT 'silver_erp_px_cat_g1v2_load_time:'+ 
				CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)
		SET @batch_end = GETDATE();
		PRINT 'Total  silver load time:'+ CAST(DATEDIFF(millisecond,@batch_start,@batch_end)AS NVARCHAR)
		END TRY 
		BEGIN CATCH 
		PRINT'======================================='
		PRINT 'ERRORS OCCURRED DURING LOADING THE SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'  + ERROR_LINE();
		PRINT'======================================='
		END CATCH 
END 
