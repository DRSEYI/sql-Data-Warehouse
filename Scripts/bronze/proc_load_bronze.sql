/*
==============================================================================
Stored procedure :Load bronze Layer(Source -> Bronze )
==============================================================================
Script Purpose :
		This stored procedure loads data into the 'bronze' schema from 
		external CSV files.
		-It truncates the bronze tables before loading data.
		-Uses the bulk insert command to load data from scv files to bronze tables.

Usage example :
EXEC bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE  bronze.load_bronze AS

BEGIN 
	DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME ;
	BEGIN TRY 
		PRINT 'loading the bronze layer';
		PRINT'===============================';
		PRINT 'Loading CRM Tables '
		PRINT'===============================';

		SET @batch_start_time=GETDATE();
		SET @start_time=GETDATE();
		PRINT '>>Truncating table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;


		PRINT '>>Inserting Data into:bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\seyio\Project\DWH_Project\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT'>>Load Time:' + CAST(DATEDIFF(second, @start_time, @end_time )AS NVARCHAR) + 'seconds';
		PRINT '-----------------------------------------------------'
		
		SET @start_time=GETDATE();
		PRINT '>>Truncating table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>Inserting Data into:bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\seyio\Project\DWH_Project\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'load time:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------------'


		SET @start_time=GETDATE();
		PRINT '>>Truncating table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>Inserting Data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\seyio\Project\DWH_Project\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'Load Time: '+ CAST( DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		PRINT '----------------------------------------------'
		PRINT '==============================================';
		PRINT 'Loading ERP Tables ';
		PRINT '===============================';

		SET @start_time=GETDATE();
		PRINT'>>Truncating table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;


		PRINT '>>Inserting data into :bronze.erp_cust_az12'

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\seyio\Project\DWH_Project\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'Load Time: '+ CAST( DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		PRINT'--------------------------------------------------------------'
		SET @start_time=GETDATE();
		PRINT'>>Truncating table: bronze.erp_loc_a101'

		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'>>Inserting data into : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\seyio\Project\DWH_Project\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'Load Time: '+ CAST( DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		PRINT '---------------------------------------------------------------------------'

		SET @start_time=GETDATE();
		PRINT'>>Truncating  table:bronze.erp_px_cat_g1v2 table '
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT'>>Inserting data into: bronze.erp_px_cat_g1v2 table'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\seyio\Project\DWH_Project\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'Load Time: '+ CAST( DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
		SET @batch_end_time= GETDATE();
		PRINT '>> Bronze Load Time: '+ CAST( DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'ms';

	END TRY 
	BEGIN CATCH 
		PRINT'======================================='
		PRINT 'ERRORS OCCURRED DURING LOADING THE BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'  + ERROR_LINE();
		PRINT'======================================='
	END CATCH 
END










