/*
==========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================================

Script:
  This stored procedure loads into the 'bronze' schema froom external CSV files.
  It performs the following actions:
-Truncates the bronze tables before loading the data.
- Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
===========================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_broonze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY 
		PRINT '===================================';
		PRINT 'Loading Bronze Layer';

		PRINT '===================================';
		PRINT '-----------------------------------';

		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------';
		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\data-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		
		PRINT '------------------------------------'
		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\data-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_info';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data Into: bronze.crm_sales_info';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\data-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '-----------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------';

		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\data-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\data-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		SET @start_time= GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\data-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT '>> Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT '======================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT 'ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '======================================';

	END CATCH
END
EXEC bronze.load_broonze
