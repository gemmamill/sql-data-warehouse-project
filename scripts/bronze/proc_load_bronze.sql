 


CREATE OR ALTER PROCEDURE Bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
	SET @batch_start_time = GETDATE ();
	PRINT '===================================';
	PRINT 'Loading Bronze Layer';
	PRINT '===================================';

	PRINT '---------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '---------------------------------------';

	SET @start_time = GETDATE();
	BULK INSERT Bronze.crm_cust_info
	FROM 'C:\Users\gemma.mill\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)

	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_prd_info;
	BULK INSERT Bronze.crm_prd_info
	FROM 'C:\Users\gemma.mill\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)


	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_sales_details;
	BULK INSERT Bronze.crm_sales_details
	FROM 'C:\Users\gemma.mill\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)


	PRINT '---------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '---------------------------------------';

	SET @start_time = GETDATE();
	BULK INSERT Bronze.erp_CUST_AZ12
	FROM 'C:\Users\gemma.mill\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)


	SET @start_time = GETDATE();
	BULK INSERT Bronze.erp_LOC_A101
	FROM 'C:\Users\gemma.mill\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)

	SET @start_time = GETDATE ();
	BULK INSERT Bronze.erp_PX_CAT_G1V2
	FROM 'C:\Users\gemma.mill\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)

	SET @batch_end_time = GETDATE ();

	PRINT '=================================================';
	PRINT 'Loading Bronze Layer is Completed';
	PRINT '   - Total Load Duration: ' +CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '=================================================';

	END TRY 
	BEGIN CATCH 
	END CATCH
END
