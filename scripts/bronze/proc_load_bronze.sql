/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME; 

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '=====================================================================================';
        PRINT 'Loading the Bronze Layer';
        PRINT '=====================================================================================';

        ------------------------------------------
        -- CRM Tables Load
        ------------------------------------------
        PRINT '--------------------------------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '--------------------------------------------------------------------------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_cust_info';
        TRUNCATE TABLE DataWarehouse.bronze.crm_cust_info;

        PRINT '>> Inserting Data into : bronze.crm_cust_info';
        BULK INSERT DataWarehouse.bronze.crm_cust_info
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_prd_info';
        TRUNCATE TABLE DataWarehouse.bronze.crm_prd_info;

        PRINT '>> Inserting Data into : bronze.crm_prd_info';
        BULK INSERT DataWarehouse.bronze.crm_prd_info
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_sales_details';
        TRUNCATE TABLE DataWarehouse.bronze.crm_sales_details;

        PRINT '>> Inserting Data into : bronze.crm_sales_details';
        BULK INSERT DataWarehouse.bronze.crm_sales_details
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';

        ------------------------------------------
        -- ERP Tables Load
        ------------------------------------------
        PRINT '--------------------------------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '--------------------------------------------------------------------------------------';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_cust_az12';
        TRUNCATE TABLE DataWarehouse.bronze.erp_cust_az12;

        PRINT '>> Inserting Data into : bronze.erp_cust_az12';
        BULK INSERT DataWarehouse.bronze.erp_cust_az12
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';

        -- erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_loc_a101';
        TRUNCATE TABLE DataWarehouse.bronze.erp_loc_a101;

        PRINT '>> Inserting Data into : bronze.erp_loc_a101';
        BULK INSERT DataWarehouse.bronze.erp_loc_a101
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE DataWarehouse.bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data into : bronze.erp_px_cat_g1v2';
        BULK INSERT DataWarehouse.bronze.erp_px_cat_g1v2
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';

        ------------------------------------------
        -- End of Procedure
        ------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '=====================================================================================';
        PRINT 'Bronze Layer Load Completed';
        PRINT '>> Total Load Duration : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=====================================================================================';

    END TRY

    BEGIN CATCH
        PRINT '============================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================';
    END CATCH
END;
