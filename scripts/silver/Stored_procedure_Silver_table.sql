-- CREATE STORE PROCDEURE OF SILVER LAYER
/*

============================================================================
SIMILAR TO BRONZE, ADD THE FOLLOWING TO THE SILVER STORED PROCEDURE
 - PRINT MESSAGES FOR EACH SECTION AND STEP
 - IMPLEMENT ERROR HANDLING
 - PRINT THE DURATION OF EACH STEP
 - PRINT THE DURATION OF LOADING SILVER
============================================================================

FOR EXECUTION :
EXEC SILVER.LOAD_SILVER;

============================================================================

SCRIPT PURPOSE:
This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the pronze schema.

ACTIONS PERFORMED:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

============================================================================

ADD PRINTS => TO TRACK EXECUTION, DEBUG ISSUES, AND UNDERSTAND ITS FLOW.

============================================================================


*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 
     DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

     BEGIN TRY
             SET @batch_start_time = GETDATE();

             PRINT '========================================================='
             PRINT 'LOADING SILVER LAYER'
             PRINT '========================================================='

                
                PRINT '---------------------------------------------------------'
                PRINT 'LOADING CRM TABLES'
                PRINT '---------------------------------------------------------'

                PRINT '>> TRUNCATING TABLE: silver.crm_cust_info';
                TRUNCATE TABLE silver.crm_cust_info;
                PRINT '>> INSERTING DATA INTO: silver.crm_cust_info';

                SET @start_time = GETDATE();

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
                TRIM(CST_FIRSTNAME) AS CST_FIRSTNAME, -- REMOVE LEADING AND TRAILING SPACES FROM A STRING
                TRIM(CST_LASTNAME) AS CST_LASTNAME,   -- REMOVE LEADING AND TRAILING SPACES FROM A STRING

                CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                     ELSE 'N/A'
                END cst_marital_status, -- NORMALIZE MARITAL  STATUS VALUES TO READABLE FORMAT

                CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                     ELSE 'N/A'
                END cst_gndr, -- NORMALIZE GENDER VALUES TO READABLE FORMAT
                cst_create_date
                FROM (
                SELECT *,
                ROW_NUMBER() OVER (PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS FLAG_LAST
                FROM bronze.crm_cust_info
                where cst_id IS NOT NULL
                ) T WHERE FLAG_LAST = 1; -- SELECT THE MOST RECENT RECORD PER CUSTOMER

                select * from silver.crm_cust_info;

                SET @end_time = GETDATE();
                PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
                PRINT '----------------------------------------------------------------------------------------------'

                -- 2

                SET @start_time = GETDATE();

                PRINT '>> TRUNCATING TABLE: silver.crm_prd_info';
                TRUNCATE TABLE silver.crm_prd_info;
                PRINT '>> INSERTING DATA INTO: silver.crm_prd_info';


                INSERT INTO silver.crm_prd_info(
                      prd_id,
                      cat_id,
                      prd_key,
                      prd_nm,
                      prd_cost,
                      prd_line,
                      prd_start_dt,
                      prd_end_dt
                )


                select 
                prd_id,
                SUBSTRING(PRD_KEY, 7, LEN(PRD_KEY)) AS prd_key,
                REPLACE(SUBSTRING(PRD_KEY,1,5),'-', '_') AS cat_id,
                prd_nm,
                ISNULL(PRD_COST,0) AS prd_cost,
                CASE UPPER(TRIM(prd_line))
                     WHEN 'M' THEN 'Mountain'
                     WHEN 'R' THEN 'Road'
                     WHEN 'S' THEN 'Other Sales'
                     WHEN 'T' THEN 'Touring'
                     ELSE 'N/A'
                END prd_line,
                CAST(prd_start_dt AS DATE)  prd_start_dt,
                CAST(LEAD(prd_start_dt) OVER (PARTITION BY PRD_KEY ORDER BY PRD_START_DT) - 1 AS DATE) AS prd_end_dt
                from bronze.crm_prd_info;

                select * from silver.crm_prd_info;

                SET @end_time = GETDATE();
                PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
                PRINT '----------------------------------------------------------------------------------------------'



                -- 3

                SET @start_time = GETDATE();

                PRINT '>> TRUNCATING TABLE: silver.CRM_SALES_DETAILS';
                TRUNCATE TABLE silver.CRM_SALES_DETAILS;
                PRINT '>> INSERTING DATA INTO: silver.CRM_SALES_DETAILS';


                INSERT INTO SILVER.CRM_SALES_DETAILS (
                      sls_ord_num,
                      sls_prd_key,
                      sls_cust_id,
                      sls_order_dt,
                      sls_ship_dt,
                      sls_due_dt,
                      sls_sales,
                      sls_qunatity,
                      sls_price
                )

                select 
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
                     else cast(cast(sls_order_dt as varchar)as date)
                end sls_order_dt,
                case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
                     else cast(cast(sls_ship_dt as varchar)as date)
                end sls_ship_dt,
                case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
                     else cast(cast(sls_due_dt as varchar)as date)
                end sls_due_dt,
                case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
                            then sls_quantity * abs(sls_price)
                     else sls_sales
                end as sls_sales,
                sls_quantity,
                case when sls_price is null or sls_price <= 0
                          then sls_sales /  nullif(sls_quantity,0)
                     else sls_price
                end as sls_price

                from bronze.crm_sales_detail;

                select * from SILVER.CRM_SALES_DETAILS;

                SET @end_time = GETDATE();
                PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
                PRINT '----------------------------------------------------------------------------------------------'



                PRINT '---------------------------------------------------------'
                PRINT 'LOADING ERP TABLES'
                PRINT '---------------------------------------------------------'


                -- 4 

                SET @start_time = GETDATE();



                PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12';
                TRUNCATE TABLE silver.erp_cust_az12;
                PRINT '>> INSERTING DATA INTO: silver.erp_cust_az12';


                INSERT INTO SILVER.erp_cust_az12 (
                      CID,
                      bdate,
                      gen
                )

                SELECT
                CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID)) -- REMOVE 'NAS' PREFIX IF PRESENT
                     ELSE CID
                END AS cid,
                CASE WHEN bdate > GETDATE() THEN NULL  -- SET FUTURE BIRTHDATES TO NULL
                     ELSE bdate 
                END AS bdate,
                CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
                     WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male' -- NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES
                     ELSE 'N/A'
                END AS gen
                from bronze.erp_cust_az12;

                select * from silver.erp_cust_az12;

                SET @end_time = GETDATE();
                PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
                PRINT '----------------------------------------------------------------------------------------------'


                -- 5

                SET @start_time = GETDATE();


                PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101';
                TRUNCATE TABLE silver.erp_loc_a101;
                PRINT '>> INSERTING DATA INTO: silver.erp_loc_a101';

                INSERT INTO silver.erp_loc_a101 (
                         CID,
                         CNTRY
                )

                SELECT
                REPLACE(CID,'-','') AS CID,
                CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
                     WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
                     WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'N/A'
                     ELSE TRIM(CNTRY)
                END AS CNTRY
                FROM BRONZE.erp_loc_a101;


                select * from silver.erp_loc_a101;

                SET @end_time = GETDATE();
                PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
                PRINT '----------------------------------------------------------------------------------------------'


                -- 6

                SET @start_time = GETDATE();

                PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2';
                TRUNCATE TABLE silver.erp_px_cat_g1v2;
                PRINT '>> INSERTING DATA INTO: silver.erp_px_cat_g1v2';

                INSERT INTO silver.erp_px_cat_g1v2(
                      ID,
                      CAT,
                      SUBCAT,
                      maintenance
                )

                SELECT 
                   ID,
                   CAT,
                   SUBCAT,
                   maintenance
                FROM 
                bronze.erp_px_cat_g1v2;
                        select * from bronze.erp_px_cat_g1v2;

         SET @end_time = GETDATE();
         PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
         PRINT '----------------------------------------------------------------------------------------------'


        SET @batch_end_time = GETDATE();
                PRINT '=============================================================================================='
                PRINT 'LOADING BRONZE LAYER IS COMPLETED';
                PRINT '  - TOTAL LAOD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' SECONDS';
                PRINT '=============================================================================================='

        END TRY

        BEGIN CATCH
              PRINT '======================================================='
              PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
              PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
              PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
              PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
              PRINT '======================================================='
        END CATCH


END


EXEC silver.load_silver;
