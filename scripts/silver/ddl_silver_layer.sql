/*
=====================================================

Working on Silver Layer

=====================================================

1st ->  We create DDL Script of Silver Layer, so for that consult with 
technical experts of the source system to understand its metadata.

2nd -> Data Profiling -> Explore the data to identify column names and data types.

3rd -> Follow naming convention "snake_case"

4th -> We need to create all table from file.csv that was in your PC

5th -> We put bulk data to our database from sources.

-- If you delete table to check that it exists or not 

If OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

6th -> We create store procedure to insert data in bulk.

*/

use datawarehouse;

-- This part is for CRM Files

If OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

create table silver.crm_cust_info(

	cst_id int,
	cst_key Nvarchar(50),
	cst_firstname Nvarchar(50),
	cst_lastname Nvarchar(50),
	cst_marital_status Nvarchar(50),
	cst_gndr Nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()

);

If OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

create table silver.crm_prd_info(

	prd_id int,
	prd_key Nvarchar(50),
	prd_nm Nvarchar(50),
	prd_cost int,
	prd_line Nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime,
	dwh_create_date datetime2 default getdate()



);

If OBJECT_ID ('silver.crm_sales_detail', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_detail;

create table silver.crm_sales_detail(

	sls_ord_num Nvarchar(50),	
	sls_prd_key Nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()


);

-- This part is for ERP Files


If OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

create table silver.erp_loc_a101(

	CID Nvarchar(50),
	CNTRY nvarchar(50),
	dwh_create_date datetime2 default getdate()

);

If OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;

create table silver.erp_cust_az12(

	CID Nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh_create_date datetime2 default getdate()

);

If OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

create table silver.erp_px_cat_g1v2(

	id Nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()

);







