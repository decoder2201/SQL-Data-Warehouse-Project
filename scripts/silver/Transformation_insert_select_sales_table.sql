use datawarehouse;

IF OBJECT_ID ('SILVER.CRM_SALES_DETAILS', 'U') IS NOT NULL
   DROP TABLE SILVER.CRM_SALES_DETAILS;
CREATE TABLE SILVER.CRM_SALES_DETAILS (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_qunatity INT,
    sls_price INT,
    dwh_create_date datetime2 default getdate()
);


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


