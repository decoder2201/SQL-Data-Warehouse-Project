-- CHECK FOR INVALID DATES
   -- NEGATIVE NUMBERS OR ZEROS CAN'T BE CAST TO A DATE
   -- ZEROS REPLACE WITH NULL WITH THE HELP OF NULL IF
   -- IN DATE SENERIO, THE LENGTH OF THE DATE MUST BE 8

SELECT
NULLIF(SLS_ORDER_DT,0) AS sls_order_dt
FROM bronze.crm_sales_detail
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
or sls_order_dt < 19000101;

SELECT
NULLIF(SLS_ship_DT,0) AS sls_ship_dt
FROM bronze.crm_sales_detail
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
or sls_ship_dt < 19000101;

SELECT
NULLIF(sls_due_dt,0) AS sls_due_dt
FROM bronze.crm_sales_detail
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
or sls_due_dt < 19000101;

-- check for invalid date orders

select
* 
from bronze.crm_sales_detail
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- check our sales with our buisness rules
   -- sales = quantity * price
   -- no negetives, nulls are not allowed.

   select 
   sls_quantity,   
   case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
             then sls_quantity * abs(sls_price)
        else sls_sales
   end as sls_sales,
   case when sls_price is null or sls_price <= 0
             then sls_sales /  nullif(sls_quantity,0)
        else sls_price
   end as sls_price
   from bronze.crm_sales_detail
   where sls_sales != sls_quantity * sls_price
   or sls_sales is null or sls_quantity is null or sls_price is null
   or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
   order by    sls_sales,   sls_quantity,   sls_price;


   -- if sales is -, 0, or null, drive it using quantity and price
   -- if price is zero or null, calculate it using sales and quantity
   -- if price is -, convert it to a + value.

   use datawarehouse;

   select * from silver.crm_sales_details;