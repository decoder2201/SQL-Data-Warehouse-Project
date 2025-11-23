use datawarehouse;

IF OBJECT_ID ('SILVER.CRM_PRD_INFO', 'U') IS NOT NULL
   DROP TABLE SILVER.CRM_PRD_INFO;
CREATE TABLE SILVER.CRM_PRD_INFO (
    prd_id int,
    prd_key nvarchar(50),
    cat_id nvarchar(50),
    prd_nm nvarchar(50),
    prd_cost int,
    prd_line nvarchar(50),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date datetime2 default getdate()
);



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