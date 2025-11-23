USE datawarehouse;

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