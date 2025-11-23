use datawarehouse;

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