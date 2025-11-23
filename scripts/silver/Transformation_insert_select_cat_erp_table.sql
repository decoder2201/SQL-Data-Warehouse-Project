USE datawarehouse;

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