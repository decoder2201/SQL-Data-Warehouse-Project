use datawarehouse;

select 
id,
cat,
subcat,
maintenance
from
bronze.erp_px_cat_g1v2;


-- check for unwanted spaces

select 
*
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or
subcat != trim(subcat) or
maintenance != trim(maintenance);

-- data standarization and consistency

select distinct maintenance from bronze.erp_px_cat_g1v2;

select * from silver.erp_px_cat_g1v2;
