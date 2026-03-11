/*
++++++++++++++++++++++++++++++++++++++
DDL script: Create Gold Views
++++++++++++++++++++++++++++++++++++++
*/




--Drop old view 
DROP VIEW IF EXISTS gold.dim_customers;
GO 

CREATE  VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER()OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id   AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,

	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  ---CRM as the gender master
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender, 
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN 
silver.erp_cust_az12 ca
ON ci.cst_key= ca.cid
LEFT JOIN
silver.erp_loc_a101 la
ON ci.cst_key = la.cid


===================================================================
  --Create the product dimension for gold layer 
===================================================================
--Drop old view 
DROP VIEW IF EXISTS gold.dim_products;
GO 
CREATE VIEW gold.dim_products AS 
SELECT 
ROW_NUMBER()OVER(ORDER BY cpi.prd_start_dt ,cpi.prd_key) AS product_key,
cpi.prd_id AS product_id,
cpi.prd_key AS product_number,
cpi.prd_nm AS product_name,
cpi.cat_id AS category_id,
epg.cat AS category,
epg.subcat AS subcategory,
epg.maintenance,
cpi.prd_cost AS cost,
cpi.prd_line AS product_line,
cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info  cpi
LEFT JOIN  silver.erp_px_cat_g1v2   epg
ON cpi.cat_id= epg.id
WHERE cpi.prd_end_dt IS NULL --Filter out all historical data

===================================================================
  --Create the product dimension for gold layer 
===================================================================










