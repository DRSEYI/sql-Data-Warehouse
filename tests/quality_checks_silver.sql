
 /* The bronze layer is first used to check for the mishap in the 
data quality, cleansing is done on another query page  before being 
checked again to verify that the cleansing was 100% and fit for the next
level 
This script performs various quality checks for data consistency
accuracy, and standardisation across the silver schemas. It includes checks for 
**Null or duplicates primary keys
**data standardisation and consistency
**Invalid date ranges and orders
**Data consistency between related fields 
 USAGE:
 --Run these checks after loading the silver layers  
 --Investigate and resolve any discrepancies found during the checks 
 ======================================================================

*/


--Check for Nulls or Duplicates in Primary Key
--Expectation: No Result

SELECT 
DISTINCT cst_id
FROM bronze.crm_cust_info
GROUP BY  cst_id
HAVING count(cst_id) >1  OR cst_id is NUll

--Check For Unwanted Spaces 
--Expectation: No result 
/* At this point we will  be checking all of the string columns 
lastname, firstname,gender
to attest that there are no spaces in between the strings */

SELECT 
cst_firstname, 
cst_lastname
FROM 
bronze.crm_cust_info
WHERE TRIM(cst_firstname) != cst_firstname 

---Data standardization & Consistency 
SELECT
DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT
DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT
prd_id,
COUNT(*) AS count
FROM bronze.crm_prd_info
GROUP BY  prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL   


--Check for unwanted spaces 
--Expectation: No result 
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Check for nulls or Negative Numbers
--Expectations: No result 
SELECT *, prd_cost 
FROM bronze.crm_prd_info 
WHERE prd_cost IS NULL  OR  prd_cost < 0


--Data standardization & Consistency 
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--Check for invalid date
--NB: end date must not be earlier than the start date 
SELECT * 
FROM bronze.crm_prd_info 
WHERE prd_end_dt < prd_start_dt




SELECT 
sls_cust_id,
COUNT(sls_cust_id),
SUM (sls_sales)
FROM bronze.crm_sales_details
GROUP BY sls_cust_id
HAVING count(sls_cust_id)>1
ORDER BY COUNT(sls_cust_id) DESC, SUM (sls_sales) DESC

--Check date validity for the date columns 
--- After cleaning we should get Operand type clash Error #. This is because the 
---datatype would have been casted as date in the silver table as against the initial INT 
SELECT 
sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt = 0  OR 
LEN(sls_order_dt) != 8 ---OR 
sls_order_dt > 205000000002;

SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE  sls_due_dt IS NULL



--business rule: sales = quantity * price 
--Negative , zeros, Nulls are not allowed 
SELECT DISTINCT
--sls_sales ,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 THEN  sls_quantity * abs(sls_price)
		ELSE sls_sales
END AS sls_sales,
sls_quantity, 
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0  OR sls_quantity < = 0 OR sls_price < =0
ORDER BY sls_sales, sls_quantity, sls_price 


SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen

FROM  bronze.erp_cust_az12

--Identify out-of-range date
SELECT  bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

---data standardization & Consistency 
SELECT 
cntry,
cid
FROM bronze.erp_loc_a101



