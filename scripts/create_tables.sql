/****************************************************************************************************
 Script Name   : bronze_layer_table_setup.sql
 Author        : [Your Name]
 Date Created  : [YYYY-MM-DD]
 
 Purpose:
   This script creates the foundational *bronze layer* tables within the data warehouse/lakehouse. 
   These tables are raw ingestion structures designed to store unprocessed data from CRM and ERP 
   source systems before transformations are applied in the silver and gold layers.

 Tables Created:
   - bronze.crm_cust_info       : Stores basic customer information from CRM
   - bronze.crm_prd_info        : Holds product master details from CRM
   - bronze.crm_sales_details   : Captures raw sales order transactions from CRM
   - bronze.erp_loc_a101        : ERP location lookup for country information
   - bronze.erp_px_cat_g1v2     : ERP product category and sub-category details
   - bronze.erp_cust_az12       : ERP customer demographic details (DOB, gender, etc.)

 Notes:
   - Existing tables with the same names will be dropped before creation (IF OBJECT_ID … DROP).
   - Data types are preserved as close to source formats as possible for traceability.
   - These tables should not be directly used for reporting—subsequent layers will 
     enforce business rules, cleaning, and conforming.

****************************************************************************************************/
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
cst_id				INT,
cst_key				NVARCHAR(50),
cst_firstname		NVARCHAR(50),
cst_lastname		NVARCHAR(50),
cst_marital_status	NVARCHAR(50),
cst_gndr			NVARCHAR(50),
cst_create_date		DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
prd_id			INT,
prd_key			NVARCHAR(50),
prd_nm			NVARCHAR(50),
prd_cost		INT,
prd_line		NVARCHAR(50),
prd_start_dt	DATETIME,
prd_end_dt		DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
sls_ord_num		NVARCHAR(50),
sls_prd_key		NVARCHAR(50),
sls_cust_id		INT,
sls_order_dt	INT,
sls_ship_dt		INT,
sls_due_dt		INT,
sls_sales		INT,
sls_quantity	INT,
sls_price		INT
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
cid		NVARCHAR(50),
cntry	NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
id			 NVARCHAR(50),
cat			 NVARCHAR(50),
subcat		 NVARCHAR(50),
maintainance NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
cid		NVARCHAR(50),
bdate	DATE,
gen		NVARCHAR(50)
);
