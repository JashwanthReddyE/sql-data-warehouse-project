/*
Script Name   : Create_DataWarehouse.sql
   Purpose       : This script recreates a clean 'DataWarehouse' database with a layered schema 
                   design (bronze, silver, gold) for data ingestion, transformation, and analytics.
                   - bronze: Raw, unprocessed data
                   - silver: Cleansed and standardized data
                   - gold  : Business-ready curated data

   Warnings      : 
   1. This script will DROP the existing 'DataWarehouse' database if it exists.
   2. All objects and data in the 'DataWarehouse' database will be permanently lost.
   3. Ensure proper backup before running this script in production environments.
*/
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMIDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
