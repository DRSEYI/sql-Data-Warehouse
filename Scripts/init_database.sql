/*
====================================================================================================
Create Database and Schema 
====================================================================================================
Script Purpose:
	This script creates a new database called 'DataWarehouse'. It  checks if the database exists, drops it, and
	recreates it if found. Also, the script creates 3 schemas within the database to represent the bronze,
	silver and gold layers

Caution:
	Running this script will drop the entire 'DataWarehouse' database if it exists. All the data in the 
	The database will be permanently deleted. Ensure you have a backup before running the script
*/

USE  master;
GO 

IF EXISTS ( SELECT 1 FROM sys.databases WHERE name ='DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;

END;
GO

--create the Datawarehouse database 
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create the schema 

CREATE SCHEMA bronze;
GO


CREATE SCHEMA silver;
GO


CREATE SCHEMA gold;
GO
