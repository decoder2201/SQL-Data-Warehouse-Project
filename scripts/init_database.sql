/*

Create Database and Schemas

===================================================================================================

Script Purpose:
              This script creates a new database named 'DataWarehouse' after checking if it already exists. If the database exists, it is dropped and recreated.
              Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
       Running this script will drop the entire 'DataWarehouse' database if it exists. All data in the database will be permanently deleted.
       Proceed with caution and ensure you have proper backups before running this script.
*/

use master;

-- Drop and recreate the 'datawarehouse' database

if exists (select 1 from sys.databases where name = 'datawarehouse')
begin
     alter database datawarehouse set single_user with rollback immediate;
     drop database datawarehouse;
end;
go

-- craete database with a name
create database datawarehouse;

-- run command for use that new database  
use datawarehouse;

-- after that we created 3 schemas a/c to our data management approach
create schema bronze;
create schema silver;
create schema gold;

-- GO => separate batches when working with multiple SQL statements
