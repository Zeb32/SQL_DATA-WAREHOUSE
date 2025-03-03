/*
------ Create DataBase & Schema ---------

The Purpose of this Script is to Create Database and Schema for Our Data Warehouse project. 
And also Create Schema for our all Three Lyers This is the Initiol Step In any Data warehouse 
Project
*/

-- Active Master DataBase in Order to Create Our Database
USE master

-- Drop & Create DataBase 
DROP DATABASE DataWarehouse
CREATE DATABASE DataWarehouse 

-- Active The Database
USE DataWarehouse

-- Create Schema for All Layer
CREATE SCHEMA Bronze GO
CREATE SCHEMA Silver GO
CREATE SCHEMA Gold GO
