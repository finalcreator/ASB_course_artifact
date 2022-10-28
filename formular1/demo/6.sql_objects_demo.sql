-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objective
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating Managed Tables
-- MAGIC ###### Learning Objective
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table 
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the configuration notebook from here to import all the variables in it

-- COMMAND ----------

-- MAGIC %run 
-- MAGIC "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC race_results_python;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
FROM demo.race_results_python;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC using the CTAS stment to create table and insert data 

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

-- DROP TABLE demo.race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Tables
-- MAGIC * The only difference between external and managed tables is that
-- MAGIC   * Managed tables - spark manages both data and meta data
-- MAGIC     * To see the data, goto + icon > Data > DBFS > user > hive > warehouse > demo.db
-- MAGIC   * External tables - you are in charge of where data is saved and deleted
-- MAGIC     * hence even if meta-data is dropped, data is not deleted unless you explicitely delete or write a script to do so
-- MAGIC 
-- MAGIC ###### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Lets create an external table using python
-- MAGIC # Note: that for external tables, we can choose the file path we want our table data to be saved
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

-- using sql to create external tables
CREATE TABLE demo.race_results_ext_sql (
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING parquet -- if we are creating managed table in sql, we will stop here
LOCATION "/mnt/adfcourseanyistaccdl/presentation-ctnr/race_results_ext_sql" -- if we want to create external tables, then we have to put the location where we want the data kept

-- COMMAND ----------

--DROP TABLE demo.race_results_ext_py

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- insert into the table we just created using a SELECT stmt
INSERT INTO demo.race_results_ext_sql
SELECT *
FROM demo.race_results_ext_py
WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC ###### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Temp Views

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tvw_race_results
AS
SELECT 
*
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM tvw_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Global Temp Views

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gvw_race_results
AS
SELECT 
*
FROM demo.race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gvw_race_results;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Permanent Views

-- COMMAND ----------

CREATE OR REPLACE VIEW pvw_race_results
AS
SELECT
*
FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------


