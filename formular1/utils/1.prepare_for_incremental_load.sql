-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables
-- MAGIC * clean up scripts

-- COMMAND ----------

-- drop all tables in database and database
-- and because they are managed tables, the data will also be deleted
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

-- recreate the database and point it to the data location
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/adfcourseanyistaccdl/processed-ctnr";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/adfcourseanyistaccdl/presentation-ctnr";
