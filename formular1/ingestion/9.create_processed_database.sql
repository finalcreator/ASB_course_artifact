-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###### Crwate a Database for our Managed Tables
-- MAGIC Since we want to create managed tables, we have to specify the location where the tables data will be created
-- MAGIC * if we do not spacify a location the data will be created under the databricks default location dbfs:/user/hive/warehouse

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/adfcourseanyistaccdl/processed-ctnr"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------


