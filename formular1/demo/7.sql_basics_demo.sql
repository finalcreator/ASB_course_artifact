-- Databricks notebook source
-- to see all the avaliable databases
SHOW DATABASES;

-- COMMAND ----------

-- to see the current database that is active
SELECT CURRENT_DATABASE()

-- COMMAND ----------

-- to use another database, e.g. f1_processed
USE f1_processed;

-- COMMAND ----------

-- show tables in f1_processed
SHOW TABLES;

-- COMMAND ----------

SELECT
*
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT
*
FROM f1_processed.drivers
LIMIT 10;

-- COMMAND ----------

DESC f1_processed.drivers;

-- COMMAND ----------

SELECT *
FROM drivers
WHERE nationality = 'British';

-- COMMAND ----------

SELECT *
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01';

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01';

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'
ORDER BY dob;

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'
ORDER BY dob DESC;

-- COMMAND ----------

SELECT *
FROM drivers
ORDER BY 
nationality ASC,
dob DESC;

-- COMMAND ----------

SELECT name, nationality, dob AS date_of_birth
FROM drivers
WHERE (nationality = 'British' AND dob >= '1990-01-01') OR nationality = 'Indian'
ORDER BY dob DESC;

-- COMMAND ----------


