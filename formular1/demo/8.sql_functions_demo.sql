-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SQL built in functions:
-- MAGIC https://spark.apache.org/docs/latest/api/sql/index.html

-- COMMAND ----------

SELECT
*
FROM drivers;

-- COMMAND ----------

SELECT
*, CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

-- split name based on space
SELECT
*, SPLIT(name, ' ') AS split_name
FROM drivers;

-- COMMAND ----------

-- split and saparate the names into 2 columns
SELECT
*, SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[1] surename 
FROM drivers;

-- COMMAND ----------

SELECT
*, current_timestamp 
FROM drivers;

-- COMMAND ----------

SELECT
*, date_format(dob, 'dd-MM-yyyy') AS new_dob_fmt
FROM drivers;

-- COMMAND ----------

-- add days to everybodys dob
SELECT
*, date_add(dob, 1)
FROM drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Aggregate Data

-- COMMAND ----------

SELECT COUNT(*) FROM drivers;

-- COMMAND ----------

SELECT MAX(dob)
FROM drivers;

-- COMMAND ----------

SELECT
*
FROM drivers
WHERE dob = '2000-05-11'

-- COMMAND ----------

SELECT
COUNT(*)
FROM drivers
WHERE nationality = 'British'

-- COMMAND ----------

SELECT
nationality, COUNT(*)
FROM drivers
GROUP BY nationality;

-- COMMAND ----------

SELECT
nationality, COUNT(*)
FROM drivers
GROUP BY nationality
ORDER BY nationality;

-- COMMAND ----------

SELECT
nationality, COUNT(*)
FROM drivers
GROUP BY nationality
HAVING COUNT(*) > 100
ORDER BY nationality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Window Functions

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality, age_rank;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Joins in SQL

-- COMMAND ----------


