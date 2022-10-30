-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

-- create 1st temp view
CREATE OR REPLACE TEMP VIEW vw_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

select * FROM vw_driver_standings_2018

-- COMMAND ----------

-- create 2nd temp view
CREATE OR REPLACE TEMP VIEW vw_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

select * FROM vw_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Inner Join

-- COMMAND ----------

-- Get the drivers who raced on both 2018 and 2020
SELECT *
FROM vw_driver_standings_2018 d_2018
JOIN vw_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Left Join

-- COMMAND ----------

-- get all drivers who raced in 2018 irrespective of if they raced in 2020
SELECT *
FROM vw_driver_standings_2018 d_2018
LEFT JOIN vw_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Right Join

-- COMMAND ----------

-- get all drivers who raced in 2020 irrespective of if they raced in 2018
SELECT *
FROM vw_driver_standings_2018 d_2018
RIGHT JOIN vw_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Full Join

-- COMMAND ----------

-- get all drivers who raced in ether 2018 or 2020
SELECT *
FROM vw_driver_standings_2018 d_2018
FULL JOIN vw_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Semi Join

-- COMMAND ----------

-- Inner join with only data from the left table
-- Get the drivers who raced on both 2018 and 2020 (BUT fetch only the columns from the left table e.g. 2018 table)
-- this makes more sence if we have different cols in both tables (however, in this example both tables have same columns)
SELECT *
FROM vw_driver_standings_2018 d_2018
SEMI JOIN vw_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Anti Join

-- COMMAND ----------

-- drivers who raced in 2018 but did not race in 2020 (vice versa)
SELECT *
FROM vw_driver_standings_2018 d_2018
ANTI JOIN vw_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Cross Join

-- COMMAND ----------

-- combination of every column in left table with every column in the right table
-- be carefull using cross joins
SELECT *
FROM vw_driver_standings_2018 d_2018
CROSS JOIN vw_driver_standings_2020 d_2020

-- COMMAND ----------


