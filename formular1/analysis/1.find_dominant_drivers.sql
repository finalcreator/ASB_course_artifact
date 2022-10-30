-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

-- lets see top ten drivers that dominated the races
SELECT 
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- lets see top ten drivers that dominated the races in the last dacade (2011 to 2020)
SELECT 
  driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- lets see top ten drivers that dominated the races in the last dacade (2001 to 2010)
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------


