-- Databricks notebook source
-- To get the dominant teams overall
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points 
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- OVER() usually takes 2 params, PARTITION BY & ORDER BY
-- In this case We do not need to partition by anything 
-- because we are ranking by the whole population of the data
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank  
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- lets get the race years
SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * to get the race years for just the selected drivers we are interested in (top ten)
-- MAGIC   * We need to 1st create a temp view

-- COMMAND ----------

-- create a temp view to find out how drivers performed overall
CREATE OR REPLACE TEMP VIEW tvw_dominant_teams
AS
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT * FROM tvw_dominant_teams

-- COMMAND ----------

-- tets get the years for just the drivers that ranked top 10
-- this will give top 10 drivers and thier performance over the years
-- NOTE we can also perform a join b/w this table and the temp table, then we can sve the new result
SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE team_name IN (SELECT team_name FROM tvw_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


