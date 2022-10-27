# Databricks notebook source
# can not access temp view from another notebook
%sql
SELECT *
FROM tvw_race_results

# COMMAND ----------

# can access a global temp view from another notebook
%sql
SELECT *
FROM global_temp.gvw_race_results

# COMMAND ----------


