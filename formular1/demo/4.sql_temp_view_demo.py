# Databricks notebook source
# MAGIC %md
# MAGIC ### Accessing dataframes using SQL
# MAGIC 
# MAGIC #### Objectives
# MAGIC 1. Create temp views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell

# COMMAND ----------

# MAGIC %md
# MAGIC Run the configuration notebook from here to import all the variables in it

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Create a temp views
# MAGIC * temp views are only valied within a spark session
# MAGIC * not avaliable for another notebook other than this one
# MAGIC * hence, once the cluster terminates, view will no longer be avaliable even if you restart the cluster

# COMMAND ----------

#race_results_df.createTempView("vw_race_results")
race_results_df.createOrReplaceTempView("vw_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vw_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM vw_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# Running SQL stmts from python
# gives you the ability to put sql data into a dataframe
# hence you can now use the datafframe to do whatever
race_results_2019_df = spark.sql("SELECT * FROM vw_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# another benefit of running SQL smts from python is that you can parameterize
param_race_year = 2019
race_results_2019_df = spark.sql(f"SELECT * FROM vw_race_results WHERE race_year = {param_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------


