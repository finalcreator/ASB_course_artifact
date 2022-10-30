# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- set up a database with a location where we will store our data
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/adfcourseanyistaccdl/demo-ctnr'

# COMMAND ----------

# read the data we want to use
results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/adfcourseanyistaccdl/raw-ctnr/2021-03-28/results.json")

# COMMAND ----------

# writes only the data into the container
#results_df.write.format("delta").mode("overwrite").saveAsTable("/mnt/adfcourseanyistaccdl/demo-ctnr/results_external")

# This creates a managed table to hold the meta-data and 
# writes the data into the container location we already setup and assigned when we created our database f1_demo on line one
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC --read data from that table we created in delta format
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------


