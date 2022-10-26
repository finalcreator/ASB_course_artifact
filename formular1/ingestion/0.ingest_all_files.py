# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

# lets call 8b.ingestion_parameterize_nbs from this noteook and add a parameter value to it before the run
value_result = dbutils.notebook.run("8b.ingestion_parameterize_nbs", 0, {"param_data_source": "Ergast API"})

# We can now click on the notebook job to open it

# COMMAND ----------

## Hence if we want to chain other nothebooks to run 
# we can write a conditional stmt to run only if this is a success
value_result
