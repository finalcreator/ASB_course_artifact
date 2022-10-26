# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

# ltes call 8b.ingestion_parameterize_nbs from this noteook and add a parameter value to it before the run
dbutils.notebook.run("8b.ingestion_parameterize_nbs", 0, {"param_data_source": "Ergast API"})

# We can now click on the notebook job to open it

# COMMAND ----------


