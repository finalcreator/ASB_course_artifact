# Databricks notebook source
# MAGIC %md
# MAGIC #### Function adds a new timestamp column to a dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
#this function adds a new timestamp column to a dataframe 
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------


