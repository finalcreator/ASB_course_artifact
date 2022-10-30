# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

# Import the datatypes
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/raw-ctnr/raw/

# COMMAND ----------

#circuits_df = spark.read \
#.option("header", True) \
#.csv('dbfs:/mnt/adfcourseanyistaccdl/raw-ctnr/raw/races.csv')

# COMMAND ----------

# add schema
races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False), # not nullable
    StructField("year", IntegerType(), True), 
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

# read the data
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/races.csv")

# COMMAND ----------

# quick look at data
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Add ingest date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# we want to concact the date and time together into one column
# to_timestamp() accepts timestamp and format to display
# concat() concactenates the date and time, saparated by a space leteral
races_wt_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

display(races_wt_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step  3 - Select only the columns required & rename as required

# COMMAND ----------

races_selected_df = races_wt_timestamp_df.select(
    col('raceId').alias('race_id'),
    col('year').alias('race_year'),
    col('round'),
    col('circuitId').alias('circuit_id'),
    col('name'),
    col('ingestion_date'),
    col('race_timestamp')
)

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the output to process container in parquet format

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl

# COMMAND ----------

# MAGIC %md
# MAGIC * partition by race_year
# MAGIC   * It will put data belonging to each year into a separate folder
# MAGIC     * making it more efficient to search

# COMMAND ----------

#races_selected_df.write.mode('overwrite').parquet('/mnt/adfcourseanyistaccdl/processed-ctnr/races')
# partition the data by race_year
# vedio 48
races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/adfcourseanyistaccdl/processed-ctnr/races')

# COMMAND ----------

# MAGIC %md
# MAGIC Test what you have written

# COMMAND ----------

# MAGIC 
# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/processed-ctnr/races

# COMMAND ----------

## lets try and read that data
df = spark.read.parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/races")
display(df)

# COMMAND ----------


