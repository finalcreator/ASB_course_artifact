# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest MULTILINE pit_stop.json file

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/raw-ctnr/raw/

# COMMAND ----------

# see the schema
spark.read.json("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/pit_stops.json").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Reading MULTILINE JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True)\
.json("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new column
# MAGIC 1. Rename driverId and race Id
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                       .withColumnRenamed("driverId", "driver_id")\
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to process container in parquet format

# COMMAND ----------

final_df.write\
.mode("overwrite")\
.parquet('/mnt/adfcourseanyistaccdl/processed-ctnr/pit_stops')

# COMMAND ----------

# MAGIC %md
# MAGIC Test what you have written

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/processed-ctnr/pit_stops

# COMMAND ----------

## lets try and read that data
df = spark.read.parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/pit_stops")
display(df)

# COMMAND ----------


