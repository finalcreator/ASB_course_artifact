# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest MULTIPLE csv files

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/raw-ctnr/raw/lap_times/

# COMMAND ----------

# see the schema
spark.read.csv("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/lap_times/lap_times_split_1.csv").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Reading CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

lap_times_schema = spark.read \
.schema(pit_stops_schema) \
.csv("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/lap_times/")
#.csv("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

display(lap_times_schema)

# COMMAND ----------

lap_times_schema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new column
# MAGIC 1. Rename driverId and race Id
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_schema.withColumnRenamed("raceId", "race_id") \
                       .withColumnRenamed("driverId", "driver_id")\
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to process container in parquet format

# COMMAND ----------

final_df.write\
.mode("overwrite")\
.parquet('/mnt/adfcourseanyistaccdl/processed-ctnr/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC Test what you have written

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/processed-ctnr/lap_times

# COMMAND ----------

## lets try and read that data
df = spark.read.parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/lap_times")
display(df)

# COMMAND ----------


