# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/raw-ctnr/raw/

# COMMAND ----------

#/mnt/adfcourseanyistaccdl/raw-ctnr/raw/results.json
spark.read.json("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/results.json").printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True),
])

# COMMAND ----------

result_df = spark.read \
.schema(results_schema) \
.json("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/results.json")

# COMMAND ----------

display(result_df)

# COMMAND ----------

result_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

result_with_columns_df = result_df.withColumnRenamed("resultId", "result_id")\
                                  .withColumnRenamed("raceId", "race_id")\
                                  .withColumnRenamed("driverId", "driver_id")\
                                  .withColumnRenamed("constructorId", "constructor_id")\
                                  .withColumnRenamed("positionText", "position_text")\
                                  .withColumnRenamed("positionOrder", "position_order")\
                                  .withColumnRenamed("fastestLap", "fastest_lap")\
                                  .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                  .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(result_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns

# COMMAND ----------

result_final_df = result_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

result_final_df.write\
.mode("overwrite")\
.parquet('/mnt/adfcourseanyistaccdl/processed-ctnr/results')

# COMMAND ----------

# MAGIC %md
# MAGIC Test what you have written

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/processed-ctnr/results

# COMMAND ----------

## lets try and read that data
df = spark.read.parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/results")
display(df)

# COMMAND ----------


