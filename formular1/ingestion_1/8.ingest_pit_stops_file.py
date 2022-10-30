# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest MULTIPLE MULTILINE qualifying json file

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/raw-ctnr/raw/qualifying/

# COMMAND ----------

# see the schema
spark.read.json("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/qualifying/qualifying_split_1.json").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Reading MULTIPLE MULTILINE JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True)\
.json("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/qualifying/")
#.json("/mnt/adfcourseanyistaccdl/raw-ctnr/raw/qualifying/qualifying_split*.json")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new column
# MAGIC 1. Rename driverId and race Id
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("constructorId", "constructor_id")\
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to process container in parquet format

# COMMAND ----------

final_df.write\
.mode("overwrite")\
.parquet('/mnt/adfcourseanyistaccdl/processed-ctnr/qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC Test what you have written

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/processed-ctnr/qualifying

# COMMAND ----------

## lets try and read that data
df = spark.read.parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/qualifying")
display(df)

# COMMAND ----------


