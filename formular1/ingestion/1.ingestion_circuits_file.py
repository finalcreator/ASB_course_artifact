# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Documentation Page
# MAGIC https://spark.apache.org/docs/latest/api/python/index.html
# MAGIC 
# MAGIC * API Reference

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# display all the mountables
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC * NOTE: %fs is a shotcut to dbutils.fs.mounts()
# MAGIC 
# MAGIC 
# MAGIC Drill down further into raw-cntr

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/raw-ctnr

# COMMAND ----------

# MAGIC %md
# MAGIC Drill down further into the raw folder

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/raw-ctnr/raw/

# COMMAND ----------

# you can unmout this way
##dbutils.fs.unmount('/mnt/adfcourseanyistaccdl/raw')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read CSV File

# COMMAND ----------

# Import the datatypes
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# specify schema
# Note StructType represents rows and StructField represents columns
circuits_schema = StructType(
    fields=[
         StructField("circuitId", IntegerType(), False), #primary key
         StructField("circuitRef", StringType(), True),
         StructField("name", StringType(), True),
         StructField("location", StringType(), True),
         StructField("country", StringType(), True),
         StructField("lat", DoubleType(), True),
         StructField("lng", DoubleType(), True),
         StructField("alt", IntegerType(), True),
         StructField("url", StringType(), True),
    ])

# COMMAND ----------

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.csv.html#pyspark.sql.DataFrameReader.csv
#circuits_df = spark.read.csv('dbfs:/mnt/adfcourseanyistaccdl/raw-ctnr/raw/circuits.csv', header=true)
# read csv with the headers recognised
#circuits_df = spark.read.option("header", True).csv('dbfs:/mnt/adfcourseanyistaccdl/raw-ctnr/raw/circuits.csv')

# print with inferScheme to infere the data types
# however this is not an efficient mtd for production
#circuits_df = spark.read \
#.option("header", True)\
#.option("inferSchema", True)\
#.csv('dbfs:/mnt/adfcourseanyistaccdl/raw-ctnr/raw/circuits.csv')


circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv('/mnt/adfcourseanyistaccdl/raw-ctnr/raw/circuits.csv') 
# do not have to use .csv('dbfs:/mnt/adfcourseanyistaccdl/raw-ctnr/raw/circuits.csv')

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

# Show first 20 records
circuits_df.show()

# COMMAND ----------

# Better way to display tables
display(circuits_df)

# COMMAND ----------

# See the schema of our data
circuits_df.printSchema()

# COMMAND ----------

# describe
circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select only required columns
# MAGIC 
# MAGIC * Ther are 4 main selection methods

# COMMAND ----------

# 1st mtd
circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
display(circuits_selected_df)

# COMMAND ----------

# 2nd mtd
circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
display(circuits_selected_df)

# COMMAND ----------

# 3rd mtd
circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])
display(circuits_selected_df)

# COMMAND ----------

# 4th mtd
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns
# MAGIC * There are 2 mtds of renaming columns
# MAGIC   * alias
# MAGIC   * withColumnRenamed

# COMMAND ----------

#use alias to rename columns
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))
display(circuits_selected_df)

# COMMAND ----------

# using withColumnRenamed to rename columns
circuits_renamed_df = circuits_selected_df \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumnRenamed("race_country", "country")

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4 - Add column to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
# add the ingestion_date column to the dataframe
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

display(circuits_final_df)

# COMMAND ----------

from pyspark.sql.functions import lit
# What if its not a literal value and not a function, 
# then you will have to wrap it around the lit() function
circuits_test_df = circuits_final_df.withColumn("env", lit("production"))

display(circuits_test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write the dataframe into a parquet format

# COMMAND ----------

# specify a folder 'circutes' to write contents to
# inside the container /mnt/adfcourseanyistaccdl/processed-ctnr/
#circuits_final_df.write.parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/circuits")
# make it rerunable by overwriting the data

circuits_final_df.write.mode("overwrite").parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC * lets use the ls comand to list the file system in the path: /mnt/adfcourseanyistaccdl/processed-ctnr/circuits

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adfcourseanyistaccdl/processed-ctnr/circuits

# COMMAND ----------

## lets try and read that data
df = spark.read.parquet("/mnt/adfcourseanyistaccdl/processed-ctnr/circuits")
display(df)
