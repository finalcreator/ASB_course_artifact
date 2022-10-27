# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read all the data as required

# COMMAND ----------

# MAGIC %md
# MAGIC run the configuration notebook from here to import all the variables in it

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# read data from the /mnt/adfcourseanyistaccdl/processed-ctnr/drivers
# also rename some fields that are no conflicts when we perform our join
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

# read data from the /mnt/adfcourseanyistaccdl/processed-ctnr/constructors
# also rename some fields that are no conflicts when we perform our join
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

# read data from the /mnt/adfcourseanyistaccdl/processed-ctnr/circuits
# also rename some fields that are no conflicts when we perform our join
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

# read data from the /mnt/adfcourseanyistaccdl/processed-ctnr/races
# also rename some fields that are no conflicts when we perform our join
races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

# read data from the /mnt/adfcourseanyistaccdl/processed-ctnr/results
# also rename some fields that are no conflicts when we perform our join
results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

# join two tables, 'races_df' and 'circuits_df' into one big table 'race_circuits_df'
race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

# join 4 tables, 'results_df', 'race_circuits_df', drivers_df, constructors_df
# this is becaus results_df is linked to all the other 3 tables
race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Select just the columns we will be needing for this report
final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp()) # add the date column

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'")\
        .orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to the presentation layer

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
