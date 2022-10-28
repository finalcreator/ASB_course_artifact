# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce driver standings

# COMMAND ----------

# MAGIC %md
# MAGIC Run the configuration notebook from here to import all the variables in it

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

# MAGIC %md
# MAGIC ###### GroupBy Function

# COMMAND ----------

## We want to get the total points and wins each driver has per year
driver_standings_df = race_results_df\
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins") # we only want to count when the position is 1, we do not want to count the rest
)

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Windows Function

# COMMAND ----------

# We want to rank the drivers in desending order per year
# we can use the windows function to achieve this

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, desc

# Partition by race year, then order by total points and wins for each race year
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

# Apply our window() function to our groupBy() function using rank() function in a new column
# hence we are using the rank() function to go over our grouped dataset using the windows specification driver_rank_spec
# also note that withColumn() is used to add a new column "rank" to the dataframe
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a Managed Table and  Write to the data lake blob container

# COMMAND ----------

# we can now create a managed table in the database we created in "4.create_presentation_database"
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------


