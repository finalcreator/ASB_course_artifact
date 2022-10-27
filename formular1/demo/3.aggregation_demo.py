# Databricks notebook source
# MAGIC %md
# MAGIC run the configuration notebook from here to import all the variables in it

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Aggregation functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Built-in Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### filter data related to only 2020

# COMMAND ----------

#demo_df = race_results_df.filter("race_year=2020")
demo_df = race_results_df.filter(race_results_df.race_year==2020)

# COMMAND ----------

display(demo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

# COMMAND ----------

## These stmts will give the same results
#demo_df.select(count("*")).show()
#demo_df.select(count("race_name")).show()
demo_df.select(count(demo_df.race_name)).show()

# COMMAND ----------

## to get the distinct races
demo_df.select(countDistinct("race_name")).show() 

# COMMAND ----------

demo_df.select(sum("points")).show() 

# COMMAND ----------

demo_df.filter(demo_df.driver_name == 'Lewis Hamilton')\
.select(sum(demo_df.points))\
.show() 

# COMMAND ----------

## Selecting multiple columns
demo_df.filter(demo_df.driver_name == 'Lewis Hamilton')\
.select(sum(demo_df.points), countDistinct(demo_df.race_name))\
.show() 

# COMMAND ----------

## Selecting multiple columns and rename the columns (1st mtd)
demo_df.filter(demo_df.driver_name == 'Lewis Hamilton')\
.select(sum(demo_df.points), countDistinct(demo_df.race_name))\
.withColumnRenamed("sum(points)", "total_points")\
.withColumnRenamed("count(DISTINCT race_name)", "number_of_races")\
.show()

# COMMAND ----------

## Selecting multiple columns and rename the columns (2nd mtd)
demo_df.filter(demo_df.driver_name == 'Lewis Hamilton')\
.select(sum(demo_df.points).alias("total_points"), countDistinct(demo_df.race_name).alias("number_of_races"))\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Groupby Functions

# COMMAND ----------

## This is a pyspark.sql.group.GroupedData
## If you go to the doc, there are a number of functions that can be applied in a GroupedData
demo_df.groupBy(demo_df.driver_name)

# COMMAND ----------

demo_df\
.groupBy("driver_name")\
.sum("points")\
.show()

# COMMAND ----------

# this will NOT work
# you can not assign more than one agg function to a groupBy
# except ypu use the agg()

#demo_df\
#.groupBy("driver_name")\
#.sum("points")\
#.countDistinct("race_name")\
#.show()

## multiple agg functions to a groupBy() using the agg()
demo_df\
.groupBy("driver_name")\
.agg(sum("points"), countDistinct("race_name"))\
.show()

# COMMAND ----------

## multiple agg functions to a groupBy() using the agg() AND renaming cols
demo_df\
.groupBy("driver_name")\
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ###### filter data related to 2019 and 2020

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

# Now that we have filtered by two years, we can now group by race year and driver name
demo_df\
.groupBy("race_year", "driver_name")\
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))\
.show()

# COMMAND ----------

# Now that we have filtered by two years, we can now group by race year and driver name
demo_grouped_df = demo_df\
.groupBy("race_year", "driver_name")\
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Windows Function can be applyed to a groupby function
# MAGIC * How do you want to partition the data
# MAGIC   * e.g., we want to treat the years separately
# MAGIC     * we want to treat 2019 separate from 2020
# MAGIC     
# MAGIC * Hence our partitioned col is the race_year
# MAGIC * Order by total_points for each partition
# MAGIC * Apply a function e.g., rank() to produce the output

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

# Create window specification
# Partition by race year, then order by total points for each race year
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

# COMMAND ----------

# Apply our window() function to our groupBy() function using rank() function in a new column
# hence we are using the rank() function to go over our grouped dataset using the windows specification driverRankSpec
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(50)

# COMMAND ----------

dis
