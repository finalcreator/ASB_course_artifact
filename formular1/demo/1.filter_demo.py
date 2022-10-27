# Databricks notebook source
# MAGIC %md
# MAGIC ##### Get the configuration from the configuration notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read data from this path

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### filter based on condition

# COMMAND ----------

#races_filtered_df = races_df.filter("race_year = 2019")
#races_filtered_df = races_df.filter(races_df["race_year"] == 2019)
races_filtered_df = races_df.filter(races_df.race_year == 2019)

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

#races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")
#races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))
races_filtered_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))

# NOTE: you can also change the filters to where
# races_filtered_df = races_df.where("race_year = 2019 and round <= 5")
# races_filtered_df = races_df.where((races_df.race_year == 2019) & (races_df.round <= 5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


