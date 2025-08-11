# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Combine Habitat asset tables into single dataframe - Semi-Natural Grassland
# MAGIC
# MAGIC Miles Clement (miles.clement@defra.gov.uk)
# MAGIC
# MAGIC Last Updated 24/03/25

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC ####Packages

# COMMAND ----------

from pathlib import Path

# COMMAND ----------

from sedona.spark import *
from pyspark.sql import functions as F

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

username = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####User-defined Variables

# COMMAND ----------

# Define size of grid square
grid_square_size = 10

# COMMAND ----------

# Locations for data
# Normally wouldn't have this many, or do the path to a higher folder level instead once asset catalogue fully setup in dbfs
# Separate paths to pull in Living England, LCM and PHI
in_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Model_Grids"
)
alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

out_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Final/Asset_Tables"
)
alt_out_path = str(out_path).replace("/dbfs", "dbfs:")

par_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Assets"
)
alt_par_path = str(par_path).replace("/dbfs", "dbfs:")

le_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/sds-assets/10m/LE"
)
alt_le_path = str(le_path).replace("/dbfs", "dbfs:")

lcm_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/sds-assets/10m/LCM"
)
alt_lcm_path = str(lcm_path).replace("/dbfs", "dbfs:")

phi_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/sds-assets/10m/PHI"
)
alt_phi_path = str(phi_path).replace("/dbfs", "dbfs:")

# COMMAND ----------

# Create list of assets to be combined
parquet_list = [f"{alt_par_path}/{grid_square_size}m_x_dgl_fh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_dgl_lh.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_unimproved_grass.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_grassland_calaminarian.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_grassland_calcareous_lowland.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_grassland_dry_acid_lowland.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_lowland_meadows.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_purple_moor_rush_pastures.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_grassland_calcareous_upland.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_upland_hay_meadow.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_neutral_grassland.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_acid_grassland.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_calcareous_grassland.parquet"]


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ####Load in core datasets

# COMMAND ----------

# read in grid points
eng_combo_centroids = (
    sedona.read.format("parquet")
    .load(f"{alt_in_path}/{grid_square_size}m_england_grid_centroids.parquet")
    .repartition(500)
)
eng_combo_centroids.createOrReplaceTempView("eng_combo_centroids")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Processing

# COMMAND ----------

data_combined = eng_combo_centroids

# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset (using ID)
for parquet_path in parquet_list:

    parquet_df = sedona.read.format("parquet").load(parquet_path)

    data_combined = data_combined.join(parquet_df, on="ID", how="left")


# COMMAND ----------

data_combined.cache()
data_combined.createOrReplaceTempView("data_combined")

# COMMAND ----------

# Code to prevent double counting from specific habitat
# eg. arable and improved grassland equals 1, rather than 2 in the final counts when summed together
# Possibility for Living England, LCM and PHI

# Get column names from source datasets
lcm_columns = [col for col in data_combined.columns if col.startswith("lcm")]
phi_columns = [col for col in data_combined.columns if col.startswith("phi")]

# Create new columns combining dataset specific columns
# If any equal 1, than new columns equals 1
data_combined = data_combined.withColumn("lcm_comb", F.when(F.greatest(*[F.col(c) for c in lcm_columns]) == 1, 1).otherwise(None)) \
       .withColumn("phi_comb", F.when(F.greatest(*[F.col(c) for c in phi_columns]) == 1, 1).otherwise(None)) 

# Remove dataset/habitat specific columns 
exclude_columns = set(lcm_columns + phi_columns)
remaining_columns = [col for col in data_combined.columns if col not in exclude_columns]
data_combined = data_combined.select(*remaining_columns)

# COMMAND ----------

data_combined.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_assets_combined_grassland.parquet"
)

# COMMAND ----------


