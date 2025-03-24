# Databricks notebook source
# MAGIC %md
# MAGIC # Combine Habitat asset tables into single dataframe - Mountain, Moorland and Heath
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
parquet_list_all = [f"{alt_par_path}/{grid_square_size}m_x_dgl_fh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_dgl_lh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_moorland_line.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_dwarf_shrub_heath.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_bracken.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_bog.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_upland_heathland.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_lowland_heathland.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_mountain_heath_scrub.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_blanket_bog.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_upland_fens_swamps.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_heather.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_heather_grassland.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_bog.parquet"]

# Create list of assets to be combined - upland bog subset
parquet_list_bog = [f"{alt_par_path}/{grid_square_size}m_x_dgl_fh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_dgl_lh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_moorland_line.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_bog.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_bog.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_blanket_bog.parquet"]

# COMMAND ----------

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

data_combined_full = eng_combo_centroids
data_combined_bog = eng_combo_centroids

# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset (using ID)
for parquet_path in parquet_list_all:

    parquet_df = sedona.read.format("parquet").load(parquet_path)

    data_combined_full = data_combined_full.join(parquet_df, on="ID", how="left")


# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset (using ID)
for parquet_path in parquet_list_bog:

    parquet_df = sedona.read.format("parquet").load(parquet_path)

    data_combined_bog = data_combined_bog.join(parquet_df, on="ID", how="left")

# COMMAND ----------

data_combined_full.cache()
data_combined_full.createOrReplaceTempView("data_combined_full")

data_combined_bog.cache()
data_combined_bog.createOrReplaceTempView("data_combined_bog")

# COMMAND ----------

# Update full dataset so that only upland bog classes are included
# This is done at the rasterisation phase for the upland bog subset

data_combined_full = data_combined_full.withColumn(
    "le_bog_up",
    F.when((F.col("le_bog") == 1) & (F.col("moorland_line") == 1), 1).otherwise(None)
)

data_combined_full = data_combined_full.withColumn(
    "phi_bog_up",
    F.when((F.col("phi_blanket_bog") == 1) & (F.col("moorland_line") == 1), 1).otherwise(None)
)

data_combined_full = data_combined_full.withColumn(
    "lcm_bog_up",
    F.when((F.col("lcm_bog") == 1) & (F.col("moorland_line") == 1), 1).otherwise(None)
)

data_combined_full = data_combined_full.drop("le_bog","phi_blanket_bog","lcm_bog")
data_combined_full.createOrReplaceTempView("data_combined_full")

# COMMAND ----------

# Code to prevent double counting from specific habitat
# eg. arable and improved grassland equals 1, rather than 2 in the final counts when summed together
# Possibility for Living England, LCM and PHI

# Get column names from source datasets
le_columns = [col for col in data_combined_full.columns if col.startswith("le")]
lcm_columns = [col for col in data_combined_full.columns if col.startswith("lcm")]
phi_columns = [col for col in data_combined_full.columns if col.startswith("phi")]

# Create new columns combining dataset specific columns
# If any equal 1, than new columns equals 1
data_combined_full = data_combined_full.withColumn("le_comb", F.when(F.greatest(*[F.col(c) for c in le_columns]) == 1, 1).otherwise(None)) \
       .withColumn("lcm_comb", F.when(F.greatest(*[F.col(c) for c in lcm_columns]) == 1, 1).otherwise(None)) \
       .withColumn("phi_comb", F.when(F.greatest(*[F.col(c) for c in phi_columns]) == 1, 1).otherwise(None)) 

# Remove dataset/habitat specific columns 
exclude_columns = set(le_columns + lcm_columns + phi_columns)
remaining_columns = [col for col in data_combined_full.columns if col not in exclude_columns]
data_combined_full = data_combined_full.select(*remaining_columns)

# COMMAND ----------

data_combined_full.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_assets_combined_moorland.parquet"
)

# COMMAND ----------

data_combined_bog.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_assets_combined_upland_bog.parquet"
)

# COMMAND ----------


