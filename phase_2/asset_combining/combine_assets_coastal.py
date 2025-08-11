# Databricks notebook source
# MAGIC %md
# MAGIC # Combine Habitat asset tables into single dataframe - Coastal Margins & Marine
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
                      f"{alt_le_path}/{grid_square_size}m_x_le_saltmarsh.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_sand_dunes.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_saltmarsh.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_sand_dunes.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_vegetated_shingle.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_maritime_cliff.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_mudflats.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_saline_lagoons.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_saltwater.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_supra_rock.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_supra_sediment.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_littoral_rock.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_littoral_sediment.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_saltmarsh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_ne_marine.parquet"]

# Create list of assets to be combined - saltmarsh subset
parquet_list_saltmarsh = [f"{alt_par_path}/{grid_square_size}m_x_dgl_fh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_dgl_lh.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_saltmarsh.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_saltmarsh.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_saltmarsh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_ne_marine_saltmarsh.parquet"]

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

# Create output dataset
data_combined_full = eng_combo_centroids
data_combined_saltmarsh = eng_combo_centroids

# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset (using ID)
for parquet_path in parquet_list_all:

    parquet_df = sedona.read.format("parquet").load(parquet_path)

    data_combined_full = data_combined_full.join(parquet_df, on="ID", how="left")


# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset (using ID)
for parquet_path in parquet_list_saltmarsh:

    parquet_df = sedona.read.format("parquet").load(parquet_path)

    data_combined_saltmarsh = data_combined_saltmarsh.join(parquet_df, on="ID", how="left")

# COMMAND ----------

data_combined_full.cache()
data_combined_full.createOrReplaceTempView("data_combined_full")

data_combined_sm.cache()
data_combined_sm.createOrReplaceTempView("data_combined_sm")

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
    f"{alt_out_path}/10m_x_assets_combined_coastal.parquet"
)

# COMMAND ----------

data_combined_saltmarsh.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_assets_combined_saltmarsh.parquet"
)

# COMMAND ----------


