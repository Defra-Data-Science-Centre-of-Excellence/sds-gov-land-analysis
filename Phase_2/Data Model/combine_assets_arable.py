# Databricks notebook source
# MAGIC %pip install keplergl pydeck mapclassify rtree pygeos geopandas==1.0.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from sedona.spark import *

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

username = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

# COMMAND ----------

# Define size of grid square
grid_square_size = 10

# COMMAND ----------

from pathlib import Path

# location for input grid/centroids
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

# read in grid points
eng_combo_centroids = (
    sedona.read.format("parquet")
    .load(f"{alt_in_path}/{grid_square_size}m_england_grid_centroids.parquet")
    .repartition(500)
)
eng_combo_centroids.createOrReplaceTempView("eng_combo_centroids")

# COMMAND ----------

parquet_list = [f"{alt_par_path}/{grid_square_size}m_x_dgl_fh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_dgl_lh.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_improved grass.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_arable.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_coast_floodplain_grazing_marsh.parquet",
                      f"{alt_phi_path}/{grid_square_size}m_x_phi_grassland_semi_improved_good.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_arable.parquet",
                      f"{alt_lcm_path}/{grid_square_size}m_x_lcm_improved_grassland.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_crome.parquet"]

# COMMAND ----------

data_combined_full = eng_combo_centroids

# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset
for parquet_path in parquet_list:

    # Read the Parquet file
    parquet_df = sedona.read.format("parquet").load(parquet_path)
    
    # Join the base dataset with the current Parquet file on 'ID'
    data_combined_full = data_combined_full.join(parquet_df, on="ID", how="left")


# COMMAND ----------

data_combined_full.cache()
data_combined_full.createOrReplaceTempView("data_combined_full")

# COMMAND ----------

from pyspark.sql import functions as F

le_columns = [col for col in data_combined_full.columns if col.startswith("le")]
phi_columns = [col for col in data_combined_full.columns if col.startswith("phi")]
lcm_columns = [col for col in data_combined_full.columns if col.startswith("lcm")]

data_combined_full = data_combined_full.withColumn("le_comb", F.when(F.greatest(*[F.col(c) for c in le_columns]) == 1, 1).otherwise(None)) \
       .withColumn("phi_comb", F.when(F.greatest(*[F.col(c) for c in phi_columns]) == 1, 1).otherwise(None)) \
       .withColumn("lcm_comb", F.when(F.greatest(*[F.col(c) for c in lcm_columns]) == 1, 1).otherwise(None)) 


exclude_columns = set(le_columns+phi_columns+lcm_columns)
remaining_columns = [col for col in data_combined_full.columns if col not in exclude_columns]
data_combined_full = data_combined_full.select(*remaining_columns)

# COMMAND ----------

data_combined_full.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_assets_combined_arable.parquet"
)

# COMMAND ----------


