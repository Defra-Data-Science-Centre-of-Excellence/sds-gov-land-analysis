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
                f"{alt_par_path}/{grid_square_size}m_x_moorland_line.parquet",
                f"{alt_le_path}/{grid_square_size}m_x_le_bog.parquet",
                f"{alt_le_path}/{grid_square_size}m_x_le_water.parquet",
                f"{alt_le_path}/{grid_square_size}m_x_le_fen_marsh_swamp.parquet",
                f"{alt_phi_path}/{grid_square_size}m_x_phi_blanket_bog.parquet",
                f"{alt_phi_path}/{grid_square_size}m_x_phi_lowland_fens.parquet",
                f"{alt_phi_path}/{grid_square_size}m_x_phi_lowland_raised_bog.parquet",
                f"{alt_phi_path}/{grid_square_size}m_x_phi_reedbeds.parquet",
                f"{alt_phi_path}/{grid_square_size}m_x_phi_upland_fens_swamps.parquet",
                f"{alt_lcm_path}/{grid_square_size}m_x_lcm_freshwater.parquet",
                f"{alt_lcm_path}/{grid_square_size}m_x_lcm_fen.parquet",
                f"{alt_lcm_path}/{grid_square_size}m_x_lcm_bog.parquet",
                f"{alt_par_path}/{grid_square_size}m_x_os_ngd_water.parquet"]


# COMMAND ----------

data_combined = eng_combo_centroids

# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset
for parquet_path in parquet_list:

    # Read the Parquet file
    parquet_df = sedona.read.format("parquet").load(parquet_path)
    
    # Join the base dataset with the current Parquet file on 'ID'
    data_combined = data_combined.join(parquet_df, on="ID", how="left")


# COMMAND ----------

data_combined.cache()
data_combined.createOrReplaceTempView("data_combined")

# COMMAND ----------

from pyspark.sql import functions as F

data_combined = data_combined.withColumn(
    "le_bog_low",
    F.when((F.col("le_bog") == 1) & (F.col("moorland_line").isNull()), 1).otherwise(None)
)

data_combined = data_combined.withColumn(
    "phi_bog_low",
    F.when((F.col("phi_blanket_bog") == 1) & (F.col("moorland_line").isNull()), 1).otherwise(None)
)

data_combined = data_combined.withColumn(
    "lcm_bog_low",
    F.when((F.col("lcm_bog") == 1) & (F.col("moorland_line").isNull()), 1).otherwise(None)
)

data_combined = data_combined.drop("le_bog","phi_blanket_bog","lcm_bog")
data_combined.createOrReplaceTempView("data_combined")


# COMMAND ----------

phi_columns = [col for col in data_combined.columns if col.startswith("phi")]
le_columns = [col for col in data_combined.columns if col.startswith("le")]
lcm_columns = [col for col in data_combined.columns if col.startswith("lcm")]

data_combined = data_combined.withColumn("phi_comb", F.when(F.greatest(*[F.col(c) for c in phi_columns]) == 1, 1).otherwise(None)) 
data_combined = data_combined.withColumn("le_comb", F.when(F.greatest(*[F.col(c) for c in le_columns]) == 1, 1).otherwise(None)) 
data_combined = data_combined.withColumn("lcm_comb", F.when(F.greatest(*[F.col(c) for c in lcm_columns]) == 1, 1).otherwise(None)) 

exclude_columns = set(phi_columns+le_columns+lcm_columns)
remaining_columns = [col for col in data_combined.columns if col not in exclude_columns]
data_combined = data_combined.select(*remaining_columns)

# COMMAND ----------

data_combined.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_assets_combined_water.parquet"
)

# COMMAND ----------


