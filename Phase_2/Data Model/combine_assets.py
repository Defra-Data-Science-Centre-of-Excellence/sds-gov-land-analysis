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
    "/dbfs/mnt/lab/restricted/ESD-Project/Defra_Land/Model_Grids"
)

alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

# location for outputs
par_path = Path(
    "/dbfs/mnt/lab/restricted/ESD-Project/Defra_Land/Assets"
)

alt_par_path = str(par_path).replace("/dbfs", "dbfs:")

le_path = Path(
    "/dbfs/mnt/lab/restricted/ESD-Project/sds-assets/10m"
)

alt_le_path = str(le_path).replace("/dbfs", "dbfs:")

# COMMAND ----------

# read in grid points
eng_combo_centroids = (
    sedona.read.format("parquet")
    .load(f"{alt_in_path}/{grid_square_size}m_england_grid_centroids.parquet")
    .repartition(500)
)
eng_combo_centroids.createOrReplaceTempView("eng_combo_centroids")

# COMMAND ----------

parquet_list_dense = [f"{alt_par_path}/{grid_square_size}m_x_dgl_fh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_dgl_lh.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_coniferous.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_deciduous.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_lcm_conif_wood.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_lcm_decid_wood.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_rpa_lc_woodland.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_nfi.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_phi_deciduous.parquet"]

parquet_list_sparse = [f"{alt_par_path}/{grid_square_size}m_x_dgl_fh.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_dgl_lh.parquet",
                      f"{alt_le_path}/{grid_square_size}m_x_le_scrub.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_wood_pasture_park.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_phi_orchard.parquet",
                      f"{alt_par_path}/{grid_square_size}m_x_fr_tow.parquet"]

# COMMAND ----------

data_combined_dense = eng_combo_centroids
data_combined_sparse = eng_combo_centroids

# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset
for parquet_path in parquet_list_dense:

    # Read the Parquet file
    parquet_df = sedona.read.format("parquet").load(parquet_path)
    
    # Join the base dataset with the current Parquet file on 'ID'
    data_combined_dense = data_combined_dense.join(parquet_df, on="ID", how="left")


# COMMAND ----------

# Loop through each Parquet file and join it with the base dataset
for parquet_path in parquet_list_sparse:

    # Read the Parquet file
    parquet_df = sedona.read.format("parquet").load(parquet_path)
    
    # Join the base dataset with the current Parquet file on 'ID'
    data_combined_sparse = data_combined_sparse.join(parquet_df, on="ID", how="left")

# COMMAND ----------

data_combined_dense.cache()
data_combined_dense.createOrReplaceTempView("data_combined_dense")

data_combined_sparse.cache()
data_combined_sparse.createOrReplaceTempView("data_combined_sparse")

# COMMAND ----------

data_combined_dense.write.format("parquet").mode("overwrite").save(
    f"{alt_par_path}/10m_x_assets_combined_dense.parquet"
)

data_combined_sparse.write.format("parquet").mode("overwrite").save(
    f"{alt_par_path}/10m_x_assets_combined_sparse.parquet"
)

# COMMAND ----------

data_combined_dense_reloaded = sedona.read.format("parquet").load(
    f"{alt_par_path}/10m_x_assets_combined_dense.parquet"
)

data_combined_sparse_reloaded = sedona.read.format("parquet").load(
    f"{alt_par_path}/10m_x_assets_combined_sparse.parquet"
)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

filtered_data = data_combined_dense_reloaded.filter((col("dgl_fh") == 1) | (col("dgl_lh") == 1))

filtered_data.write.format("parquet").mode("overwrite").save(
    f"{alt_par_path}/10m_x_assets_combined_dense_dgl.parquet"
)

# COMMAND ----------

filtered_data = data_combined_sparse_reloaded.filter((col("dgl_fh") == 1) | (col("dgl_lh") == 1))

filtered_data.write.format("parquet").mode("overwrite").save(
    f"{alt_par_path}/10m_x_assets_combined_sparse_dgl.parquet"
)

# COMMAND ----------


