# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect National Forest Inventory (2022) with Data Model Grid (10m)
# MAGIC
# MAGIC Miles Clement (miles.clement@defra.gov.uk)
# MAGIC
# MAGIC Last Updated 17/03/25

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC ####Packages

# COMMAND ----------

from pathlib import Path
import geopandas as gpd
from geopandas import read_file
import pandas as pd

# COMMAND ----------

from sedona.spark import *
from sedona.sql import st_constructors as cn
from pyspark.sql.functions import lit, expr, col, like, sum
from sedona.sql import st_functions as fn

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC ####User-defined Variables

# COMMAND ----------

# DBTITLE 1,USER INPUTS
# Define size of grid square
grid_square_size = 10

# COMMAND ----------

# location for input grid/centroids
in_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Model_Grids"
)

alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

# location for outputs
out_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Assets"
)

alt_out_path = str(out_path).replace("/dbfs", "dbfs:")

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
# MAGIC ####Load in and explore asset dataset

# COMMAND ----------

focal_path = "dbfs:/mnt/base/unrestricted/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england_2022/format_GEOPARQUET_national_forest_inventory_woodland_england_2022/LATEST_national_forest_inventory_woodland_england_2022/National_Forest_Inventory_England_2022.parquet"

# COMMAND ----------

# Quick-load data and display to explore
focal_layer = sedona.read.format("geoparquet").load(focal_path).withColumn("geometry", expr("ST_MakeValid(geometry)"))
focal_layer.createOrReplaceTempView("focal_layer")
focal_layer.display()

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Define naming and filter vaiables
# Set name for new columns and output assets
focal_name_dense = "nfi_dense"
focal_name_parse = "nfi_sparse" 

# COMMAND ----------

# Initial Filtering
focal_column = 'CATEGORY'
focal_variable = "Woodland"
focal_layer = focal_layer.filter(focal_layer[focal_column] == focal_variable)

# List options for Secondary filtering
focal_column = 'IFT_IOA'
focal_layer[focal_column].distinct().display()

# COMMAND ----------

# Split out secondary features into dense/sparse
focal_layer_dense = focal_layer.filter(focal_layer[focal_column].isin(["Mixed mainly broadleaved","Mixed mainly conifer","Conifer","Ground prep","Coppice","Young trees","Broadleaved","Assumed woodland","Coppice with standards","Felled","Windblow","Failed"]))

focal_layer_sparse = focal_layer.filter(focal_layer[focal_column].isin(["Low density","Shrub","Uncertain"]))

# COMMAND ----------

focal_layer_dense.createOrReplaceTempView("focal_layer_dense")
focal_layer_sparse.createOrReplaceTempView("focal_layer_sparse")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Processing

# COMMAND ----------

# Explode polygons
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_dense.geometry, 12) AS geometry FROM focal_layer_dense"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

# Find cells that intersect asset layer and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_dense, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_dense}.parquet"
)

# Drop duplicates
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_dense}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_dense, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_dense}.parquet"
)


# COMMAND ----------

# Explode polygons
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_sparse.geometry, 12) AS geometry FROM focal_layer_sparse"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

# Find cells that intersect asset layer and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_sparse, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_sparse}.parquet"
)

# Drop duplicates
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_sparse}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_sparse, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_S}.parquet"
)

