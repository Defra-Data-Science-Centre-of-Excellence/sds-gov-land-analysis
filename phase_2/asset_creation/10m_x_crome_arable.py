# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect RPA Crop Map of England (2022) with Data Model Grid (10m)
# MAGIC
# MAGIC Miles Clement (miles.clement@defra.gov.uk)
# MAGIC
# MAGIC Last Updated 17/03/25
# MAGIC
# MAGIC
# MAGIC

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

# DBTITLE 1,USER INPUT
# Define size of grid square
grid_square_size = 10

# COMMAND ----------

# DBTITLE 1,USER INPUT
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

# DBTITLE 1,USER INPUT
# Where to find the asset data
focal_path = "dbfs:/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_crome_2022/format_GEOPARQUET_crome_2022/LATEST_crome_2022/crome_2022.parquet"

# COMMAND ----------

# Quick-load data and display to explore
focal_layer = sedona.read.format("geoparquet").load(focal_path)
focal_layer.createOrReplaceTempView("focal_layer")
focal_layer.display()

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Define naming and filter vaiables
# Set name for new columns and output assets
focal_name = "crome"

# What attribute is the data filtered using?
focal_column = 'lucode'

# Define attribute values 
# Below are the non-arable classifications in CROME
focal_variable = ['SR01','FA01','HE02','HEAT','PG01','NA01','WA00','TC01','NU01','WO12','AC00']

# Filter to EXCLUDE focal_variables in focal_column
focal_layer = focal_layer.filter(~focal_layer[focal_column].isin(focal_variable))

focal_layer.createOrReplaceTempView("focal_layer")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Processing

# COMMAND ----------

# Explode polygons
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer.GEOM, 12) AS geometry FROM focal_layer"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

# Find cells that intersect asset layer and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name}.parquet"
)

# Drop duplicates
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name}.parquet").groupBy("id").count().drop("count").withColumn(focal_name, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name}.parquet"
)

