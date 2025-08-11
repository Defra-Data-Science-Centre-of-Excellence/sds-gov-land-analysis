# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect Natural England Living England (2022-23) with Data Model Grid (10m)
# MAGIC - This code iterates through all habitats and creates the relevant assets
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
import os

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

# DBTITLE 1,USER INPUTS
# location for input grid/centroids
in_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Model_Grids"
)

alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

# location for outputs
out_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/sds-assets/10m/LE"
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
# MAGIC ####Load in asset dataset

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Where to find the asset data
focal_path = "dbfs:/mnt/lab-res-a1001005/esd_project/miles.clement@defra.gov.uk/LE/LE2223.parquet"
focal_layer = sedona.read.format("geoparquet").load(focal_path)
focal_layer.createOrReplaceTempView("focal_layer")

# Define naming and filter vaiables
dataset_prefix = 'le'
focal_column = 'prmry_h'

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Create dictionary of habitat names and desired output names
asset_dict = {'Water':'water',
 'Built-up Areas and Gardens':'urban',
 'Coastal Sand Dunes':'sand_dunes',
 'Coastal Saltmarsh':'saltmarsh',
 'Solar Farms':'solar',
 'Dwarf Shrub Heath':'dwarf_shrub_heath',
 'Bare Sand':'bare_sand',
 'Unimproved Grassland':'unimproved_grass',
 'Bare Ground':'bare_ground',
 'Broadleaved, Mixed and Yew Woodland':'deciduous',
 'Coniferous Woodland':'coniferous',
 'Scrub':'scrub',
 'Arable and Horticultural':'arable',
 'Bog':'bog',
 'Fen, Marsh and Swamp':'fen_marsh_swamp',
 'Bracken':'bracken',
 'Improved and Semi-Improved Grassland':'improved grass'}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Processing

# COMMAND ----------

# Iterate through dictionary
for asset, outname in asset_dict.items(): 

  print(f"Processing: {asset}")

  # Set outname
  focal_name = f"{dataset_prefix}_{outname}"

  # Filter dataset for habitat
  asset_layer = focal_layer.filter(focal_layer[focal_column] == asset) 
  asset_layer.createOrReplaceTempView("asset_layer")

   # Explode polygons
  asset_layer_exploded = spark.sql(
      "SELECT ST_SubDivideExplode(asset_layer.geometry, 12) AS geometry FROM asset_layer"
  ).repartition(500)

  asset_layer_exploded.createOrReplaceTempView("asset_layer_exploded")

  # Find cells that intersect asset layer and assign them a 1
  out = spark.sql(
      "SELECT eng_combo_centroids.id FROM eng_combo_centroids, asset_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, asset_layer_exploded.geometry)"
  ).withColumn(focal_name, lit(1))

  out.write.format("parquet").mode("overwrite").save(
      f"{alt_out_path}/10m_x_{focal_name}.parquet"
  )

  # Drop duplicates
  out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name}.parquet").groupBy("id").count().drop("count").withColumn(focal_name, lit(1))

  out2.write.format("parquet").mode("overwrite").save(
      f"{alt_out_path}/10m_x_{focal_name}.parquet"
  )


# COMMAND ----------


