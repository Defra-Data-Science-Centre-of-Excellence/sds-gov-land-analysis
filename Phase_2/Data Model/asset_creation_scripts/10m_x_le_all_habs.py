# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect layers with grid
# MAGIC
# MAGIC Work out which 10 m grid squares intersect different layers (e.g. National Parks and AONBs)
# MAGIC
# MAGIC ###Master Version 
# MAGIC
# MAGIC Clone and update to run analysis

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %pip install keplergl pydeck mapclassify rtree pygeos geopandas==1.0.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from sedona.spark import *

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

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
out_path = Path(
    "/dbfs/mnt/lab/restricted/ESD-Project/sds-assets/10m"
)

alt_out_path = str(out_path).replace("/dbfs", "dbfs:")

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
# MAGIC ### Update Cell Below

# COMMAND ----------

focal_path = "dbfs:/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/LE/LE2223.parquet"

dataset_prefix = 'le'

focal_column = 'prmry_h'

focal_layer = sedona.read.format("geoparquet").load(focal_path)

# COMMAND ----------

asset_list = focal_layer.select("Prmry_H").distinct().rdd.flatMap(lambda x: x).collect()
asset_list = [x for x in asset_list if x is not None]

# COMMAND ----------

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

from sedona.sql import st_constructors as cn
from pyspark.sql.functions import lit, expr, col, like, sum
from sedona.sql import st_functions as fn
from geopandas import read_file

# COMMAND ----------

for asset, outname in asset_dict.items(): 

  print(f"Processing: {asset}")

  asset_layer = focal_layer.filter(focal_layer[focal_column] == asset) 

  focal_name = f"{dataset_prefix}_{outname}"

  asset_layer.createOrReplaceTempView("asset_layer")

  # break them up
  asset_layer_exploded = spark.sql(
      "SELECT ST_SubDivideExplode(asset_layer.geometry, 12) AS geometry FROM asset_layer"
  ).repartition(500)

  asset_layer_exploded.createOrReplaceTempView("asset_layer_exploded")

  #find cells that intersect and assign them a 1
  out = spark.sql(
      "SELECT eng_combo_centroids.id FROM eng_combo_centroids, asset_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, asset_layer_exploded.geometry)"
  ).withColumn(focal_name, lit(1))

  out.write.format("parquet").mode("overwrite").save(
      f"{alt_out_path}/10m_x_{focal_name}.parquet"
  )

  # current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
  out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name}.parquet").groupBy("id").count().drop("count").withColumn(focal_name, lit(1))

  out2.write.format("parquet").mode("overwrite").save(
      f"{alt_out_path}/10m_x_{focal_name}.parquet"
  )


# COMMAND ----------


