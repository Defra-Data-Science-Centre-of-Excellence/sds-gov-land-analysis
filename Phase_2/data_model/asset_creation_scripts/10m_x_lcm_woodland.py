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
    "/dbfs/mnt/lab/restricted/ESD-Project/Defra_Land/Assets"
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

# MAGIC %md
# MAGIC LCM Codes to keep - 1 (Deciduous Woodland), 2 (Coniferous Woodland)

# COMMAND ----------

import geopandas as gpd

# COMMAND ----------

lcm = gpd.read_file("/dbfs/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/LCM/LCM_Eng.shp")

# COMMAND ----------

lcm.to_parquet('/dbfs/mnt/lab/restricted/ESD-Project/Defra_Land/Layers/LCM_Eng.parquet')

# COMMAND ----------

focal_name_1 = "lcm_decid_wood"
focal_name_2 = "lcm_conif_wood"  # used to name the layer in outputs

focal_path = "/mnt/lab/restricted/ESD-Project/Defra_Land/Layers/LCM_Eng.parquet"

focal_column = 'f_mode'

focal_variable_1 = 1
focal_variable_2 = 2

# COMMAND ----------

focal_layer = sedona.read.format("geoparquet").load(focal_path)

focal_layer_1 = focal_layer.filter(focal_layer[focal_column] == focal_variable_1) 
focal_layer_2 = focal_layer.filter(focal_layer[focal_column] == focal_variable_2) 

focal_layer_1.createOrReplaceTempView("focal_layer_1")
focal_layer_2.createOrReplaceTempView("focal_layer_2")

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

# break them up
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_1.geometry, 12) AS geometry FROM focal_layer_1"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_1, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_1}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_1}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_1, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_1}.parquet"
)


# COMMAND ----------

# break them up
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_2.geometry, 12) AS geometry FROM focal_layer_2"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_2, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_2}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_2}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_2, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_2}.parquet"
)
