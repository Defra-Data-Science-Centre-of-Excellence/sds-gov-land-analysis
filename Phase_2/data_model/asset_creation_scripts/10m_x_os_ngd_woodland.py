# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect layers with grid
# MAGIC
# MAGIC Work out which 10 m grid squares intersect different layers (e.g. National Parks and AONBs)
# MAGIC
# MAGIC ### OS NGD Land Features - Woodland

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

ngd_land_path = 'dbfs:/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_land_features/format_GEOPARQUET_ngd_land_features/LATEST_ngd_land_features/*.parquet'

# COMMAND ----------

ngd_land = spark.read.format("geoparquet").load(ngd_land_path)

# COMMAND ----------

ngd_land.display()

# COMMAND ----------

unique_values = ngd_land.select("description").distinct().collect()

unique_values


# oslandcovertierb = 'Heath,Rock,Rough Grassland,Scattered Coniferous Trees,Scattered Non-Coniferous Trees', 'Heath,Rock,Rough Grassland,Scattered Non-Coniferous Trees', 'Heath,Rough Grassland,Scattered Coniferous Trees,Scattered Non-Coniferous Trees', 

# COMMAND ----------

ngd_land_trees = ngd_land.filter(ngd_land["oslandcovertiera"] == "Trees")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

ngd_land_trees_dense = ngd_land_trees.filter(
    F.col("oslandcovertierb").startswith("Coniferous Trees") |
    F.col("oslandcovertierb").startswith("Non-Coniferous Trees"))

# COMMAND ----------

ngd_land_trees_sparse = ngd_land_trees.filter(~(
    F.col("oslandcovertierb").startswith("Coniferous Trees") |
    F.col("oslandcovertierb").startswith("Non-Coniferous Trees")))

# COMMAND ----------

focal_name_dense = "os_ngd_land_trees_dense"
focal_name_sparse = "os_ngd_land_trees_sparse" # used to name the layer in outputs

# COMMAND ----------

ngd_land_trees_dense.createOrReplaceTempView("focal_layer_D")
ngd_land_trees_sparse.createOrReplaceTempView("focal_layer_S")

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
    "SELECT ST_SubDivideExplode(focal_layer_D.geometry, 12) AS geometry FROM focal_layer_D"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_dense, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_dense}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_dense}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_dense, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_dense}.parquet"
)


# COMMAND ----------

# break them up
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_S.geometry, 12) AS geometry FROM focal_layer_S"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_sparse, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_sparse}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_sparse}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_sparse, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_sparse}.parquet"
)
