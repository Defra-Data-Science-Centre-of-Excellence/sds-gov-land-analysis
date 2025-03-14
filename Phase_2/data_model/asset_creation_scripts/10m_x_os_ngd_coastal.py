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

unique_values = ngd_land.select("oslandcovertierb").distinct().collect()

unique_values


# COMMAND ----------

from pyspark.sql.functions import split, col

# Extract the first habitat by splitting the string at the first comma
unique_values_first_habitat = ngd_land \
    .select(split(col("oslandcovertierb"), ",")[0].alias("first_habitat")) \
    .distinct() \
    .collect()

# Show the unique values
for row in unique_values_first_habitat:
    print(row.first_habitat)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

ngd_land_coastal = ngd_land.filter(
    F.col("oslandcovertierb").startswith("Saltmarsh") |
    F.col("oslandcovertierb").startswith("Shingle") |
    F.col("oslandcovertierb").startswith("Sand") |
    F.col("oslandcovertierb").startswith("Inter Tidal"))

# COMMAND ----------

ngd_land_sm = ngd_land_coastal.filter(
    F.col("oslandcovertierb").startswith("saltmarsh"))

# COMMAND ----------

focal_name_coastal = "os_ngd_land_coastal"
focal_name_sm = "os_ngd_land_saltmarsh" # used to name the layer in outputs

# COMMAND ----------

ngd_land_coastal.createOrReplaceTempView("focal_layer_C")
ngd_land_sm.createOrReplaceTempView("focal_layer_SM")

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
    "SELECT ST_SubDivideExplode(focal_layer_C.geometry, 12) AS geometry FROM focal_layer_C"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_coastal, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_coastal}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_coastal}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_coastal, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_coastal}.parquet"
)


# COMMAND ----------

# break them up
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_SM.geometry, 12) AS geometry FROM focal_layer_SM"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_sm, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_sm}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_sm}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_sm, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_sm}.parquet"
)
