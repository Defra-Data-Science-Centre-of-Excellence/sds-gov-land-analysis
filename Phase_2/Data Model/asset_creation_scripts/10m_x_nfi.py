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
# MAGIC ####Planned Assets for DGL: Woodland
# MAGIC - LE Woodland
# MAGIC - NFI
# MAGIC - PHI
# MAGIC - LCM Woodland

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %pip install keplergl pydeck mapclassify rtree pygeos geopandas==1.0.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from sedona.spark import *
from pyspark.sql.functions import expr

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

focal_name_D = "nfi_dense"
focal_name_S = "nfi_sparse" # used to name the layer in outputs
focal_path = "dbfs:/mnt/base/unrestricted/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england_2022/format_GEOPARQUET_national_forest_inventory_woodland_england_2022/LATEST_national_forest_inventory_woodland_england_2022/National_Forest_Inventory_England_2022.parquet"
focal_column = 'CATEGORY'
focal_variable = "Woodland"

# COMMAND ----------

focal_layer = sedona.read.format("geoparquet").load(focal_path).withColumn("geometry", expr("ST_MakeValid(geometry)"))
focal_layer = focal_layer.filter(focal_layer[focal_column] == focal_variable)
focal_layer.select("IFT_IOA").distinct().display()

# COMMAND ----------

focal_layer_dense = focal_layer.filter(focal_layer["IFT_IOA"].isin(["Mixed mainly broadleaved","Mixed mainly conifer","Conifer","Ground prep","Coppice","Young trees","Broadleaved","Assumed woodland","Coppice with standards","Felled","Windblow","Failed"]))

focal_layer_sparse = focal_layer.filter(focal_layer["IFT_IOA"].isin(["Low density","Shrub","Uncertain"]))

# COMMAND ----------

focal_layer_dense.createOrReplaceTempView("focal_layer_dense")
focal_layer_sparse.createOrReplaceTempView("focal_layer_sparse")

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
    "SELECT ST_SubDivideExplode(focal_layer_dense.geometry, 12) AS geometry FROM focal_layer_dense"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_D, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_D}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_D}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_D, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_D}.parquet"
)


# COMMAND ----------

# break them up
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_sparse.geometry, 12) AS geometry FROM focal_layer_sparse"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_S, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_S}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_S}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_S, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_S}.parquet"
)

