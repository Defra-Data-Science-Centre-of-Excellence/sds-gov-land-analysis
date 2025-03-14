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
# MAGIC ###Load in Conversion Data

# COMMAND ----------

import pandas as pd

# COMMAND ----------

codes = pd.read_csv('/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_land_cover_codes/format_CSV_rpa_land_cover_codes/LATEST_rpa_land_cover_codes/LOOKUP_LANDCOVERS_Asset_Mapping.csv')

# COMMAND ----------

codes.display()

# COMMAND ----------

code_list_dense = [230, 284, 285, 286, 323, 611, 612, 613, 614, 615, 651]
code_list_sparse = [130, 335, 588, 592, 593]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Cell Below

# COMMAND ----------

focal_name_d = "rpa_lc_woodland_dense"
focal_name_s = "rpa_lc_woodland_sparse"  

focal_path = "dbfs:/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_land_cover/format_GEOPARQUET_rpa_land_cover/LATEST_rpa_land_cover/land_cover.parquet"

focal_column = 'LAND_COVER_CLASS_CODE'

# COMMAND ----------

focal_layer = sedona.read.format("geoparquet").load(focal_path).withColumn("GEOM", expr("ST_MakeValid(GEOM)"))

focal_layer_d = focal_layer.filter(focal_layer[focal_column].isin(code_list_dense)) # Optional Filter statement
focal_layer_d.createOrReplaceTempView("focal_layer_d")

focal_layer_s = focal_layer.filter(focal_layer[focal_column].isin(code_list_sparse)) # Optional Filter statement
focal_layer_s.createOrReplaceTempView("focal_layer_s")

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
    "SELECT ST_SubDivideExplode(focal_layer_d.GEOM, 12) AS geometry FROM focal_layer_d"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_d, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_d}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_d}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_d, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_d}.parquet"
)


# COMMAND ----------

# break them up
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer_s.GEOM, 12) AS geometry FROM focal_layer_s"
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
out = spark.sql(
    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
).withColumn(focal_name_s, lit(1))

out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_s}.parquet"
)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_s}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_s, lit(1))

out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name_s}.parquet"
)
