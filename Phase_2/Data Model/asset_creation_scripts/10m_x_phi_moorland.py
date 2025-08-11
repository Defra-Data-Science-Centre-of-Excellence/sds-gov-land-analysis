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

import geopandas as gpd
import pandas as pd

# COMMAND ----------

filepath = "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"

phi_north = gpd.read_file(filepath+"dataset_priority_habitat_inventory_north/format_SHP_priority_habitat_inventory_north/LATEST_priority_habitat_inventory_north/PHI_v2_3_North.shp")
phi_central = gpd.read_file(filepath+"dataset_priority_habitat_inventory_central/format_SHP_priority_habitat_inventory_central/LATEST_priority_habitat_inventory_central/PHI_v2_3_Central.shp")
phi_south = gpd.read_file(filepath+"dataset_priority_habitat_inventory_south/format_SHP_priority_habitat_inventory_south/LATEST_priority_habitat_inventory_south/PHI_v2_3_South.shp")

phi_comb = pd.concat([phi_north, phi_central, phi_south], ignore_index=True)

# COMMAND ----------

phi_comb.to_parquet('/dbfs/mnt/lab/restricted/ESD-Project/Defra_Land/Layers/phi_comb.parquet')

# COMMAND ----------

phi_north['Main_Habit'].unique()    

# COMMAND ----------

focal_name_1 = "phi_blanket_bog"
focal_name_2 = "phi_upland_flushes" # used to name the layer in outputs
#focal_name_3 = "phi_mountain_scrub"

focal_path = "dbfs:/mnt/lab/restricted/ESD-Project/Defra_Land/Layers/phi_comb.parquet"
focal_column = 'Main_Habit'

focal_variable_1 = 'Blanket bog'
focal_variable_2 = 'Upland flushes, fens and swamps'
#focal_variable_3 = 'Mountain heaths and willow scrub'

# COMMAND ----------

focal_layer = sedona.read.format("geoparquet").load(focal_path)

focal_layer_1 = focal_layer.filter(focal_layer[focal_column].isin(focal_variable_1)) # Optional Filter statement
focal_layer_2 = focal_layer.filter(focal_layer[focal_column].isin(focal_variable_2))
#focal_layer_3 = focal_layer.filter(focal_layer[focal_column].isin(focal_variable_3))

focal_layer_1.createOrReplaceTempView("focal_layer_1")
focal_layer_2.createOrReplaceTempView("focal_layer_2")
#focal_layer_3.createOrReplaceTempView("focal_layer_3")

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

# COMMAND ----------

# break them up
#focal_layer_exploded = spark.sql(
#    "SELECT ST_SubDivideExplode(focal_layer_3.geometry, 12) AS geometry FROM focal_layer_3"
#).repartition(500)

#focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

#find cells that intersect and assign them a 1
#out = spark.sql(
#    "SELECT eng_combo_centroids.id FROM eng_combo_centroids, focal_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)"
#).withColumn(focal_name_3, lit(1))

#out.write.format("parquet").mode("overwrite").save(
#    f"{alt_out_path}/10m_x_{focal_name_3}.parquet"
#)

# current work around to get rid of duplicates quickly - distinct/dropDuplicates is slow in both pyspark and SQL
#out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name_3}.parquet").groupBy("id").count().drop("count").withColumn(focal_name_3, lit(1))

#out2.write.format("parquet").mode("overwrite").save(
#    f"{alt_out_path}/10m_x_{focal_name_3}.parquet"
#)
