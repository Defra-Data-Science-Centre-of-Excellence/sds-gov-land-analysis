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

import pandas
import geopandas
import rasterio

# COMMAND ----------

# Define size of grid square
grid_square_size = 10

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/mnt/lab-res-a1001005/esd_project/sds-assets/10m/LCM')

# COMMAND ----------

from pathlib import Path

# location for input grid/centroids
in_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Model_Grids"
)

alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

# location for outputs
out_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/sds-assets/10m/LCM"
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

import os

parquet_directory = "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/LCM23"
lcm_par = [os.path.join(parquet_directory, file) for file in os.listdir(parquet_directory) if file.endswith('.parquet')]
lcm_par

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Cell Below

# COMMAND ----------

from sedona.sql import st_constructors as cn
from pyspark.sql.functions import lit, expr, col, like, sum
from sedona.sql import st_functions as fn
from geopandas import read_file

# COMMAND ----------

for par in lcm_par:

    focal_layer = sedona.read.format("geoparquet").load(par[5:])

    outname = par.split('/')[-1]  
    outname = outname.split('LCM23_')[1]   
    outname = outname.rsplit('.', 1)[0] 

    focal_name = f"lcm_{outname}"

    print(focal_name)

    focal_layer.createOrReplaceTempView("asset_layer")

    print('Explode')

    asset_layer_exploded = spark.sql(
        "SELECT ST_SubDivideExplode(asset_layer.geometry, 12) AS geometry FROM asset_layer"
    ).repartition(500)

    asset_layer_exploded.createOrReplaceTempView("asset_layer_exploded")

    print('Intersect')

    out = spark.sql(
        "SELECT eng_combo_centroids.id FROM eng_combo_centroids, asset_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, asset_layer_exploded.geometry)"
    ).withColumn(focal_name, lit(1))

    out.write.format("parquet").mode("overwrite").save(
        f"{alt_out_path}/10m_x_{focal_name}.parquet"
    )

    print('Drop')

    out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name}.parquet").groupBy("id").count().drop("count").withColumn(focal_name, lit(1))

    out2.write.format("parquet").mode("overwrite").save(
        f"{alt_out_path}/10m_x_{focal_name}.parquet"
    )


# COMMAND ----------

out2.display()
