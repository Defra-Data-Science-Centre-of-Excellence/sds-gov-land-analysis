# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect UKCEH Land Cover Map (2023) with Data Model Grid (10m)
# MAGIC - This code iterates through all habitats and creates the relevant assets
# MAGIC - Data has been pre-processed locally (converted from raster to vector) into separate habitat parquet files to ensure latest version of dataset is used
# MAGIC
# MAGIC Miles Clement (miles.clement@defra.gov.uk)
# MAGIC
# MAGIC Last Updated 17/03/25
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

# DBTITLE 1,USER INPUTS
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
    "/dbfs/mnt/lab-res-a1001005/esd_project/sds-assets/10m/LCM"
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
# MAGIC ### Get list of habitat parquets from data lake

# COMMAND ----------

# DBTITLE 1,USER INPUT
parquet_directory = "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/LCM23"

lcm_par = [os.path.join(parquet_directory, file) for file in os.listdir(parquet_directory) if file.endswith('.parquet')]
lcm_par

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing

# COMMAND ----------

# Iterate through parquets
for par in lcm_par:

    focal_layer = sedona.read.format("geoparquet").load(par[5:])

    # Extract habitat from filepath
    outname = par.split('/')[-1]  
    outname = outname.split('LCM23_')[1]   
    outname = outname.rsplit('.', 1)[0] 

    # Create name for column & output asset
    focal_name = f"lcm_{outname}"

    print(focal_name)

    focal_layer.createOrReplaceTempView("asset_layer")

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

