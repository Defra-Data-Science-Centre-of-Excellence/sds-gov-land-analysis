# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect Defra Group Land HM Land Registry Polygons (split by Organisation) with Data Model Grid (10m)
# MAGIC
# MAGIC - The code for creating this asset is slightly different, as it assigns the organisation string to each grid centroid rather than creating a boolean asset catalogue.
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

# COMMAND ----------

from sedona.spark import *
from sedona.sql import st_constructors as cn
from pyspark.sql.functions import lit, expr, col, like, sum, when, concat
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

# DBTITLE 1,USER INPUT
# location for input grid/centroids
in_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Model_Grids"
)

alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

# location for outputs
out_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Assets"
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
# MAGIC ####Load in and explore asset dataset

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Where to find the asset data
focal_path = "dbfs:/mnt/lab-res-a1001005/esd_project/jasmine.elliott@defra.gov.uk/gov_land_analysis/phase_one_final_report_outputs/polygon_ccod_defra_by_organisation_tenure.parquet"

# COMMAND ----------

# Quick-load data and display to explore
focal_layer = sedona.read.format("geoparquet").load(focal_path)
focal_layer.createOrReplaceTempView("focal_layer")
focal_layer.display()

# COMMAND ----------

# Create new column combining 'current_organisation' and 'land_management_organisation' columns
focal_layer = focal_layer.withColumn(
    "organisation",
    when(
        col("land_management_organisation").isNull(),
        col("current_organisation")
    ).otherwise(
        concat(
            col("current_organisation"),
            lit(" ("),
            col("land_management_organisation"),
            lit(")")
        )
    )
)

focal_layer.createOrReplaceTempView("focal_layer")

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Define naming and filter vaiables
# Set name for new columns and output assets
focal_name = "dgl_organisation"

# What attribute is the data filtered using?
focal_column = 'organisation'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Processing

# COMMAND ----------

# Explode the focal layer geometries
# Select both the subdivided geometry and the specified focal_column for further processing.
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer.geometry, 12) AS geometry, focal_layer.{focal_column} AS {focal_column} FROM focal_layer".format(focal_column=focal_column)
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

# Find cells that intersect and extract the organisation value from the focal column
out = spark.sql(
    f"""
    SELECT eng_combo_centroids.id, focal_layer_exploded.{focal_column}
    FROM eng_combo_centroids, focal_layer_exploded
    WHERE ST_INTERSECTS(eng_combo_centroids.geometry, focal_layer_exploded.geometry)
    """
)

# Save intermediate results
out.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name}.parquet"
)

# Remove duplicates by keeping distinct `id` and focal_column combinations
out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name}.parquet").dropDuplicates(["id", focal_column])

# Save the final results
out2.write.format("parquet").mode("overwrite").save(
    f"{alt_out_path}/10m_x_{focal_name}.parquet"
)

