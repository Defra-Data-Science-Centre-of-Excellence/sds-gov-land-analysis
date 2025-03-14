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
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Model_Grids"
)

alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

# location for outputs
out_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Assets"
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

focal_path = "dbfs:/mnt/lab-res-a1001005/esd_project/jasmine.elliott@defra.gov.uk/gov_land_analysis/phase_one_final_report_outputs/polygon_ccod_defra_by_organisation_tenure.parquet"

# COMMAND ----------

focal_layer = sedona.read.format("geoparquet").load(focal_path)
focal_layer.createOrReplaceTempView("focal_layer")

# COMMAND ----------

from pyspark.sql.functions import when, concat, lit, col

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

# COMMAND ----------

focal_layer.createOrReplaceTempView("focal_layer")
focal_name = "dgl_organisation"
focal_column = 'organisation'

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

# Explode the focal layer geometries
focal_layer_exploded = spark.sql(
    "SELECT ST_SubDivideExplode(focal_layer.geometry, 12) AS geometry, focal_layer.{focal_column} AS {focal_column} FROM focal_layer".format(focal_column=focal_column)
).repartition(500)

focal_layer_exploded.createOrReplaceTempView("focal_layer_exploded")

# Find cells that intersect and include the value from the focal column
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


# COMMAND ----------

check = sedona.read.format("parquet").load('dbfs:/mnt/lab-res-a1001005/esd_project/Defra_Land/Assets/10m_x_dgl_organisation.parquet')

# COMMAND ----------

check.display()

# COMMAND ----------

unique_values = check.select("organisation").dropDuplicates()
unique_values.display()

# COMMAND ----------


