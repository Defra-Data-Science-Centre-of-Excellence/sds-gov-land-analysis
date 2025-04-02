# Databricks notebook source
# MAGIC %md
# MAGIC ### Rasterisation - SUM Version - Urban
# MAGIC Creates a 10m resolution raster from centroids data. Raster values represents the sum of selected columns. 
# MAGIC
# MAGIC Miles Clement (miles.clement@defra.gov.uk)
# MAGIC
# MAGIC Last Updated 02/04/25

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC ####Packages

# COMMAND ----------

from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from affine import Affine

# COMMAND ----------

from sedona.spark import *

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

username = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Functions

# COMMAND ----------

import rasterio
from rasterio.features import rasterize
import gc

# function to rasterise and return new dataset
def rasterise_points_initial(
    gdf, shape, transform, crs, out_fname, background=0, dtype=rasterio.uint8
):

    """Creates a rasterised dataset using the geometries provided

    Parameters
    gdf : GeoDataFrame
        Geopandas GeoDataFrame containing the geometries to be burned
    shape : (int, int)
        Rasterio DatasetReader pointing to a dataset (LCM) that will provide the definition of the output
    transform : Affine
        Rasterio Affine object containing transformation
    crs : crs
        crs representation e.g. epsg string
    out_fname : str
        Filename to save the raster output to
    background : uint8
        Value to give to the background data e.g. the sea - not necessarily the same as nodata
    dtype : datatype
        Datatype of the output raster should be uint8

    Returns
    str
        Filename of raster output file created
    """

    # Prepare (geometry, raster_score) tuples
    # This part is different from boolean example
    shapes_with_values = zip(gdf.geometry, gdf["raster_score"])

    # Rasterize using the provided values
    data_array = rasterize(
        shapes=shapes_with_values,  # Use tuples of (geometry, value)
        nodata=None,
        out_shape=shape,
        fill=background,
        transform=transform,
        all_touched=True,
        dtype=dtype,
    )

    with rasterio.open(
        fp=out_fname,
        mode="w",
        driver="GTiff",
        height=shape[0],
        width=shape[1],
        count=1,
        dtype=dtype,
        crs=crs,
        transform=transform,
        tiled=True,
        blockxsize=256,
        blockysize=256,
        compress="LZW",
    ) as dataset:
        dataset.write(data_array, 1)

    del data_array
    del dataset
    gc.collect()
    return out_fname

# COMMAND ----------

# function to rasterise and update existing dataset
def rasterise_points_update(
    gdf, shape, transform, crs, out_fname, background=0, dtype=rasterio.uint8
):

    """Creates a rasterised dataset using the geometries provided

    Parameters
    gdf : GeoDataFrame
        Geopandas GeoDataFrame containing the geometries to be burned
    shape : (int, int)
        Rasterio DatasetReader pointing to a dataset (LCM) that will provide the definition of the output
    transform : Affine
        Rasterio Affine object containing transformation
    crs : crs
        crs representation e.g. epsg string
    out_fname : str
        Filename to save the raster output to
    background : uint8
        Value to give to the background data e.g. the sea - not necessarily the same as nodata
    dtype : datatype
        Datatype of the output raster should be uint8

    Returns
    str
        Filename of raster output file created
    """
    # Prepare (geometry, raster_score) tuples
    shapes_with_values = zip(gdf.geometry, gdf["raster_score"])

    # Rasterize using the provided values
    data_array = rasterize(
        shapes=shapes_with_values,  # Use tuples of (geometry, value)
        nodata=None,
        out_shape=shape,
        fill=background,
        transform=transform,
        all_touched=True,
        dtype=dtype,
    )

    with rasterio.open(fp=out_fname, mode="r+") as dataset:
        existing_data = dataset.read(1)
        updated_data = existing_data.copy()

        # Update only where data_array has values
        updated_data[data_array > 0] = data_array[data_array > 0]
        dataset.write(updated_data, 1)

    del data_array
    del updated_data
    del existing_data
    del dataset
    gc.collect()
    return out_fname

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Paths

# COMMAND ----------

# location for outputs
par_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Final/Asset_Tables"
)

alt_par_path = str(par_path).replace("/dbfs", "dbfs:")

data_combined = sedona.read.format("parquet").load(
    f"{alt_par_path}/10m_x_assets_combined_urban.parquet"
)

# COMMAND ----------

# Set condition to find rows overlapping DGL (FH or LH)
condition = ((F.col("dgl_fh") == 1) | (F.col("dgl_lh") == 1)) 

# COMMAND ----------

# DBTITLE 1,Quick print to check cols
data_combined.display()

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Define which columns to combine into raster cell
columns_to_sum = ["le_urban","lcm_comb","ons_urban"]


# COMMAND ----------

# MAGIC %md
# MAGIC -----------------

# COMMAND ----------

# Add a new column with the sum of the values across the specified columns
data_combined_score = data_combined.withColumn(
    "raster_score",
    F.when(
        condition,
        sum(F.coalesce(F.col(col), F.lit(0)) for col in columns_to_sum)  # Replace nulls with 0 before summing
    ).otherwise(None)  # Optionally handle rows not meeting the condition
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Constants

# COMMAND ----------

# the names of the 100km grid cells covering the bounds of England (some won't contain data)
os_grid_codes = [
    "SV",
    "SW",
    "SX",
    "SY",
    "SZ",
    "TV",
    "TW",
    "TR",
    "TQ",
    "SU",
    "ST",
    "SS",
    "SR",
    "SQ",
    "SL",
    "SM",
    "SN",
    "SO",
    "SP",
    "TL",
    "TM",
    "TG",
    "TF",
    "SK",
    "SJ",
    "SH",
    "SG",
    "SF",
    "SA",
    "SB",
    "SC",
    "SD",
    "SE",
    "TA",
    "TB",
    "OW",
    "OV",
    "NZ",
    "NY",
    "NX",
    "NW",
    "NV",
    "NQ",
    "NR",
    "NS",
    "NT",
    "NU",
    "OQ",
    "OR",
]

# Define the affine transform parameters
pixel_width = 10  # The pixel width (in map units per pixel)
pixel_height = (
    -10
)  # The pixel height (in map units per pixel, negative because y values increase downward)
top_left_x = 0  # The x-coordinate of the top-left corner of the raster (in map units)
top_left_y = (
    700000  # The y-coordinate of the top-left corner of the raster (in map units)
)

# Create the affine transform object
transform = Affine(pixel_width, 0, top_left_x, 0, pixel_height, top_left_y)

# shape of the raster
shape = (70000, 70000)
# out file path - has to be in `tmp` directory
tif_file = "/tmp/dgl_urban_sum.tif"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Processing

# COMMAND ----------

from pyspark.sql.functions import col

# the column with the identifier
column_name = "raster_score"

# track the first iteration to apply the appropriate function
first_iteration = True

# loop through all 100km OS grid codes, extract relevant data and rasterise
for grid_code in os_grid_codes:
    print(grid_code)
    sub_comb = data_combined_score.filter(col("id").contains(grid_code))
    sub_comb.createOrReplaceTempView("sub_comb")
    sub_comb = spark.sql(f"SELECT * FROM sub_comb WHERE {column_name} > 0")

    # check if any points with a 1 have been extracted
    if len(sub_comb.head(1)) == 0:
        print(f"Skipping tile {grid_code} - no data present")

    else:
        # Slight change to sub_comb too
        sub_comb = sub_comb.select("geometry", "raster_score").toPandas()
        if first_iteration:
            print("Running first iteration")
            # use function to create the raster and insert rasterised values
            rasterise_points_initial(sub_comb, shape, transform, "epsg:27700", tif_file)
            first_iteration = False
        else:
            print("Running other iteration")
            # use function to updated existing raster with rasterised values
            rasterise_points_update(sub_comb, shape, transform, "epsg:27700", tif_file)

# COMMAND ----------

from sds_dash_download import download_file

displayHTML(download_file(tif_file, move=False))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------
# MAGIC
