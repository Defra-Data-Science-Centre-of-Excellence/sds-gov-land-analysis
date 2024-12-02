# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create spatial grid and associated centroids - England
# MAGIC
# MAGIC This script creates a standard grid and set of centroids across England. 
# MAGIC
# MAGIC To do this, is uses the standard OS grids (e.g. at 10 km) resolution to create finer reoslution grids in chunks.
# MAGIC
# MAGIC The individual chunk parquets are then combined, their centroids created, then clipped to the England boundary. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Setup

# COMMAND ----------

from sedona.spark import *

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

# COMMAND ----------

username = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

# COMMAND ----------

# Define size of grid square
grid_square_size = 10

# COMMAND ----------

from pathlib import Path

# path to polygon of England
england_path = "/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_countries_bdy_uk_bfe/format_GEOPARQUET_countries_bdy_uk_bfe/LATEST_countries_bdy_uk_bfe/countries_bdy_uk_bfe.parquet"

# grab big grid(s) from GitHub - https://github.com/OrdnanceSurvey/OS-British-National-Grids (may have to change directory if you want them somewhere else)
os_grid_path = f"/Workspace/Users/{username}/30x30/os_bng_grids.gpkg"

# path to store outputs
out_path = Path(
    f"/dbfs/mnt/lab/restricted/ESD-Project/{username}/30x30/grids/"
)

alt_out_path = str(out_path).replace("/dbfs", "dbfs:")

if not out_path.exists():
    out_path.mkdir(parents=True, exist_ok=True)

# path to store individual grid parquets
sub_out_path = Path(
    f"{out_path}/{grid_square_size}m_grids"
)

alt_sub_out_path = str(sub_out_path).replace("/dbfs", "dbfs:")

if not sub_out_path.exists():
    sub_out_path.mkdir(parents=True, exist_ok=True)


# COMMAND ----------

from geopandas import read_parquet, GeoDataFrame

england_bound = read_parquet(england_path).query("CTRY23NM == 'England'")
england_bound = GeoDataFrame(england_bound.loc[:, "geometry"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create grids

# COMMAND ----------

from geopandas import read_file, sjoin
from shapely.geometry import box

# read in 10km OS grid and select those that intersect terrestrial England
big_grid = read_file(
    os_grid_path,
    engine="pyogrio",
    layer="10km_grid",
)

eng_bbox = GeoDataFrame(
    {
        "geometry": [
            box(
                england_bound.total_bounds[0],
                england_bound.total_bounds[1],
                england_bound.total_bounds[2],
                england_bound.total_bounds[3],
            )
        ]
    },
    crs=27700,
)

# select tiles that intersect England's bounding box
sub_big_grid = sjoin(big_grid, eng_bbox, predicate="intersects", how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Loop through each 'big tile' and create a set of small user defined size tiles and write out as geoparquet

# COMMAND ----------

from pandas import DataFrame
from geopandas import GeoDataFrame, points_from_xy, sjoin
from itertools import product
from pathlib import Path

# loop through each 10km box and make small boxes, saving each as a geoparquet
for index, row in sub_big_grid.iterrows():

    tile_id = row["tile_name"]
    print(tile_id)

    bounds = row.geometry.bounds

    # work out centroids on both axes
    x_centroids = list(
        range(
            int(int(bounds[0]) + grid_square_size / 2), int(bounds[2]), grid_square_size
        )
    )
    y_centroids = list(
        range(
            int(int(bounds[1]) + grid_square_size / 2), int(bounds[3]), grid_square_size
        )
    )
    xy_centroids = list(product(x_centroids, y_centroids))

    # create geodataframe
    df = DataFrame(xy_centroids, columns=["x", "y"])
    gdf = GeoDataFrame(
        df,
        geometry=points_from_xy(df["x"], df["y"]),
        crs="EPSG:27700",
    )

    # create polygons dataframe of size grid_square_size (e.g. 5000 x 5000 m)
    boxes = (
        GeoDataFrame(gdf["geometry"].buffer((grid_square_size / 2), cap_style=3))
        .assign(id=range(len(gdf)))
        .rename(columns={0: "geometry"})
        .set_geometry("geometry")
    )

    # make ID unique (incoporating OS grid ID)
    boxes["id"] = tile_id + "_" + boxes["id"].astype(str)

    boxes.to_parquet(f"{sub_out_path}/{tile_id}.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Combine sub tile parquet files into single parquet and write out

# COMMAND ----------

# read in the newly create grid data - use mergeschema to read in all parquets from the directory
combo = sedona.read.format("geoparquet").load(
    alt_sub_out_path, mergeSchema=True
)

# write out a copy of combined grids - one parquet covering all sub big grid squares
combo.write.format("geoparquet").mode("overwrite").save(
    f"{alt_out_path}/{grid_square_size}m_full_grid.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Point Dataset 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC First create a centroid dataset for all grid cells. Not clipped to England. Write out.

# COMMAND ----------

combo = sedona.read.format("geoparquet").load(f"{alt_out_path}/{grid_square_size}m_full_grid.parquet")

combo.createOrReplaceTempView("combo")

# get centroids
combo_centroids = spark.sql(
    "SELECT combo.id, st_centroid(combo.geometry) AS geometry FROM combo"
)
combo_centroids.write.format("geoparquet").mode("overwrite").save(
    f"{alt_out_path}/{grid_square_size}m_full_grid_centroids.parquet"
)

# read back in
combo_centroids = (
    sedona.read.format("geoparquet")
    .load(f"{alt_out_path}/{grid_square_size}m_full_grid_centroids.parquet")
    .repartition(2000)
)
combo_centroids.createOrReplaceTempView("combo_centroids")

# COMMAND ----------

# read in england bound with spark
england_bound = (
    sedona.read.format("geoparquet")
    .load(str(england_path).replace("/dbfs", "dbfs:"))
    .filter("CTRY23NM == 'England'")
)

combo.createOrReplaceTempView("combo")
england_bound.createOrReplaceTempView("england_bound")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Sub-divide explode England to make intersecting with the grid tiles easier

# COMMAND ----------

england_bound_sub_divided = spark.sql(
    "SELECT ST_SubDivideExplode(england_bound.geometry, 12) AS geometry FROM england_bound"
)
england_bound_sub_divided.write.format("geoparquet").mode("overwrite").save(
    f"{alt_out_path}/england_bound_sub_divided.parquet"
)

# COMMAND ----------

# read back in
england_bound_sub_divided = sedona.read.format("geoparquet").load(
    f"{alt_out_path}/england_bound_sub_divided.parquet"
)
england_bound_sub_divided.createOrReplaceTempView("england_bound_sub_divided")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Subset centroids that intersect England and write out

# COMMAND ----------

eng_combo_centroids = spark.sql(
    "SELECT combo_centroids.* FROM combo_centroids, england_bound_sub_divided WHERE ST_INTERSECTS(combo_centroids.geometry, england_bound_sub_divided.geometry)"
)
eng_combo_centroids.write.format("geoparquet").mode("overwrite").save(
    f"{alt_out_path}/{grid_square_size}m_england_grid_centroids.parquet"
)
