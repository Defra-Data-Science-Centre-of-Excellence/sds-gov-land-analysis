# Databricks notebook source
import geopandas as gpd

# COMMAND ----------

# MAGIC %md
# MAGIC Set the name of the study area you want to output

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# User to set variables
easting = 297293
northing = 82203
study_area_filename = 'starcross'
buffer_distance = 1000

# COMMAND ----------

point = gpd.points_from_xy(x=[easting], y=[northing], crs="EPSG:27700")
square = point.buffer(distance=buffer_distance, cap_style=3)
study_area = gpd.GeoDataFrame(geometry = square)

# COMMAND ----------

display(square)

# COMMAND ----------

print(study_area.crs)
display(study_area)

# COMMAND ----------

# visually inspect study area box
study_area.explore()

# COMMAND ----------

# write to parquet
study_area.to_parquet(f'{study_area_directory_path}/{study_area_filename}.parquet')
