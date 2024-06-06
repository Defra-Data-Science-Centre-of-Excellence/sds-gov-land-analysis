# Databricks notebook source
# MAGIC %sh
# MAGIC pip install geodatasets

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install contextily

# COMMAND ----------

import geopandas as gpd
import matplotlib
import geodatasets
import contextily as cx

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

all_defra_land = gpd.read_file(polygon_ccod_defra_path)

# COMMAND ----------

URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png".format

# COMMAND ----------

england_boundary = gpd.read_file('/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/LATEST_england_boundary/')
os_map = gpd.read_file('/dbfs/mnt/base/unrestricted/source_openstreetmap /dataset_united_kingdom/format_PBF_united_kingdom/LATEST_united_kingdom/')

# COMMAND ----------

ax = all_defra_land.plot(column='current_organisation', legend=True, figsize=(20, 15))
leg = ax.get_legend()
leg.set_bbox_to_anchor((0., 0., 0.5, -0.1))
