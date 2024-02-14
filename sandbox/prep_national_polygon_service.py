# Databricks notebook source
# import packages
import geopandas

# COMMAND ----------

# set file path
national_polygon_path = '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_0.shp'

# COMMAND ----------

national_polygon = geopandas.read_file(national_polygon_path, rows=1000)
national_polygon.head()
#national_polygon.plot(cmap="Set1")
