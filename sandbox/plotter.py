# Databricks notebook source
import geopandas as gpd
import matplotlib

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

data = gpd.read_parquet('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/15_minute_greenspace/open_access_land/study_area_nps/salisbury/nps_ccod.parquet')

# COMMAND ----------

data.to_file('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/15_minute_greenspace/open_access_land/study_area_nps/salisbury/nps_ccod.geojson', driver='GeoJSON')

# COMMAND ----------

data.explore()

# COMMAND ----------

all_defra_land = gpd.read_file(polygon_ccod_defra_path)
