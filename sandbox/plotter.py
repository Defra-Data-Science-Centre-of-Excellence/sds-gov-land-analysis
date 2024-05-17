# Databricks notebook source
import geopandas as gpd

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

data = gpd.read_parquet('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/15_minute_greenspace/open_access_land/study_area_nps/salisbury/nps_ccod.parquet')

# COMMAND ----------

data.to_file('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/15_minute_greenspace/open_access_land/study_area_nps/salisbury/nps_ccod.geojson', driver='GeoJSON')

# COMMAND ----------

data.explore()
