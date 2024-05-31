# Databricks notebook source
import geopandas as gpd
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

def get_overlaps(polygon_gdf_1, polygon_gdf_2):
    '''
    '''
    intersecting_gdf = polygon
    return polygon_gdf_overlaps

# COMMAND ----------

polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)
polygon_ccod = gpd.read_parquet(polygon_ccod_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get self overlap inside defra estate

# COMMAND ----------

freehold_polygon_ccod_defra = polygon_ccod_defra[polygon_ccod_defra['Tenure']=='Freehold']

# COMMAND ----------

overlaps = gpd.overlay(freehold_polygon_ccod_defra, freehold_polygon_ccod_defra, how='intersection')

# COMMAND ----------

overlaps[overlaps['POLY_ID_1']!=overlaps['POLY_ID_2']]

# COMMAND ----------

overlaps.area.sum()/10000

# COMMAND ----------

overlaps.dissolve().area.sum()/10000

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get overlap with non-defra estate

# COMMAND ----------

# remove defra parcels from polygon ccod by title number
polygon_ccod_defra_titles = polygon_ccod_defra['Title Number'].unique()
polygon_ccod_defra_removed = polygon_ccod[~polygon_ccod['Title Number'].isin(polygon_ccod_defra_titles)]
freehold_polygon_ccod_defra_removed = polygon_ccod_defra_removed[polygon_ccod_defra_removed['Tenure'] == 'Freehold']

# COMMAND ----------

overlap_with_non_defra_estate = gpd.overlay(freehold_polygon_ccod_defra, freehold_polygon_ccod_defra_removed, how='intersection', make_valid=True)

# COMMAND ----------

overlap_with_non_defra_estate.area.sum()/10000


# COMMAND ----------

overlap_with_non_defra_estate.dissolve().area.sum()/10000
