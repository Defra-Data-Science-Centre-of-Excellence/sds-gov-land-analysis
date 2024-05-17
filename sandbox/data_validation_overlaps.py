# Databricks notebook source
import geopandas as gpd
import numpy as np

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

def get_overlaps(polygon_gdf):
    '''
    '''
    intersecting_gdf = polygon_gdf.sjoin(polygon_gdf, predicate="intersects")
    intersecting_gdf = intersecting_gdf.loc[
        intersecting_gdf.index != intersecting_gdf.index_right
    ]
    overlaps_ids = np.unique(intersecting_gdf.index)
    polygon_gdf_overlaps = polygon_gdf.iloc[overlaps_ids]
    return polygon_gdf_overlaps

# COMMAND ----------

hmlr_fe_buffer_minus1_gaps_ccod_info = gpd.read_file(hmlr_fe_buffer_minus1_gaps_ccod_info_path)
overlaps_hmlr_fe_buffer_minus1_gaps_ccod_info = get_overlaps(hmlr_fe_buffer_minus1_gaps_ccod_info)

# COMMAND ----------

hmlr_fe_buffer_minus05_gaps_ccod_info = gpd.read_file(hmlr_fe_buffer_minus05_gaps_ccod_info_path)
overlaps_hmlr_fe_buffer_minus1_gaps_ccod_info = get_overlaps(hmlr_fe_buffer_minus05_gaps_ccod_info)

# COMMAND ----------

overlaps_hmlr_fe_buffer_minus1_gaps_ccod_info
