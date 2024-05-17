# Databricks notebook source
import geopandas as gpd
import pandas as pd

# COMMAND ----------

# change the pandas float display for easy reading
pd.options.display.float_format = '{:.2f}'.format

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# Read in the entire epims datasets - this doesn't have any department information only an epims holding number, objectid and geometry, so need to join up to the point dataset for that info
epims = gpd.read_file(epims_path)
display(epims)

# COMMAND ----------

# Read in point dataset - filter for defra points only
epims_point = pd.read_csv(epims_point_path)
epims_point_defra = epims_point[epims_point['Department']=='Department for Environment, Food and Rural Affairs']

# COMMAND ----------

# Join epims polygons to defra points by holding ID, this gives us the attribute info for polygons
epims = epims.merge(epims_point_defra, left_on='ePIMSHoldi', right_on='ePIMSHoldingId')
epims = epims.explode()
epims.geom_type.unique()
epims['df'] = 'epims'

# COMMAND ----------

#validate geometries
epims['geometry'] = epims.make_valid()
invalid_epims = epims.loc[~epims.geometry.is_valid]
display(invalid_epims)

# COMMAND ----------

# Read in defra polygon dataset created from HMLR data 
polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)
polygon_ccod_defra = polygon_ccod_defra.explode()
polygon_ccod_defra.geom_type.unique()
polygon_ccod_defra['df'] = 'polygon_ccod_defra'

# COMMAND ----------

# validate geometries for spatial work
polygon_ccod_defra['geometry'] = polygon_ccod_defra.make_valid()
invalid_polygon_ccod_defra = polygon_ccod_defra.loc[~polygon_ccod_defra.geometry.is_valid]
display(invalid_polygon_ccod_defra)

# COMMAND ----------

# Comparison of dissolved datasets

#dissolve epims for flat comparison
epims_dissolved = epims.dissolve()
epims_dissolved['area_m2'] = epims_dissolved.area
# dissolve polygon_ccod for flat comparison
polygon_ccod_defra_dissolved = polygon_ccod_defra.dissolve()
polygon_ccod_defra_dissolved['df'] = 'polygon_ccod_defra_dissolved'
polygon_ccod_defra_dissolved['area_m2'] = polygon_ccod_defra_dissolved.area
# get difference in dissolved polyons and area comparisons
difference_dissolved_epims_polygon_ccod_defra = polygon_ccod_defra_dissolved.overlay(epims_dissolved, how='symmetric_difference', keep_geom_type=False, make_valid=True)
difference_dissolved_epims_polygon_ccod_defra['area_m2'] = difference_dissolved_epims_polygon_ccod_defra.area
# combine df columns
difference_dissolved_epims_polygon_ccod_defra['df'] = None
difference_dissolved_epims_polygon_ccod_defra['df'].update({0: 'polygon_ccod_defra_with_no_overlapping_epims', 1: 'epims_with_no_overlapping_polygon_ccod_defra'})

# COMMAND ----------

# Comparison of undissolved datasets
polygon_ccod_defra_with_no_overlapping_epims_undissolved = polygon_ccod_defra.overlay(epims, how='difference', keep_geom_type=False, make_valid=True)
epims_with_no_overlapping_polygon_ccod_defra_undissolved = epims.overlay(polygon_ccod_defra, how='difference', keep_geom_type=False, make_valid=True)

# COMMAND ----------

# get count of polygons present in epims but not hmlr derrived data
epims_with_no_overlapping_polygon_ccod_defra_undissolved['PropertyCentre'].value_counts()
#Dissolve on property centre and get total area for each 
epims_with_no_overlapping_polygon_ccod_defra_by_organisation = epims_with_no_overlapping_polygon_ccod_defra_undissolved.dissolve(by='PropertyCentre')
epims_with_no_overlapping_polygon_ccod_defra_by_organisation['area_m2'] = epims_with_no_overlapping_polygon_ccod_defra_by_organisation.area
display(epims_with_no_overlapping_polygon_ccod_defra_by_organisation)

# COMMAND ----------

# get count of polygons present in hmlr derrived data but not epims
display(polygon_ccod_defra_with_no_overlapping_epims_undissolved['current_organisation'].value_counts())
# dissolve on current org field and get total area for each
polygon_ccod_defra_with_no_overlapping_epims_by_organisation = polygon_ccod_defra_with_no_overlapping_epims_undissolved.dissolve(by='current_organisation')
polygon_ccod_defra_with_no_overlapping_epims_by_organisation['area_m2'] = polygon_ccod_defra_with_no_overlapping_epims_by_organisation.area
display(polygon_ccod_defra_with_no_overlapping_epims_by_organisation)

# COMMAND ----------

#create area df
area_df = gpd.GeoDataFrame(difference_dissolved_epims_polygon_ccod_defra[['df','area_m2', 'geometry']])
pd.concat([area_df, polygon_ccod_defra_dissolved[['df', 'area_m2', 'geometry']], epims_dissolved[['df', 'area_m2', 'geometry']]])

# COMMAND ----------

polygon_ccod_defra_with_no_overlapping_epims = area_df[area_df['df']=='polygon_ccod_defra_with_no_overlapping_epims']
epims_with_no_overlapping_polygon_ccod_defra = area_df[area_df['df']=='epims_with_no_overlapping_polygon_ccod_defra']

# COMMAND ----------

polygon_ccod_defra_with_no_overlapping_epims.to_file(polygon_ccod_defra_with_no_overlapping_epims_path, driver='GeoJSON', mode='w')
epims_with_no_overlapping_polygon_ccod_defra.to_file(epims_with_no_overlapping_polygon_ccod_defra_path, driver='GeoJSON', mode='w')

# COMMAND ----------

area_df[area_df['df']=='epims_defra_with_no_overlapping_polygon_ccod_defra'].explore()

# COMMAND ----------

difference_epims_defra_polygon_ccod_defra = polygon_ccod_defra.overlay(epims_defra, how='symmetric_difference', keep_geom_type=False, make_valid=True)


# COMMAND ----------

difference_epims_defra_polygon_ccod_defra[difference_epims_defra_polygon_ccod_defra['Department'].notnull()]
