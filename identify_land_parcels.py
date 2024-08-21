# Databricks notebook source
# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

# import packages
import geopandas as gpd
import pandas as pd
import folium

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./constants

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read in required datasets

# COMMAND ----------

# import unfiltered ccod data
ccod = pd.read_csv(
    ccod_path,    
    usecols=[
        "Title Number",
        "Tenure",
        "Proprietor Name (1)",
        "Company Registration No. (1)",
        "Proprietorship Category (1)",
        "Proprietor (1) Address (1)",
        "Date Proprietor Added",
        "Additional Proprietor Indicator",
        "Proprietor Name (2)",
        "Proprietor Name (3)",
        "Proprietor Name (4)"
        ]
)

# import ccod filtered for defra data
ccod_defra = pd.read_csv(ccod_defra_and_alb_path, sep = ',')

# COMMAND ----------

national_polygon = gpd.read_parquet(national_polygon_parquet_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join CCOD to polygon data

# COMMAND ----------

# join polygon dataset with unfiltered ccod data - need this to look at adjascent polyogon info etc.
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')

# COMMAND ----------

# MAGIC %md
# MAGIC #### temp

# COMMAND ----------

ccod_defra['Proprietor (1) Address (1)'].unique()

# COMMAND ----------

ccod_defra[ccod_defra['Proprietor (1) Address (1)'].str.contains('Bridge')]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional: extract a study area portion of the NPS

# COMMAND ----------

study_area = gpd.read_parquet(f'{study_area_directory_path}/starcross.parquet')

# COMMAND ----------

study_area_nps = polygon_ccod.overlay(study_area, how='intersection', keep_geom_type=False, make_valid=True)

# COMMAND ----------

study_area_nps.to_parquet(f'{nps_by_study_area_directory_path}/starcross_nps_ccod.parquet')

# COMMAND ----------

study_area_npd = national_polygon.overlay(study_area, how='intersection', keep_geom_type=False, make_valid=True)

# COMMAND ----------

study_area_npd.explore()

# COMMAND ----------

display(pd.DataFrame(study_area_nps['Proprietor Name (1)'].value_counts()).reset_index())

# COMMAND ----------

study_area_nps.explore()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get identified DEFRA polygons

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get percentage of defra titles not in 

# COMMAND ----------

all_defra_ccod_polygons = national_polygon.merge(ccod_defra, how='right', left_on='TITLE_NO', right_on='Title Number')
display(all_defra_ccod_polygons)

# COMMAND ----------

# get a count of titles with no polygon record by organisation
missing_defra_ccod_polygons = all_defra_ccod_polygons[all_defra_ccod_polygons['TITLE_NO'].isna()]
missing_defra_ccod_polygons['current_organisation'].value_counts()

# COMMAND ----------

# for context, let's also get the total number of identified defra titles
len(all_defra_ccod_polygons)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join defra ccod to polygon dataset

# COMMAND ----------

display(ccod_defra)

# COMMAND ----------

# join polygon dataset with defra ccod data (this allows you to bring in only selected defra related polygons - which speeds up the import)
polygon_ccod_defra = national_polygon.merge(ccod_defra, how='inner', left_on='TITLE_NO', right_on='Title Number')
display(polygon_ccod_defra)

# COMMAND ----------

polygon_ccod_defra_only = polygon_ccod_defra[polygon_ccod_defra['current_organisation'].notna()]

# COMMAND ----------

display(polygon_ccod_defra_only)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Export identified defra polygons

# COMMAND ----------

polygon_ccod_defra_only.to_file(polygon_ccod_defra_path, driver='GeoJSON', mode='w')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional: Get info on polygons adjascent to defra polygons

# COMMAND ----------

# buffer defra polygons, so can identify adjescent polygons by overlap
# buffer can be changed as needed
polygon_ccod_defra_buffered = polygon_ccod_defra.copy()
polygon_ccod_defra_buffered['geometry'] = polygon_ccod_defra_buffered.geometry.buffer(0.2)

# COMMAND ----------

# spatial join buffered defra polygons with unfiltered polygon-ccod data
national_polygon_bordering_defra_properties = polygon_ccod.sjoin(polygon_ccod_defra_buffered, how='inner', lsuffix='_all', rsuffix='_defra')
# display attributes for overlapping polygons for visual inspection
display(national_polygon_bordering_defra_properties)

# COMMAND ----------

# remove records where the all nps name = identified defra nps name
national_polygon_bordering_defra_properties_not_identified_as_defra = national_polygon_bordering_defra_properties[national_polygon_bordering_defra_properties['Proprietor Name (1)__all']!=national_polygon_bordering_defra_properties['Proprietor Name (1)__defra']]
display(national_polygon_bordering_defra_properties_not_identified_as_defra)

# COMMAND ----------

# Filter for records with proprietorship category of corporate body - as this is what public sector bodies fall under and we're only interested in identifying extra defra land
defra_bordered_corporate_body_proprietors = national_polygon_bordering_defra_properties[national_polygon_bordering_defra_properties['Proprietorship Category (1)__all']=='Corporate Body']
# Get unique list of proprietor names
defra_bordered_proprietors = national_polygon_bordering_defra_properties['Proprietor Name (1)__all'].value_counts()
# print for visual inspection - this is still too many records to look at manually :(
display(national_polygon_bordering_defra_properties['Proprietor Name (1)__all'].value_counts().to_frame().reset_index())

# COMMAND ----------

# remove defra proprietor names which have already been identified
national_polygon_bordering_defra_properties_unidentified = national_polygon_bordering_defra_properties[~national_polygon_bordering_defra_properties['Proprietor Name (1)__all'].isin(ccod_defra['Proprietor Name (1)'])]
display(national_polygon_bordering_defra_properties_unidentified['Proprietor Name (1)__all'].value_counts().to_frame().reset_index())
