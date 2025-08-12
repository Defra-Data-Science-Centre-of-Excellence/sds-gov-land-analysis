# Databricks notebook source
# MAGIC %md
# MAGIC ### Identify land parcels
# MAGIC This script will join the filtered ccod dataset to the national polygon service, providing a spatial represention of land ownership for the department of interest and its ALBs. <br>
# MAGIC The produced dataset will not be flat (there may (probably!) be overlapping polygons where titles overlap). <br>
# MAGIC <b> Prerequirites: </b> Before running this script, ensure you have a filtered version of the CCOD representing just the organisations of interest (ie. have run the 'identify_title_numbers' script to produce a filtered CCOD for the department of interest and its ALBs) <br>
# MAGIC <b>Next steps:</b> 
# MAGIC To help validate the produced datasets, and identify potentially problematic overlaps, use the 'data_validation_overlaps' script.
# MAGIC To create a flat multipolygon for each organisation, use the 'create_organisation_level_data' script. <br>
# MAGIC Alternatively, to summarise this spatial dataset by area, use the 'area_calculations' script.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

# import packages
import geopandas as gpd
import pandas as pd
import folium

# COMMAND ----------

# MAGIC %run ./paths

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

# MAGIC %run ./functions

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

# import ccod filtered for department data
department_ccod = pd.read_csv(department_ccod_path, sep = ',')

# COMMAND ----------

national_polygon = gpd.read_parquet(national_polygon_parquet_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join department ccod to polygon dataset

# COMMAND ----------

display(department_ccod)

# COMMAND ----------

# join polygon dataset with department ccod data (this allows you to bring in only selected department related polygons - which speeds up the import)
polygon_ccod_w_dept = national_polygon.merge(department_ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')
display(polygon_ccod_w_dept)

# COMMAND ----------

department_polygons = polygon_ccod_w_dept[polygon_ccod_w_dept['current_organisation'].notna()]

# COMMAND ----------

display(department_polygons)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Export identified department polygons

# COMMAND ----------

department_polygons.to_parquet(department_polygons_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional: extract a study area portion of the NPS

# COMMAND ----------

# join polygon dataset with unfiltered ccod data - need this to look at adjascent polyogon info etc.
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')

# COMMAND ----------

# Working with the whole National Polygon Service dataset can be quite cumbersome, so this section allows you to 'cut' the NPS to a sample area (the sample area itself can be created using the 'create_study_boundary' script), and save the cut NPS as a parquet.

# COMMAND ----------

study_area = gpd.read_parquet(f'{study_area_directory_path}/botanic_gardens.parquet')

# COMMAND ----------

study_area_nps = polygon_ccod.overlay(study_area, how='intersection', keep_geom_type=False, make_valid=True)

# COMMAND ----------

study_area_nps.to_parquet(f'{nps_by_study_area_directory_path}/botanic_gardens_nps_ccod.parquet')

# COMMAND ----------

study_area_nps.explore()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional: Get percentage of department titles not in National Polygon Dataset

# COMMAND ----------

department_ccod_polygons = national_polygon.merge(department_ccod, how='right', left_on='TITLE_NO', right_on='Title Number')
display(department_ccod_polygons)

# COMMAND ----------

# get a count of titles with no polygon record by organisation
missing_department_ccod_polygons = department_ccod_polygons[department_ccod_polygons['TITLE_NO'].isna()]
missing_department_ccod_polygons['current_organisation'].value_counts()

# COMMAND ----------

# for context, let's also get the total number of identified department titles
len(department_ccod_polygons)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional: Get info on polygons adjascent to department polygons

# COMMAND ----------

# join polygon dataset with unfiltered ccod data - need this to look at adjascent polyogon info etc.
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')

# COMMAND ----------

# buffer department polygons, so can identify adjescent polygons by overlap
# buffer can be changed as needed
department_polygons_buffered = department_polygons.copy()
department_polygons_buffered['geometry'] = department_polygons_buffered.geometry.buffer(0.2)

# COMMAND ----------

# spatial join buffered department polygons with unfiltered polygon-ccod data
national_polygon_bordering_department_properties = polygon_ccod.sjoin(department_polygons_buffered, how='inner', lsuffix='_all', rsuffix='_department')
# display attributes for overlapping polygons for visual inspection
display(national_polygon_bordering_department_properties)

# COMMAND ----------

# remove records where the all nps name = identified department nps name
national_polygon_bordering_department_properties_not_identified_as_department = national_polygon_bordering_department_properties[national_polygon_bordering_department_properties['Proprietor Name (1)__all']!=national_polygon_bordering_department_properties['Proprietor Name (1)__department']]
display(national_polygon_bordering_department_properties_not_identified_as_department)

# COMMAND ----------

# Filter for records with proprietorship category of corporate body - as this is what public sector bodies fall under and we're only interested in identifying extra department land
department_bordered_corporate_body_proprietors = national_polygon_bordering_department_properties[national_polygon_bordering_department_properties['Proprietorship Category (1)__all']=='Corporate Body']
# Get unique list of proprietor names
department_bordered_proprietors = national_polygon_bordering_department_properties['Proprietor Name (1)__all'].value_counts()
# print for visual inspection - this is still too many records to look at manually :(
display(national_polygon_bordering_department_properties['Proprietor Name (1)__all'].value_counts().to_frame().reset_index())

# COMMAND ----------

# remove department proprietor names which have already been identified
national_polygon_bordering_department_properties_unidentified = national_polygon_bordering_department_properties[~national_polygon_bordering_department_properties['Proprietor Name (1)__all'].isin(department_ccod['Proprietor Name (1)'])]
display(national_polygon_bordering_department_properties_unidentified['Proprietor Name (1)__all'].value_counts().to_frame().reset_index())
