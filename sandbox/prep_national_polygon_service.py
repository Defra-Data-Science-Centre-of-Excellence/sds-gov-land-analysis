# Databricks notebook source
# import packages
import geopandas as gpd
import pandas as pd

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# set file path
national_polygon_paths = [
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_0.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_1.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_2.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_3.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_4.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_5.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_6.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_7.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_8.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_NOV_2023_9.shp',
]
ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/ccod_defra.csv'

# COMMAND ----------

# read in ccod data
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
        "Additional Proprietor Indicator"
        ]
)

# COMMAND ----------

# import ccod defra data
ccod_defra = pd.read_csv(ccod_defra_path, sep = ',')
title_numbers_of_interest = ccod_defra['Title Number'].unique()
title_numbers_of_interest_sql_string = ''
for title_number in title_numbers_of_interest:
    title_numbers_of_interest_sql_string = f"{title_numbers_of_interest_sql_string}'{title_number}', "
title_numbers_of_interest_sql_string = title_numbers_of_interest_sql_string.rstrip(', ')
title_numbers_of_interest_sql_string = f'({title_numbers_of_interest_sql_string})'

# COMMAND ----------

# import national polygon dataset
national_polygon_dfs = []
for national_polygon_path in national_polygon_paths:
    national_polygon_df = gpd.read_file(national_polygon_path)#, where = f'TITLE_NO IN {title_numbers_of_interest_sql_string}')
    national_polygon_dfs.append(national_polygon_df)
    print(f'loaded into dataframe: {national_polygon_path}')
national_polygon = pd.concat(national_polygon_dfs, ignore_index=True)


# COMMAND ----------

# join polygon dataset with defra ccod data
polygon_ccod_defra = national_polygon.merge(ccod_defra, how='inner', left_on='TITLE_NO', right_on='Title Number')
# join polygon dataset with ccod data
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')


# COMMAND ----------

polygon_ccod_defra_buffered = polygon_ccod_defra
polygon_ccod_defra_buffered['geometry'] = polygon_ccod_defra_buffered.geometry.buffer(1)

# COMMAND ----------

polygon_ccod_defra_buffered

# COMMAND ----------

polygon_ccod

# COMMAND ----------

national_polygon_bordering_defra_properties = polygon_ccod.sjoin(polygon_ccod_defra_buffered, how='inner', lsuffix='_all', rsuffix='_defra')

# COMMAND ----------

national_polygon_bordering_defra_properties

# COMMAND ----------

defra_bordered_corporate_body_proprietors = national_polygon_bordering_defra_properties[national_polygon_bordering_defra_properties['Proprietorship Category (1)__all']=='Corporate Body']
defra_bordered_proprietors = national_polygon_bordering_defra_properties['Proprietor Name (1)__all'].unique()
pd.DataFrame(defra_bordered_proprietors)

# COMMAND ----------

print(len(polygon_ccod))
undissolved_area = polygon_ccod['geometry'].area.sum()
print(f'Are using undissolved polygons is: {undissolved_area}')
polygon_ccod.head(5)

# COMMAND ----------

polygon_ccod_dissolved = polygon_ccod.dissolve()
print(len(polygon_ccod_dissolved))
dissolved_area = polygon_ccod_dissolved['geometry'].area.sum()
print(type(dissolved_area))
dissolved_area_sqkm = dissolved_area/1000000
print(f'Are using dissolved polygons is: {dissolved_area} square metres, or {dissolved_area_sqkm} square kilometres')

# COMMAND ----------

polygon_ccod.plot()

# COMMAND ----------

polygon_ccod_dissolved.plot()

# COMMAND ----------

# import mapping packages
import plotly.express as px
import matplotlib.pyplot as plt
from plotly.offline import plot

# COMMAND ----------

from plotly.offline import plot
from plotly.graph_objs import *
import numpy as np

figure = plot(
    [
        
    ]
)

# COMMAND ----------


polygon_ccod_sample = polygon_ccod.sample(1200)
polygon_ccod_sample.explore()

# COMMAND ----------


polygon_ccod_dissolved.explore()

# COMMAND ----------

# convert dataframe to geojson
import json
polygon_ccod_json_string = polygon_ccod.to_json()
polygon_ccod_json = json.loads(polygon_ccod_json_string)


# COMMAND ----------

#json version
fig = px.choropleth(polygon_ccod_json)
fig.show()

# COMMAND ----------

# geodtaframe version
fig = px.choropleth(polygon_ccod, geojson=polygon_ccod.geometry, locations=polygon_ccod.index, color='Proprietor Name (1)',
                           #color_continuous_scale="Viridis",
                           #range_color=(0, 12),
                           #mapbox_style="carto-positron",
                           #zoom=5, center = {"lat": 52.378, "lon": 3.436},
                          )
fig.show()
