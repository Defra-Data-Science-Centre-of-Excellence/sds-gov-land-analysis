# Databricks notebook source
# import packages
import geopandas as gpd
import pandas as pd

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import FE supplied data

# COMMAND ----------

# import fe polygon data
fe_polygons = gpd.read_file(fe_polygons_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get overlapping titles from unfiltered nps

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
        "Additional Proprietor Indicator"
        ]
)

# COMMAND ----------

# import unfiltered national polygon dataset
national_polygon_dfs = []
for national_polygon_path in national_polygon_paths:
    national_polygon_df = gpd.read_file(national_polygon_path)#, where = f'TITLE_NO IN {title_numbers_of_interest_sql_string}')
    national_polygon_dfs.append(national_polygon_df)
    print(f'loaded into dataframe: {national_polygon_path}')
national_polygon = pd.concat(national_polygon_dfs, ignore_index=True)

# COMMAND ----------

# join ccod data to polygons
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')

# COMMAND ----------

# get nps-ccod records overlapping with fe polygons
overlap_fe_polygon_ccod = gpd.sjoin(fe_polygons, polygon_ccod, how='left', lsuffix='_fe', rsuffix='_ccod')
# write fe polygons with associated ccod info to geojson
overlap_fe_polygon_ccod.to_file(fe_title_polygons_with_ccod_data_path, driver='GeoJSON')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get overlapping polygons from defra filtered nps

# COMMAND ----------

# import defra polygon data
polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)

# COMMAND ----------

# get defra nps-ccod records overlapping with fe polygons
overlap_fe_defra_polygon_ccod = gpd.sjoin(fe_polygons, polygon_ccod_defra, how='left', lsuffix='_fe', rsuffix='_ccod')
display(overlap_fe_defra_polygon_ccod)
# write fe polygons with associated ccod info to geojson
#overlap_fe_defra_polygon_ccod.to_file(fe_title_polygons_with_ccod_data_path, driver='GeoJSON')

# COMMAND ----------

display(overlap_fe_defra_polygon_ccod['Title Number'].value_counts().to_frame().reset_index())

# COMMAND ----------

polygon_ccod_fe = polygon_ccod_defra[polygon_ccod_defra['Title Number'].isin(overlap_fe_defra_polygon_ccod['Title Number'])]
polygon_ccod_defra_not_fe = polygon_ccod_defra[~polygon_ccod_defra['Title Number'].isin(overlap_fe_defra_polygon_ccod['Title Number'])]

# COMMAND ----------

display(polygon_ccod_fe)

# COMMAND ----------

display(polygon_ccod_defra_not_fe)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare fe titles associated with unfiltered nps and defra nps

# COMMAND ----------

# import defra polygon data
polygon_ccod_fe_unfiltered_by_defra = gpd.read_file(fe_title_polygons_with_ccod_data_path)

# COMMAND ----------

polygon_ccod_fe_unfiltered = polygon_ccod[polygon_ccod['Title Number'].isin(polygon_ccod_fe_unfiltered_by_defra['Title Number'])]
display(polygon_ccod_fe_unfiltered)

# COMMAND ----------

polygon_ccod_fe_unfiltered.to_file(polygon_ccod_fe_unfiltered_path, driver='GeoJSON')
