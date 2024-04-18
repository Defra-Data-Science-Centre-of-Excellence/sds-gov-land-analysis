# Databricks notebook source
# import packages
import geopandas as gpd
import pandas as pd
import folium

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get defra related polygons

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

# import ccod filtered for defra data
ccod_defra = pd.read_csv(ccod_defra_path, sep = ',')

# COMMAND ----------

# Create sql statement to select for defra associated title numbers
title_numbers_of_interest = ccod_defra['Title Number'].unique()
title_numbers_of_interest_sql_string = ''
for title_number in title_numbers_of_interest:
    title_numbers_of_interest_sql_string = f"{title_numbers_of_interest_sql_string}'{title_number}', "
title_numbers_of_interest_sql_string = title_numbers_of_interest_sql_string.rstrip(', ')
title_numbers_of_interest_sql_string = f'({title_numbers_of_interest_sql_string})'

# COMMAND ----------

# import unfiltered national polygon dataset
national_polygon_dfs = []
for national_polygon_path in national_polygon_paths:
    national_polygon_df = gpd.read_file(national_polygon_path)#, where = f'TITLE_NO IN {title_numbers_of_interest_sql_string}')
    national_polygon_dfs.append(national_polygon_df)
    print(f'loaded into dataframe: {national_polygon_path}')
national_polygon = pd.concat(national_polygon_dfs, ignore_index=True)


# COMMAND ----------

# join polygon dataset with defra ccod data (this allows you to bring in only selected defra related polygons - which speeds up the import)
polygon_ccod_defra = national_polygon.merge(ccod_defra, how='inner', left_on='TITLE_NO', right_on='Title Number')
display(polygon_ccod_defra)

# COMMAND ----------

display(polygon_ccod_defra)

# COMMAND ----------

# write fe polygons with associated ccod info to geojson
polygon_ccod_defra.to_file(polygon_ccod_defra_path, driver='GeoJSON')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get info on polygons adjascent to defra related polygons

# COMMAND ----------

# buffer defra polygons, so can identify adjescent polygons by overlap
# buffer can be changed as needed
polygon_ccod_defra_buffered = polygon_ccod_defra
polygon_ccod_defra_buffered['geometry'] = polygon_ccod_defra_buffered.geometry.buffer(0.2)

# COMMAND ----------

# join polygon dataset with unfiltered ccod data - need this to look at adjascent polyogon info etc.
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')
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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get some stats on the selected data

# COMMAND ----------

print(len(polygon_ccod))
undissolved_area = polygon_ccod['geometry'].area.sum()
print(f'Are using undissolved polygons is: {undissolved_area}')
polygon_ccod.head(5)

# COMMAND ----------

polygon_ccod_dissolved = polygon_ccod.dissolve()
dissolved_area = polygon_ccod_dissolved['geometry'].area.sum()
dissolved_area_sqkm = dissolved_area/1000000
print(f'Area using dissolved polygons is: {dissolved_area} square metres, or {dissolved_area_sqkm} square kilometres')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot selected polygon data for inspection

# COMMAND ----------

# plot using matplotlib
plot_undissolved = polygon_ccod.plot()
plot_dissolved = polygon_ccod_dissolved.plot()

# COMMAND ----------

# use geopandas explore method to allow zooming
polygon_ccod_sample = polygon_ccod.sample(1200)
polygon_ccod_sample.explore()
