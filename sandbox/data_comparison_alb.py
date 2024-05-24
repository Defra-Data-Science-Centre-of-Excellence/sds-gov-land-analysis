# Databricks notebook source
# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install openpyxl

# COMMAND ----------

import pandas as pd
import openpyxl
import geopandas as gpd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get all ccod and identified ccod of interest data

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
        "Additional Proprietor Indicator",
        "Proprietor Name (2)",
        "Proprietor Name (3)",
        "Proprietor Name (4)"
        ]
)

# import ccod filtered for defra data
ccod_of_interest = pd.read_csv(ccod_defra_and_alb_path, sep = ',')

# COMMAND ----------

# Read in dataset created from land registry
polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comparison to Piumi's work

# COMMAND ----------

# MAGIC %md
# MAGIC #### EA data comparison

# COMMAND ----------

# import title list provided by ea
ea_titles = pd.read_excel(ea_titles_path, sheet_name='Freehold_titles_102023')
display(ea_titles)

# COMMAND ----------

# Get ea records from ccod data selected by my methods
ea_ccod = ccod_of_interest[ccod_of_interest['current_organisation'] == 'Environment Agency']

# COMMAND ----------

# join selected ea ccod data to ea title list (on title number) to enable comparison
ea_ccod_and_supplied_titles = ea_ccod.merge(ea_titles, how='outer', left_on='Title Number', right_on='Title', suffixes=('_filtered_ccod','_ea'))
display(ea_ccod_and_supplied_titles)

# COMMAND ----------

# Find ea titles which have not been identified by my script, by selecting for null Title Number (from selected ea ccod table) field. Then join to unfiltered ccod data to get attribute info.
unidentified_ea_titles = ea_ccod_and_supplied_titles[ea_ccod_and_supplied_titles['Title Number'].isna()]
unidentified_ea_titles_ccod = ccod.merge(unidentified_ea_titles, how='inner', left_on='Title Number', right_on='Title', suffixes=('_unfiltered_ccod','_comparison_table'))
display(unidentified_ea_titles_ccod)

# COMMAND ----------

display(unidentified_ea_titles_ccod['Proprietor Name (1)_unfiltered_ccod'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC Need to get the area of these?

# COMMAND ----------

# Find in ccod ea titles which have been identified by my script, but aren't in list from ea. Don't need to join back to unfiltered ccod data here as it should already be present from filtered ccod data.
extra_identified_ea_titles = ea_ccod_and_supplied_titles[ea_ccod_and_supplied_titles['Title'].isna()]
# Remove leasehold as the EA only provided Freehold titles in their list
extra_identified_ea_titles = extra_identified_ea_titles[extra_identified_ea_titles['Tenure_filtered_ccod']=='Freehold']
display(extra_identified_ea_titles)

# COMMAND ----------

display(extra_identified_ea_titles['Proprietor Name (1)'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### NE Data Comparison

# COMMAND ----------

# Import title list provided by NE
ne_titles = pd.read_csv(ne_titles_path)
# Rename Title NUmber field to 'Title' field (to match EA and help column tracking/maintenance)
ne_titles = ne_titles.rename(columns={'Title Number': 'Title'})
display(ne_titles)

# COMMAND ----------

# Get ne records from ccod data selected by my methods
ne_ccod = ccod_of_interest[ccod_of_interest['current_organisation'] == 'Natural England']

# COMMAND ----------

# join selected ne ccod data to ne title list (on title number) to enable comparison
ne_ccod_and_supplied_titles = ne_ccod.merge(ne_titles, how='outer', left_on='Title Number', right_on='Title')
display(ne_ccod_and_supplied_titles)

# COMMAND ----------

# Find ne titles which have not been identified by my script, by selecting for null Title Number (from selected ne ccod table) field. Then join to unfiltered ccod data to get attribute info.
unidentified_ne_titles = ne_ccod_and_supplied_titles[ne_ccod_and_supplied_titles['Title Number'].isna()]
unidentified_ne_titles_ccod = ccod.merge(unidentified_ne_titles, how='inner', left_on='Title Number', right_on='Title')
display(unidentified_ne_titles)

# Joining back to unfiltered ccod data produces empty table error, suggesting title numbers aren't present in unfiltered ccod - need to check this


# COMMAND ----------

# Find in ccod ne titles which have been identified by my script, but aren't in list from ne. Don't need to join back to unfiltered ccod data here as it should already be present from filtered ccod data.
extra_identified_ne_titles = ne_ccod_and_supplied_titles[ne_ccod_and_supplied_titles['Title'].isna()]
display(extra_identified_ne_titles)

# COMMAND ----------

display(extra_identified_ne_titles['Proprietor Name (1)'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defra data comparison

# COMMAND ----------

defra_ccod = ccod_of_interest[ccod_of_interest['Current_organisation'] == 'Department for Environment, Food and Rural Affairs']

# COMMAND ----------

defra_proposed_title_numbers = defra_ccod['Title Number'].unique()
print(len(defra_proposed_title_numbers))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Forestry Commission data comparison

# COMMAND ----------

fc_defra_ccod = ccod_of_interest[ccod_of_interest['current_organisation'].isin(['Forestry Commission', 'Department for Environment, Food and Rural Affairs'])]

# COMMAND ----------

fc_ccod = ccod_of_interest[ccod_of_interest['current_organisation'] == 'Forestry Commission']
display(fc_ccod)

# COMMAND ----------

potential_fc_polygon_ccod = polygon_ccod_defra[polygon_ccod_defra['current_organisation'].isin(['Forestry Commission', 'Department for Environment, Food and Rural Affairs'])]

# COMMAND ----------

# create buffered version to manage edge effects (FC data seems to have excess boundary a lot of the time)
potential_fc_polygon_ccod_buffered = potential_fc_polygon_ccod
potential_fc_polygon_ccod_buffered.geometry = potential_fc_polygon_ccod_buffered.geometry.buffer(20)

# COMMAND ----------

# MAGIC %md
# MAGIC Could this be done better by just re-removing tilte numbers after sjoin?

# COMMAND ----------

# MAGIC %md
# MAGIC FC data retrieved by Tim

# COMMAND ----------

# import supplied fe polygon data which has been spatially joined to nps-ccod data
fe_title_polygons_with_ccod_data = gpd.read_file(fe_title_polygons_with_ccod_data_path)

# COMMAND ----------

# get total number of titles identifies as possible fe titles
fe_proposed_title_number_counts = fe_title_polygons_with_ccod_data['Title Number'].value_counts()
display(fe_proposed_title_number_counts.to_frame().reset_index())
# this is quite a lot

# COMMAND ----------

# get the count of different proprietors associated with proposed fe titles
fe_proposed_title_name_counts = fe_title_polygons_with_ccod_data['Proprietor Name (1)'].value_counts()
display(fe_proposed_title_name_counts.to_frame().reset_index())
# lots of these don't look like fe proprietor names - possible leasehold/freehold thing?

# COMMAND ----------

# remove 

# COMMAND ----------

display(fe_title_polygons_with_ccod_data)

# COMMAND ----------

# MAGIC %md
# MAGIC FE Land ownership data

# COMMAND ----------

# import supplied fe polygon data
fc_ownership_polygons = gpd.read_file(fe_ownership_polygons_path, layer='LT_FC_OWNERSHIP')
fc_ownership_polygons.geometry = fc_ownership_polygons.geometry.make_valid()
fc_registration_polygons = gpd.read_file(fe_registrations_polygons_path, layer='E_LR_REGISTRATIONS')

# COMMAND ----------

fc_ownership_polygons

# COMMAND ----------

fc_registration_polygons

# COMMAND ----------

fc_registration_polygons['propriet_2'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC Comparison to registrations data (extract of HMLR data) by title number

# COMMAND ----------

# join selected fc-defra ccod data to fc title list (on title number) to enable comparison
fc_ccod_and_fe_registrations_titles = fc_defra_ccod.merge(fc_registration_polygons, how='outer', left_on='Title Number', right_on='title_no')
fc_ccod_and_fe_registrations_titles

# COMMAND ----------

identified_hlmr_not_matching_fc_registrations = fc_ccod_and_fe_registrations_titles[fc_ccod_and_fe_registrations_titles['Title Number'].isna()]
fc_registrations_not_matching_identified_hmlr = fc_ccod_and_fe_registrations_titles[fc_ccod_and_fe_registrations_titles['title_no'].isna()]

# COMMAND ----------


print(f'Number of records in hmlr derived which don`t have matching records in fc registation data: {len(identified_hlmr_not_matching_fc_registrations)}')
print(f'Number of records in hmlr derived which don`t have matching records in fc registation data: {len(fc_registrations_not_matching_identified_hmlr)}')

# COMMAND ----------

# MAGIC %md
# MAGIC Comparison to ownership dataset (managed internall by fe)

# COMMAND ----------

# Geometry type check identifies null geometries
fc_ownership_polygons.geom_type.unique()
# remove records with no geometry
fc_ownership_polygons = fc_ownership_polygons[~fc_ownership_polygons.geometry.is_empty]
# add a small buffer (needed to prevent non-noded intersection linestring error in overlay )
fc_ownership_polygons.geometry = fc_ownership_polygons.geometry.buffer(0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC Get polygon layer of spatial differences between derived hmlr data and FC ownership dataset

# COMMAND ----------

# MAGIC %md
# MAGIC With unbuffered polygon-ccod

# COMMAND ----------

# get polygon layer of spatial differences between datasets - unbuffered polygon-ccod
fc_polygons_not_overlapping_potential_fc_polygon_ccod = fc_ownership_polygons.overlay(potential_fc_polygon_ccod, how='difference', keep_geom_type=False, make_valid=True)
potential_fc_polygon_ccod_not_overlapping_fc_polygons = potential_fc_polygon_ccod.overlay(fc_ownership_polygons, how='difference', keep_geom_type=False, make_valid=True)
# get polygon layer of spatial similarities between datasets
potential_fc_polygon_ccod_overlapping_fc_polygons = potential_fc_polygon_ccod.overlay(fc_ownership_polygons, how='intersection', keep_geom_type=False, make_valid=True)

# COMMAND ----------

print(f'Area covered by FC provided dataset but not hmlr derrived data: {fc_polygons_not_overlapping_potential_fc_polygon_ccod.area.sum()}')
print(f'Area covered by hmlr derrived data but not in FC provided dataset: {potential_fc_polygon_ccod_not_overlapping_fc_polygons.area.sum()}')
print(f'Overlapping area: {potential_fc_polygon_ccod_overlapping_fc_polygons.area.sum()}')

# COMMAND ----------

fc_polygons_not_overlapping_potential_fc_polygon_ccod.to_file(fc_polygons_not_overlapping_potential_fc_polygon_ccod_path, driver='GeoJSON')

# COMMAND ----------

# MAGIC %md
# MAGIC With buffered polygon-ccod - to deal with fc ownership polygons which often seem just slightly too big all the way round

# COMMAND ----------

# get polygon layer of spatial differences between datasets - buffered polygon-ccod
fc_polygons_not_overlapping_potential_fc_polygon_ccod_buffered = fc_ownership_polygons.overlay(potential_fc_polygon_ccod_buffered, how='difference', keep_geom_type=False, make_valid=True)
potential_fc_polygon_ccod_buffered_not_overlapping_fc_polygons = potential_fc_polygon_ccod_buffered.overlay(fc_ownership_polygons, how='difference', keep_geom_type=False, make_valid=True)
# get polygon layer of spatial similarities between datasets
potential_fc_polygon_ccod_buffered_overlapping_fc_polygons = potential_fc_polygon_ccod.overlay(fc_ownership_polygons, how='intersection', keep_geom_type=False, make_valid=True)

# COMMAND ----------

print(f'Area covered by FC provided dataset but not hmlr derrived data: {fc_polygons_not_overlapping_potential_fc_polygon_ccod_buffered.area.sum()}')
print(f'Area covered by hmlr derrived data but not in FC provided dataset: {potential_fc_polygon_ccod_buffered_not_overlapping_fc_polygons.area.sum()}')
print(f'Overlapping area: {potential_fc_polygon_ccod_buffered_overlapping_fc_polygons.area.sum()}')

# COMMAND ----------

fc_polygons_not_overlapping_potential_fc_polygon_ccod_buffered.to_file(fc_polygons_not_overlapping_potential_fc_polygon_ccod_buffered_path, driver='GeoJSON')

# COMMAND ----------

# Find in ccod fc/defra titles which have been identified by my script, but aren't in list from fc. Don't need to join back to unfiltered ccod data here as it should already be present from filtered ccod data.
extra_identified_fc_titles = fc_ccod_and_supplied_titles[fc_ccod_and_supplied_titles['TITLE_NO'].isna()]
extra_identified_fc_titles

# COMMAND ----------

# MAGIC %md
# MAGIC ### EPIMS

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Plotting

# COMMAND ----------

import folium

# COMMAND ----------

fe_title_polygons_half = fe_title_polygons[1:1000]

# COMMAND ----------

m = folium.Map(location=(53, -3), zoom_start= 6, tiles="cartodb positron")
folium.GeoJson(fe_title_polygons_half).add_to(m)

# COMMAND ----------

m
