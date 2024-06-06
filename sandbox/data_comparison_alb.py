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

# unfilitered polygon ccod data
polygon_ccod = gpd.read_parquet(polygon_ccod_path)

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
ea_ccod = polygon_ccod_defra[polygon_ccod_defra['current_organisation'] == 'Environment Agency']

# COMMAND ----------

# join selected ea ccod data to ea title list (on title number) to enable comparison
ea_ccod_and_supplied_titles = ea_ccod.merge(ea_titles, how='outer', left_on='Title Number', right_on='Title', suffixes=('_filtered_ccod','_ea'))

# COMMAND ----------

# Find ea titles which have not been identified by my script, by selecting for null Title Number (from selected ea ccod table) field. Then join to unfiltered ccod data to get attribute info.
unidentified_ea_titles = ea_ccod_and_supplied_titles[ea_ccod_and_supplied_titles['Title Number'].isna()]
unidentified_ea_titles_ccod = ccod.merge(unidentified_ea_titles, how='right', left_on='Title Number', right_on='Title', suffixes=('_unfiltered_ccod','_comparison_table'))
unidentified_ea_titles_ccod

# COMMAND ----------

# to get an area for this discrepancy we need to use the unfiltered polygon_ccod data for geometries
unidentified_ea_titles_ccod_geometries = polygon_ccod.merge(unidentified_ea_titles, how='right', left_on='Title Number', right_on='Title', suffixes=('_unfiltered_polygon_ccod','_comparison_table'))
unidentified_geometries = unidentified_ea_titles_ccod_geometries[unidentified_ea_titles_ccod_geometries['geometry_unfiltered_polygon_ccod'].notnull()]
unidentified_geometries = gpd.GeoDataFrame(unidentified_geometries, geometry=unidentified_geometries['geometry_unfiltered_polygon_ccod'])
# print the number of titles just to check they're all pulling through
print(f"Number of unidientified titles associated with records in HMLR National Polygon dataset: {len(unidentified_geometries['TITLE_NO_unfiltered_polygon_ccod'].unique())}")
# print the area (needs to be of dissolved geometries)
unidentified_geometries_area = unidentified_geometries.dissolve().area.sum()
print(f'Unidentified area associated with these titles: {unidentified_geometries_area/10000} hectares')

# COMMAND ----------

# get proprietor name list and count of associated titles from ccod data - only 178, which leaves 50 titles which must be null in ccod
display(pd.DataFrame(unidentified_ea_titles_ccod['Proprietor Name (1)_unfiltered_ccod'].value_counts()).reset_index())

# COMMAND ----------

# lots of records from UNITED UTILITIES WATER LIMITED in the EA list, let's have a look at UNITED UTILIIES WATER LIMITED in the unfilitered ccod dataset and see if they have any more titles
uuwl_ccod = ccod[ccod['Proprietor Name (1)'] == 'UNITED UTILITIES WATER LIMITED']
display(uuwl_ccod)
# Lots of records under UUWL not identified as EA. No differnetiation of these by proprietor name, is there any differentiation by address?
uuwl_ccod['Proprietor (1) Address (1)'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC Need to get the area of these?

# COMMAND ----------

ea = polygon_ccod_defra[polygon_ccod_defra['current_organisation']=='Environment Agency']
ea_leasehold = ea[ea['Tenure']=='Leasehold']
ea_leasehold

# COMMAND ----------

# Find in ccod ea titles which have been identified by my script, but aren't in list from ea. Don't need to join back to unfiltered ccod data here as it should already be present from filtered ccod data.
extra_identified_ea_titles = ea_ccod_and_supplied_titles[ea_ccod_and_supplied_titles['Title'].isna()]
# Remove leasehold as the EA only provided Freehold titles in their list
extra_identified_ea_freehold_titles = extra_identified_ea_titles[extra_identified_ea_titles['Tenure_filtered_ccod']=='Freehold']
#display(extra_identified_ea_titles)
extra_identified_ea_freehold_titles = extra_identified_ea_freehold_titles.dissolve(by='TITLE_NO')
display(pd.DataFrame(extra_identified_ea_freehold_titles['Proprietor Name (1)'].value_counts()).reset_index())
extra_identified_ea_titles_area = extra_identified_ea_freehold_titles.dissolve().area.sum()/10000
print(f'Area identified in EA land parcel dataset not identified by EA freehold title list: {extra_identified_ea_titles_area} hectares')

# let's get the leasehold area as well just for reference
extra_identified_ea_leasehold_titles = extra_identified_ea_titles[extra_identified_ea_titles['Tenure_filtered_ccod']=='Leasehold']
extra_identified_ea_leasehold__titles = extra_identified_ea_leasehold_titles.dissolve(by='TITLE_NO')
display(pd.DataFrame(extra_identified_ea_leasehold_titles['Proprietor Name (1)'].value_counts()).reset_index())
extra_identified_ea_leasehold_titles_area = extra_identified_ea_leasehold_titles.dissolve().area.sum()/10000
print(f'Leasehold area identified in EA land parcel dataset not identified by EA freehold title list: {extra_identified_ea_leasehold_titles_area} hectares')

# COMMAND ----------

# MAGIC %md
# MAGIC #### NE Data Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import data

# COMMAND ----------

# Import title list provided by NE
ne_titles = pd.read_csv(ne_titles_path)
# Rename Title NUmber field to 'Title' field (to match EA and help column tracking/maintenance)
ne_titles = ne_titles.rename(columns={'Title Number': 'Title'})
display(ne_titles)

# COMMAND ----------

# import ne ownership polygons
ne_ownership_polygons = gpd.read_file(ne_title_polygons_path)

# COMMAND ----------

# Get ne records from ccod data selected by my methods
ne_ccod = polygon_ccod_defra[polygon_ccod_defra['current_organisation'] == 'Natural England']

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comparison to title list

# COMMAND ----------

# join selected ne ccod data to ne title list (on title number) to enable comparison
ne_ccod_and_supplied_titles = ne_ccod.merge(ne_titles, how='outer', left_on='Title Number', right_on='Title')

# COMMAND ----------

ccod_of_interest[ccod_of_interest['Title Number'] == 'ESX299765']

# COMMAND ----------

polygon_ccod[polygon_ccod['Title Number'] == 'SL271832']

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Get titles not identified

# COMMAND ----------

# Find ne titles which have not been identified by my script, by selecting for null Title Number (from selected ne ccod table) field. Then join to unfiltered ccod data to get attribute info.
unidentified_ne_titles = ne_ccod_and_supplied_titles[ne_ccod_and_supplied_titles['Title Number'].isna()]
unidentified_ne_titles_ccod = ccod.merge(unidentified_ne_titles, how='right', left_on='Title Number', right_on='Title')
display(pd.DataFrame(unidentified_ne_titles['Title']).reset_index())

# Joining back to unfiltered ccod data produces empty table error, suggesting title numbers aren't present in unfiltered ccod - need to check this


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Get extra identified titles

# COMMAND ----------

# Find in ccod ne titles which have been identified by my script, but aren't in list from ne. Don't need to join back to unfiltered ccod data here as it should already be present from filtered ccod data.
extra_identified_ne_polygons = ne_ccod_and_supplied_titles[ne_ccod_and_supplied_titles['Title'].isna()]
extra_identified_ne_titles = extra_identified_ne_polygons.dissolve(by='TITLE_NO')
display(extra_identified_ne_titles)

# COMMAND ----------

display(extra_identified_ne_titles['Proprietor Name (1)'].value_counts())

# COMMAND ----------

# get area of extra titles
extra_identified_ne_titles_area = extra_identified_ne_titles.dissolve().area.sum()/10000
print(f'Area of parcels identified as NE by NE land parcel dataset, but not by NE supplid title list: {extra_identified_ne_titles_area} hectares')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comparison to ownership polygons

# COMMAND ----------

# have a look at the data - has title numbers in, so may not add value to compare spatially
ne_ownership_polygons

# COMMAND ----------

# check if titles all in both datasets
ne_ownership_vs_titles = ne_ownership_polygons.merge(ne_titles, how='outer', left_on='TITLE_NO', right_on='Title')
#ne_ownership_vs_titles = ne_ownership_vs_titles.dissolve(by='TITLE_NO')
ne_ownership_not_in_titles = ne_ownership_vs_titles[ne_ownership_vs_titles['Title'].isna()]
ne_titles_not_in_ownership = ne_ownership_vs_titles[ne_ownership_vs_titles['TITLE_NO'].isna()]
display(ne_ownership_not_in_titles)
display(ne_titles_not_in_ownership)

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
fc_polygon_ccod = polygon_ccod_defra[polygon_ccod_defra['current_organisation'] == 'Forestry Commission']
display(fc_ccod)

# COMMAND ----------

potential_fc_polygon_ccod = polygon_ccod_defra[polygon_ccod_defra['current_organisation'].isin(['Forestry Commission', 'Department for Environment, Food and Rural Affairs'])]

# COMMAND ----------

# create buffered version to manage edge effects (FC data seems to have excess boundary a lot of the time) - would it be better to remove after sjoin by title number?
potential_fc_polygon_ccod_buffered = potential_fc_polygon_ccod
potential_fc_polygon_ccod_buffered.geometry = potential_fc_polygon_ccod_buffered.geometry.buffer(-2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### FE Land ownership data

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

# MAGIC %md
# MAGIC ##### Get ccod info on all fc registrations to help delineate defra and fe parcels

# COMMAND ----------

# get unique list of titles from fc data and filter ccod using this
fc_registration_titles = fc_registration_polygons['title_no'].unique()
fc_ccod = ccod[ccod['Title Number'].isin(fc_registration_titles)]
# have a look at proprietor info fields to look for patterns
display(fc_ccod['Proprietor Name (1)'].value_counts())


# COMMAND ----------

# investigating proprietor 2 field for usefulness in delineating fc records, but it's not this populated in hmlr data
fc_registration_polygons['propriet_2'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC Comparison to registrations data (extract of HMLR data) by title number

# COMMAND ----------

# join selected fc-defra ccod data to fc title list (on title number) to enable comparison
fc_ccod_and_fe_registrations_titles = fc_polygon_ccod.merge(fc_registration_polygons, how='outer', left_on='Title Number', right_on='title_no')
fc_ccod_and_fe_registrations_titles

# COMMAND ----------

identified_hlmr_not_matching_fc_registrations = fc_ccod_and_fe_registrations_titles[fc_ccod_and_fe_registrations_titles['title_no'].isna()]
fc_registrations_not_matching_identified_hmlr = fc_ccod_and_fe_registrations_titles[fc_ccod_and_fe_registrations_titles['Title Number'].isna()]

# COMMAND ----------


print(f'Number of records in hmlr derived which don`t have matching records in fc registation data: {len(identified_hlmr_not_matching_fc_registrations)}')
print(f'Number of records in fc registation data which don`t have matching records in hmlr derived data: {len(fc_registrations_not_matching_identified_hmlr)}')

# COMMAND ----------

defra_entangled = fc_registrations_not_matching_identified_hmlr[fc_registrations_not_matching_identified_hmlr['title_no'].isin(polygon_ccod_defra['Title Number'])]
defra_entangled.dissolve().area.sum()/10000

# COMMAND ----------

fc_registrations_not_matching_identified_hmlr['t']

# COMMAND ----------

# get area for 
identified_hlmr_not_matching_fc_registrations = gpd.GeoDataFrame(identified_hlmr_not_matching_fc_registrations, geometry=identified_hlmr_not_matching_fc_registrations.geometry_x)
print(f'Area identified by HMLR derived data, not in FE polygons:{identified_hlmr_not_matching_fc_registrations.dissolve().area.sum()/10000}')
# get area for 
fc_registrations_not_matching_identified_hmlr = gpd.GeoDataFrame(fc_registrations_not_matching_identified_hmlr, geometry=fc_registrations_not_matching_identified_hmlr.geometry_y)
print(f'Area identified by FE polygons, not in HMLR derived data:{fc_registrations_not_matching_identified_hmlr.dissolve().area.sum()/10000}')
fc_registrations_not_matching_identified_hmlr.dissolve().area.sum()/10000

# COMMAND ----------

fc_registrations_not_matching_identified_hmlr['Proprietor Name (1)'].unique()

# COMMAND ----------

fc_registrations_not_matching_identified_hmlr['Proprietor (1) Address (1)'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Freehold area comparison

# COMMAND ----------

fc_registration_polygons_freehold = fc_registration_polygons[fc_registration_polygons['tenure']=='Freehold']
fc_registration_polygons_freehold_area = fc_registration_polygons_freehold.dissolve().area.sum()/10000
fc_registration_polygons_freehold_area

# COMMAND ----------

fc_polygon_ccod_freehold = fc_polygon_ccod[fc_polygon_ccod['Tenure']=='Freehold']
fc_polygon_ccod_freehold_area = fc_polygon_ccod_freehold.dissolve().area.sum()/10000
fc_polygon_ccod_freehold_area

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Leasehold area comparison

# COMMAND ----------

fc_registration_polygons_leasehold = fc_registration_polygons[fc_registration_polygons['tenure']=='Leasehold']
fc_registration_polygons_leasehold_area = fc_registration_polygons_leasehold.dissolve().area.sum()/10000
fc_registration_polygons_leasehold_area

# COMMAND ----------

fc_polygon_ccod_leasehold = fc_polygon_ccod[fc_polygon_ccod['Tenure']=='Leasehold']
fc_polygon_ccod_leasehold_area = fc_polygon_ccod_leasehold.dissolve().area.sum()/10000
fc_polygon_ccod_leasehold_area

# COMMAND ----------

# compile to dataframe for export
area_df = pd.DataFrame(data={'HMLR derived (Ha)': [fc_polygon_ccod_freehold_area, fc_polygon_ccod_leasehold_area], 'FE registration area (Ha)': [fc_registration_polygons_freehold_area, fc_registration_polygons_leasehold_area], 'Difference (Ha)': [fc_polygon_ccod_freehold_area-fc_registration_polygons_freehold_area, fc_polygon_ccod_leasehold_area-fc_registration_polygons_leasehold_area]}, index=['Freehold', 'Leasehold'])
display(area_df.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comparison to ownership dataset (managed internall by fe)

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
