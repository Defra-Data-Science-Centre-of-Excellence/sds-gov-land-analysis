# Databricks notebook source
# MAGIC %md
# MAGIC ### Data comparison epims
# MAGIC Compare dataset produced using hmlr data to epims, by extracting equivalent data from epims (this requires some translation of organisation names to match format used for HMLR work) and compare, organisation by organisation, both visually and by area figures produced.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install geodatasets

# COMMAND ----------

import geopandas as gpd
import pandas as pd
import geodatasets
import matplotlib.pyplot as plt

# COMMAND ----------

# change the pandas float display for easy reading
pd.options.display.float_format = '{:.2f}'.format

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./constants

# COMMAND ----------

alb_found_names_translation_dict.update({'Department for Environment, Food and Rural Affairs': None})
organisations_of_interest = alb_found_names_translation_dict.keys()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare empis data

# COMMAND ----------

# Read in the entire epims datasets - this doesn't have any department information only an epims holding number, objectid and geometry, so need to join up to the point dataset for that info
epims = gpd.read_file(epims_path)
display(epims)

# COMMAND ----------

# Read in point dataset - filter for defra points only - forestry commission identified seperately
epims_point = pd.read_csv(epims_point_path)
epims_point_defra = epims_point[epims_point['Department']=='Department for Environment, Food and Rural Affairs']
epims_point_fc = epims_point[epims_point['PropertyCentre'].isin(['FC - FORESTRY COMMISSION â€“ LAND REGISTER', 'FC - FORESTRY COMMISSION ENGLAND', 'FC - FORESTRY COMMISSION ENGLAND (OPERATIONAL BUILDINGS)'])]
epims_point_defra_alb = pd.concat([epims_point_defra, epims_point_fc])

# COMMAND ----------

display(pd.DataFrame(epims_point['PropertyCentre'].unique()))

# COMMAND ----------

# Join epims polygons to defra points by holding ID, this gives us the attribute info for polygons
epims = epims.merge(epims_point_defra_alb, left_on='ePIMSHoldi', right_on='ePIMSHoldingId')
epims = epims.explode()
epims.geom_type.unique()
epims['df'] = 'epims'

# COMMAND ----------

#validate geometries
epims['geometry'] = epims.make_valid()
invalid_epims = epims.loc[~epims.geometry.is_valid]
display(invalid_epims)

# COMMAND ----------

epims

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Translate PropertyCentre to standard DEFRA organsiation name

# COMMAND ----------

# keys are epims property centres and values are defra organisation name in the standardised text used for our generated dataset
organisation_translation_dict = {
       'DEFRA - ENVIRONMENT AGENCY - CORPORATE ESTATE':'Environment Agency',
       'DEFRA - ENVIRONMENT AGENCY - FUNCTIONAL ESTATE': 'Environment Agency',
       'DEFRA - DEPARTMENT FOR ENVIRONMENT, FOOD AND RURAL AFFAIRS - SPECIALIST ESTATE': 'Department for Environment, Food and Rural Affairs',
       'DEFRA - DEPARTMENT FOR ENVIRONMENT, FOOD AND RURAL AFFAIRS - CORPORATE ESTATE': 'Department for Environment, Food and Rural Affairs',
       'DEFRA - NATURAL ENGLAND': 'Natural England',
       'DEFRA - NATIONAL FOREST COMPANY OPERATIONAL': 'National Forest Company',
       'DEFRA - ROYAL BOTANIC GARDENS KEW': 'Royal Botanic Gardens Kew',
       'DEFRA - AGRICULTURE & HORTICULTURE DEVELOPMENT BOARD': 'Agriculture and Horticulture Development Board',
       'DEFRA - SEA FISH INDUSTRY AUTHORITY':'Seafish',
       'FC - FORESTRY COMMISSION â€“ LAND REGISTER': 'Forestry Commission',
       'FC - FORESTRY COMMISSION ENGLAND': 'Forestry Commission',
       'FC - FORESTRY COMMISSION ENGLAND (OPERATIONAL BUILDINGS)': 'Forestry Commission',
       }

# COMMAND ----------

# add new translated organtisation column which can be matched to current organisation field in polygon-ccod data
epims['organisation'] = epims['PropertyCentre']
epims['organisation'] = epims['organisation'].map(organisation_translation_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summarise epims data

# COMMAND ----------

area_df = pd.DataFrame(columns=['organisation', 'total_area', 'land_area', 'land_buildings_area', 'overlap_freehold_leasehold'])
#For each organisation of interest
for organisation in organisations_of_interest:
    organisation_polygon_ccod = epims[epims['organisation'] == organisation]
    # get total area
    organisation_polygon_ccod.geometry = organisation_polygon_ccod.geometry.make_valid()
    total_area = organisation_polygon_ccod.dissolve().area.sum()/10000
    #dissolve by freehold/leasehold
    holding_dissolved_polygon_ccod = organisation_polygon_ccod.dissolve(by='HoldingTypeDescription', as_index=False)
    land = holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['HoldingTypeDescription'] == 'Land Only']
    land_area = land.area.sum()/10000
    land_buildings = holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['HoldingTypeDescription'] == 'Land & Buildings']
    land_buildings_area = land_buildings.area.sum()/10000
    # sense check that freehold and leasehold add to total area
    #if land_area + land_buildings_area != total_area:
    #print(f'Sense check: freehold area ({freehold_area}) and leasehold area ({leasehold_area}) do not add to total area ({total_area})')
    #sense_check = f'Difference of: {total_area - freehold_area - leasehold_area}'
    #else:
    #    sense_check = 0
    overlap_land_land_buildings = gpd.overlay(land, land_buildings, how='intersection', make_valid=True)
    overlap_land_land_buildings_area = overlap_land_land_buildings.area.sum()/10000
    # add values to dataframe
    df_row = pd.DataFrame(data={'organisation': organisation, 'total_area': total_area, 'land_area': land_area, 'land_buildings_area': land_buildings_area, 'overlap_land_land_buildings_leasehold': overlap_land_land_buildings_area}, index=[0])
    area_df = pd.concat([area_df, df_row], ignore_index=True)
display(area_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### output area summary df

# COMMAND ----------

area_df.to_csv(csv_area_df_epims_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare identified defra polygon-ccod data

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

# MAGIC %md
# MAGIC #### Look at Kew gardens visually, as this has the largest persentage disrepancy

# COMMAND ----------

polygon_ccod_rbk = polygon_ccod_defra[polygon_ccod_defra['current_organisation']=='Royal Botanic Gardens Kew']
epims_rbk = epims[epims['organisation']=='Royal Botanic Gardens Kew']

# COMMAND ----------

poylgon_ccod_area = gpd.read_parquet(f'{nps_by_study_area_directory_path}/botanic_gardens_nps_ccod.parquet')

# COMMAND ----------

polygon_ccod_area_epims_kew = gpd.overlay(poylgon_ccod_area, epims_rbk, how='intersection')

# COMMAND ----------

polygon_ccod_area_epims_kew['Proprietor Name (1)'].unique()

# COMMAND ----------

polygon_ccod_area_epims_kew[polygon_ccod_area_epims_kew['Proprietor Name (1)']=='ARDINGLY ACTIVITY CENTRE LIMITED'].explore()

# COMMAND ----------

fig, ax = plt.subplots(figsize=(9, 20))
ax1 = polygon_ccod_area_epims_kew.plot(column='Proprietor Name (1)', legend=True, figsize=(50, 30), ax=ax, cmap='tab20')
ax2 = epims_rbk.boundary.plot(color='black', ax=ax)
ax.set_xticks([])
ax.set_yticks([])
leg = ax1.get_legend()
leg.get_frame().set_alpha(0)
#leg.set_loc('upper left')
leg.frameon = False
leg.set_bbox_to_anchor((0., 0., 2, 1))
plt.show()

# COMMAND ----------

epims_rbk.explore()

# COMMAND ----------

polygon_ccod_rbk.explore()

# COMMAND ----------

polygon_ccod_rbk

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compare defra polygon-ccod and epims

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Dissolved

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

# MAGIC %md
# MAGIC ##### Undissolved

# COMMAND ----------

# Comparison of undissolved datasets
polygon_ccod_defra_with_no_overlapping_epims_undissolved = polygon_ccod_defra.overlay(epims, how='difference', keep_geom_type=False, make_valid=True)
epims_with_no_overlapping_polygon_ccod_defra_undissolved = epims.overlay(polygon_ccod_defra, how='difference', keep_geom_type=False, make_valid=True)

# COMMAND ----------

epims_with_no_overlapping_polygon_ccod_defra_undissolved.to_parquet(undissolved_epims_with_no_overlapping_polygon_ccod_defra_path)

# COMMAND ----------

# get count of polygons present in epims but not hmlr derrived data
epims_with_no_overlapping_polygon_ccod_defra_undissolved['PropertyCentre'].value_counts()
#Dissolve on property centre and get total area for each 
epims_with_no_overlapping_polygon_ccod_defra_by_propertycentre = epims_with_no_overlapping_polygon_ccod_defra_undissolved.dissolve(by='PropertyCentre')
epims_with_no_overlapping_polygon_ccod_defra_by_propertycentre['area_m2'] = epims_with_no_overlapping_polygon_ccod_defra_by_propertycentre.area
display(epims_with_no_overlapping_polygon_ccod_defra_by_propertycentre)

# Dissolve on translated organisation field and get total area for each 
epims_with_no_overlapping_polygon_ccod_defra_by_organisation = epims_with_no_overlapping_polygon_ccod_defra_undissolved.dissolve(by='organisation')
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
