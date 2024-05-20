# Databricks notebook source
# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

# import packages
import geopandas as gpd
import pandas as pd

# COMMAND ----------

# change the pandas float display for easy reading
pd.options.display.float_format = '{:.2f}'.format

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define functions

# COMMAND ----------

def output_gap_attributes_from_nps_ccod(polygon_gaps, output_path):
    '''
    Gets attribute information from ccod-nps data for polygons layer passed to it. Use to get information in gaps in the identified ccod_polygon of interest layer.
    
    Parameters:
        polygon_gaps (GeoDataFrame): polyon layer for which ccod info is needed
        output_path (str): filepath to save the output polyon layer (with added ccod info) to
    '''
    # get nps-ccod records overlapping with fe polygons
    overlap_polygon_gaps_polygon_ccod = gpd.sjoin(polygon_gaps, polygon_ccod, how='left', lsuffix='_fe', rsuffix='_ccod')
    # write fe polygons with associated ccod info to geojson
    overlap_polygon_gaps_polygon_ccod.to_file(output_path, driver='GeoJSON')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read in NPS-CCOD dataset

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
        "Proprietor Name (4)",
        ]
)

# COMMAND ----------

# import unfiltered national polygon dataset - usually takes about an hour - faster if parquet?
national_polygon_dfs = []
for national_polygon_path in national_polygon_paths:
    national_polygon_df = gpd.read_file(national_polygon_path)#, where = f'TITLE_NO IN {title_numbers_of_interest_sql_string}')
    national_polygon_dfs.append(national_polygon_df)
    print(f'loaded into dataframe: {national_polygon_path}')
national_polygon = pd.concat(national_polygon_dfs, ignore_index=True)

# COMMAND ----------

national_polygon.to_parquet(national_polygon_parquet_path)

# COMMAND ----------

national_polygon_paquet_test = pd.read_parquet(national_polygon_parquet_path)

# COMMAND ----------

# join ccod data to polygons
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output datasets for gaps which have ccod data added

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get ccod info for FE gaps

# COMMAND ----------

# get info for fe gaps
hmlr_fe_gaps = gpd.read_file(fc_polygons_not_overlapping_potential_fc_polygon_ccod_buffered_path)


# COMMAND ----------

hmlr_fe_gaps.area.sum()

# COMMAND ----------

hmlr_fe_gaps_shrunk = hmlr_fe_gaps
hmlr_fe_gaps_shrunk['geometry'] = hmlr_fe_gaps_shrunk['geometry'].buffer(-0.5)

# COMMAND ----------

output_gap_attributes_from_nps_ccod(hmlr_fe_gaps_shrunk, hmlr_fe_buffer_minus05_gaps_ccod_info_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get CCOD info for EPIMS gaps

# COMMAND ----------

# get into for gaps identified by epims
hmlr_epims_gaps = gpd.read_file(epims_with_no_overlapping_polygon_ccod_defra_path)

# COMMAND ----------

hmlr_epims_gaps

# COMMAND ----------

hmlr_epims_gaps_shrunk = hmlr_epims_gaps
hmlr_epims_gaps_shrunk['geometry'] = hmlr_epims_gaps_shrunk['geometry'].buffer(-0.5)

# COMMAND ----------

hmlr_epims_gaps_shrunk.geometry = hmlr_epims_gaps_shrunk.geometry.make_valid()

# COMMAND ----------

output_gap_attributes_from_nps_ccod(hmlr_epims_gaps_shrunk, hmlr_epims_buffer_minus1_gaps_ccod_info_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Investigate found data for gaps

# COMMAND ----------

# MAGIC %md
# MAGIC FE

# COMMAND ----------

hmlr_fe_gap_info = gpd.read_file(hmlr_fe_gaps_ccod_info_path)

# COMMAND ----------

hmlr_fe_gap_info_by_organisation = hmlr_fe_gap_info.dissolve(by='Proprietor Name (1)')

# COMMAND ----------

hmlr_fe_gap_info_by_organisation['area'] = hmlr_fe_gap_info_by_organisation.area

# COMMAND ----------

hmlr_fe_gap_info_by_organisation_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_fe_gap_info_by_proprietor.geojson'
hmlr_fe_gap_info_by_organisation.to_file(hmlr_fe_gap_info_by_organisation_path, driver='GeoJSON')

# COMMAND ----------

hmlr_fe_gap_info_by_organisation_df = hmlr_fe_gap_info_by_organisation.drop(columns=['geometry']).reset_index()
display(hmlr_fe_gap_info_by_organisation_df.sort_values(by='area', ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC EPIMS

# COMMAND ----------

hmlr_epims_gap_info = gpd.read_file(hmlr_epims_gaps_ccod_info_path)

# COMMAND ----------

hmlr_epims_gap_info_by_proprietor = hmlr_epims_gap_info.dissolve(by='Proprietor Name (1)')

# COMMAND ----------

hmlr_epims_gap_info_by_proprietor_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_epims_gap_info_by_proprietor.geojson'
hmlr_epims_gap_info_by_proprietor.to_file(hmlr_epims_gap_info_by_proprietor_path, driver='GeoJSON')

# COMMAND ----------

hmlr_epims_gap_info_by_proprietor

# COMMAND ----------

hmlr_epims_gap_info_by_proprietor['area'] = hmlr_epims_gap_info_by_proprietor.area

# COMMAND ----------

hmlr_epims_gap_info_by_proprietor_df = hmlr_epims_gap_info_by_proprietor.drop(columns=['geometry']).reset_index()
display(hmlr_epims_gap_info_by_proprietor_df.sort_values(by='area', ascending=False))
