# Databricks notebook source
# MAGIC %md
# MAGIC ### Paths
# MAGIC Script containing paths used in other scripts across repo. Designed to be imported/run from other scripts as needed.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Input data

# COMMAND ----------

# raw inputs
# UK Company Proprietorship dataset - links title numbers and companies (including public sector, but not including charities or private ownership) which own the title
ccod_path = '/dbfs/mnt/lab/restricted/ESD-Project/source_data_gov_hm_land_registry/dataset_use_land_and_property_data/format_CSV_use_land_and_property_data/LATEST_use_land_and_property_data/CCOD_FULL_2024_01.csv'
# National polygon dataset - links polygon geometries with associated title numbers
national_polygon_paths = [
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_0.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_1.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_2.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_3.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_4.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_5.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_6.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_7.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_8.shp',
    '/dbfs/mnt/base/restricted/source_data_gov_hm_land_registry/dataset_nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/LR_POLY_FULL_MAY_2024_9.shp',
]

# created inputs
# Parquet version of the national polygon dataset
national_polygon_parquet_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/national_polygon_dataset.parquet'
# Combined dataset created using the National polgon dataset and ccod - links polygon geometries to title numbers and company which owns the title/polygon.
polygon_ccod_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/hmlr_data/polygon_ccod.parquet'

# COMMAND ----------

# National polygon data by study area for manual inspection
study_area_directory_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/study_areas'
nps_by_study_area_directory_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/nps_by_study_area'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Non-spatial

# COMMAND ----------

# Proprietor names identified as associated with DEFRA or its ALBs in the CCOD data
defra_names_csv_path = "/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/ccod_outputs/mod_found_names.csv"

# COMMAND ----------

# CCOD records (including title  numbers) identified as aosciated with DEFRA or its ALBs
ccod_defra_and_alb_path = "/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/ccod_outputs/ccod_of_interest_mod_and_albs.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spatial

# COMMAND ----------

ccod_defra_path = ""
# Polygon dataset for the land owned by DEFRA and its ALBs - one record per title
polygon_ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/polygon_ccod_mod.geojson'
polygon_ccod_defra_parquet_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/polygon_ccod_mod.parquet'
# Polygon dataset for the land owned by DEFRA and its ALBs - dissolved so there is a single flat multipolygon for each organisation
polygon_ccod_defra_by_organisation_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/polygon_ccod_mod_by_organisation.parquet'
# Polygon dataset for the land owned by DEFRA and its ALBs - dissolved so there is a single flat multipolygon for each organisation-tenure combination
polygon_ccod_defra_by_organisation_tenure_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/polygon_ccod_mod_by_organisation_tenure.parquet'

# COMMAND ----------

polygon_ccod_fe_unfiltered_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/polygon_ccod_fe_unfiltered.geojson'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Area summary

# COMMAND ----------

# Summary of DEFRA and ALB land ownership by area
csv_area_df_polygon_ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/area_by_organisation_from_hmlr_data.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation data

# COMMAND ----------

# Land identified as belonging to both a DEFRA/DEFRA ALB organisation and a non-DEFRA organisation
overlap_with_non_defra_estate_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/validation/overlap_with_non_mod_estate.parquet'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Existing data for comparison

# COMMAND ----------

# EPIMS data aqcuired from the cabinet office

epims_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/epims/20230628_ePIMS_holdings_boundaries_GB.geojson'

epims_point_path = "/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/epims/20230628_ePIMS_holdings_non_sensitive.csv"

epims_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/epims/defra/DEFRAALL.shp'

csv_area_df_epims_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/epims/area_by_organsisation_from_epims.csv'

# COMMAND ----------

# EA data for validation
ea_titles_path = "/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/EA_Freehold_titles.xlsx"

# COMMAND ----------

# NE data for validation
ne_titles_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/DEFRA_NE_Ownership.csv'
ne_title_polygons_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/OP70965_Ownership_Polygons.shp'

# COMMAND ----------

# Forestry commission data for validation
fe_polygons_path = "/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/forestry_commission/National_Forest_Estate_Subcompartments_England_2019.shp"

#
fe_title_polygons_with_ccod_data_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/forestry_commission/fe_ownership_parcels_intersecting_nps_polygon_ccod.geojson'

# Ownership polygon data recieved from Peter Burnett
fe_ownership_polygons_path = '/dbfs/FileStore/jazzelliott/Ownership.gdb'

fe_registrations_polygons_path = '/dbfs/FileStore/jazzelliott/Registrations.gdb'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output comparison files

# COMMAND ----------

# outputs produced for data comparison to EPIMS, identifying areas of difference and summarising information on ownership in these areas
#epims_defra_polygon_ccod_comparison_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/epims_defra_polygon_ccod_comparison.geojson'
epims_defra_with_no_overlapping_polygon_ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/epims_defra_with_no_overlapping_polygon_ccod_defra.geojson'
polygon_ccod_defra_with_no_overlapping_epims_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/polygon_ccod_defra_with_no_overlapping_epims_defra.geojson'

epims_with_no_overlapping_polygon_ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/epims_with_no_overlapping_polygon_ccod_defra_2.geojson'
polygon_ccod_defra_with_no_overlapping_epims_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/polygon_ccod_defra_with_no_overlapping_epims_2.geojson'

undissolved_epims_with_no_overlapping_polygon_ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/undissolved_epims_with_no_overlapping_polygon_ccod_defra_2.parquet'

hmlr_epims_gaps_ccod_info_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_epims_gaps_ccod_info.geojson'

hmlr_epims_buffer_minus1_gaps_ccod_info_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_epims_buffer_minus1_gaps_ccod_info.geojson'

hmlr_epims_buffer_minus05_gaps_ccod_info_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_epims_buffer_minus05_gaps_ccod_info.geojson'

# COMMAND ----------

# outputs produced for data comparison to FE data, identifying areas of difference and summarising information on ownership in these areas

fc_polygons_not_overlapping_potential_fc_polygon_ccod_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/fc_ownership_polygons_not_overlapping_hmlr_fc_polygons.geojson'

fc_polygons_not_overlapping_potential_fc_polygon_ccod_buffered_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/fc_ownership_polygons_not_overlapping_20m_buffered_hmlr_fc_polygons.geojson'

hmlr_fe_gaps_ccod_info_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_fe_gaps_ccod_info.geojson'

hmlr_fe_buffer_minus1_gaps_ccod_info_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_fe_buffer_minus1_gaps_ccod_info.geojson'

hmlr_fe_buffer_minus05_gaps_ccod_info_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/comparison_outputs/hmlr_fe_buffer_minus05_gaps_ccod_info_2.geojson'
