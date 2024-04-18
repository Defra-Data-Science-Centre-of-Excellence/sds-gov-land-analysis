# Databricks notebook source
# MAGIC %md
# MAGIC #### Input data

# COMMAND ----------

ccod_path = '/dbfs/mnt/lab/restricted/ESD-Project/source_data_gov_hm_land_registry/dataset_use_land_and_property_data/format_CSV_use_land_and_property_data/LATEST_use_land_and_property_data/CCOD_FULL_2024_01.csv'

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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output data

# COMMAND ----------

ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/ccod_defra.csv'
polygon_ccod_defra_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/polygon_ccod_defra.csv'

# COMMAND ----------

polygon_ccod_fe_unfiltered_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/polygon_ccod_fe_unfiltered.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Existing data for comparison

# COMMAND ----------

# EA
ea_titles_path = "/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/EA_Freehold_titles.xlsx"

# COMMAND ----------

# NE
ne_titles_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/DEFRA_NE_Ownership.csv'
ne_title_polygons_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/OP70965_Ownership_Polygons.shp'

# COMMAND ----------

# Forestry commission
fe_polygons_path = "/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/forestry_commission/National_Forest_Estate_Subcompartments_England_2019.shp"

#
fe_title_polygons_with_ccod_data_path = '/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/existing_data_for_comparison/forestry_commission/fe_ownership_parcels_intersecting_nps_polygon_ccod.geojson'
