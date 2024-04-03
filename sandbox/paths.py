# Databricks notebook source
# MAGIC %md
# MAGIC #### Input data

# COMMAND ----------

ccod_path = '/dbfs/mnt/lab/restricted/ESD-Project/source_data_gov_hm_land_registry/dataset_use_land_and_property_data/format_CSV_use_land_and_property_data/LATEST_use_land_and_property_data/CCOD_FULL_2024_01.csv'


# COMMAND ----------

# MAGIC %md
# MAGIC #### Output data

# COMMAND ----------



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
