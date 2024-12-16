# Databricks notebook source
import geopandas as gpd

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

england_boundary = gpd.read_file('/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/LATEST_england_boundary/england_boundary_line.gpkg')

# COMMAND ----------

polygon_ccod = gpd.read_parquet(polygon_ccod_defra_parquet_path)
polygon_ccod_by_organisation = gpd.read_parquet(polygon_ccod_defra_by_organisation_path)
polygon_ccod_by_organisation_tenure = gpd.read_parquet(polygon_ccod_defra_by_organisation_tenure_path)

# COMMAND ----------

polygon_ccod_england = polygon_ccod.overlay(england_boundary, how='intersection')
polygon_ccod_by_organisation_england = polygon_ccod_by_organisation.overlay(england_boundary, how='intersection')
polygon_ccod_by_organisation_tenure_england = polygon_ccod_by_organisation_tenure.overlay(england_boundary, how='intersection')

# COMMAND ----------

polygon_ccod_england.geometry = polygon_ccod_england.geometry.make_valid()
polygon_ccod_by_organisation_england.geometry = polygon_ccod_by_organisation_england.geometry.make_valid()
polygon_ccod_by_organisation_tenure_england.geometry = polygon_ccod_by_organisation_tenure_england.geometry.make_valid()

# COMMAND ----------

polygon_ccod_england = polygon_ccod_england.drop(columns=['unique_id', 'country', 'perimeter', 'shape_area', 'numpoints', 'area'])
polygon_ccod_by_organisation_england = polygon_ccod_by_organisation_england.drop(columns=['unique_id', 'country', 'perimeter', 'shape_area', 'numpoints', 'area'])
polygon_ccod_by_organisation_tenure_england = polygon_ccod_by_organisation_tenure_england.drop(columns=['unique_id', 'country', 'perimeter', 'shape_area', 'numpoints', 'area'])

# COMMAND ----------

polygon_ccod_england.to_parquet('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/polygon_ccod_mod_england.parquet')
polygon_ccod_by_organisation_england.to_parquet('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/polygon_ccod_mod_by_organisation_england.parquet')
polygon_ccod_by_organisation_tenure_england.to_parquet('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/mod_outputs/polygon_ccod_mod_by_organisation_tenure_england.parquet')
