# Databricks notebook source
# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

import geopandas as gpd

# COMMAND ----------

polygon_ccod_defra = gpd.read_parquet(polygon_ccod_defra_path)
polygon_ccod_defra.geometry = polygon_ccod_defra.geometry.make_valid()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create layer of total for each organisation

# COMMAND ----------

polygon_ccod_defra

# COMMAND ----------

polygon_ccod_defra_by_organisation = polygon_ccod_defra.dissolve(by=['current_organisation','land_management_organisation'], as_index=False, aggfunc='count', dropna=False)
polygon_ccod_defra_by_organisation.geometry = polygon_ccod_defra_by_organisation.geometry.make_valid()

# COMMAND ----------

polygon_ccod_defra_by_organisation = polygon_ccod_defra_by_organisation[['current_organisation', 'land_management_organisation', 'geometry']]
polygon_ccod_defra_by_organisation['area_ha'] = polygon_ccod_defra_by_organisation.area/10000

# COMMAND ----------

polygon_ccod_defra_by_organisation

# COMMAND ----------

import numpy as np
polygon_ccod_defra_by_organisation = polygon_ccod_defra_by_organisation.fillna(np.nan)

# COMMAND ----------

polygon_ccod_defra_by_organisation.to_parquet(polygon_ccod_defra_by_organisation_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create total layer for each organisation by tenure

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure = polygon_ccod_defra.dissolve(by=['current_organisation', 'land_management_organisation', 'Tenure'], as_index=False, dropna=False)
polygon_ccod_defra_by_organisation_tenure.geometry = polygon_ccod_defra_by_organisation_tenure.geometry.make_valid()

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure = polygon_ccod_defra_by_organisation_tenure[['current_organisation', 'land_management_organisation', 'Tenure', 'geometry']]
polygon_ccod_defra_by_organisation_tenure['area_ha'] = polygon_ccod_defra_by_organisation_tenure.area/10000

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure

# COMMAND ----------

import numpy as np
polygon_ccod_defra_by_organisation_tenure = polygon_ccod_defra_by_organisation_tenure.fillna(np.nan)

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure.to_parquet(polygon_ccod_defra_by_organisation_tenure_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### GeoJSON output

# COMMAND ----------



# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure = gpd.read_parquet(polygon_ccod_defra_by_organisation_tenure_path)
polygon_ccod_defra_by_organisation_tenure.to_file('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/nps_outputs/polygon_ccod_defra_by_organisation_tenure.geojson')


# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure = gpd.read_file('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/nps_outputs/polygon_ccod_defra_by_organisation_tenure.geojson')

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure
