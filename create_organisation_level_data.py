# Databricks notebook source
# MAGIC %md
# MAGIC ### Create organisation level data
# MAGIC This script will create a flat version of your filtered dataset, creating a single flat multipolygon for each organisation and organisation-tenure combination.<br>
# MAGIC Note: there may still be overlaps in the polygons between organisations after running this, but there will not be overlaps within an organisation (or organisation-tenure combo).<br>
# MAGIC This script produces a spatial data product, for sharing with GIS users, and aid in display of the data.<br>
# MAGIC <b> Prerequisites: </b> Before running this step, ensure you have a filtered version of the national polygon sdataset-ccod for your organisations of intereset (ie. you have run 'identify_title_numbers' and 'identify_land_parcels').<br>
# MAGIC <b> Next steps: </b> Once you've produced this data it can be exported to a geopackage file and downlodad off the databricks platform using the 'output_export' script.

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

import geopandas as gpd

# COMMAND ----------

polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)
polygon_ccod_defra.geometry = polygon_ccod_defra.geometry.make_valid()

# COMMAND ----------

polygon_ccod_defra[polygon_ccod_defra['Proprietor Name (1)'].str.contains('OUR ENVIRONMENTAL DEPARTMENT LIMITED')]

# COMMAND ----------

estate_names = polygon_ccod_defra[polygon_ccod_defra['Proprietor Name (1)'].str.contains('OUR ENVIRONMENTAL DEPARTMENT LIMITED')]
estate_names = estate_names['Proprietor Name (1)'].unique()
display(estate_names)
polygon_ccod_defra = polygon_ccod_defra[~polygon_ccod_defra['Proprietor Name (1)'].isin(estate_names)]

# COMMAND ----------

polygon_ccod_defra.to_file(polygon_ccod_defra_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create layer of total for each organisation

# COMMAND ----------

polygon_ccod_defra_by_organisation = polygon_ccod_defra.dissolve(by='current_organisation', as_index=False, aggfunc='count')
polygon_ccod_defra_by_organisation.geometry = polygon_ccod_defra_by_organisation.geometry.make_valid()

# COMMAND ----------

polygon_ccod_defra_by_organisation = polygon_ccod_defra_by_organisation[['current_organisation', 'geometry']]
polygon_ccod_defra_by_organisation['area_ha'] = polygon_ccod_defra_by_organisation.area/10000

# COMMAND ----------

polygon_ccod_defra_by_organisation

# COMMAND ----------

polygon_ccod_defra_by_organisation.to_parquet(polygon_ccod_defra_by_organisation_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create total layer for each organisation by tenure

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure = polygon_ccod_defra.dissolve(by=['current_organisation', 'Tenure'], as_index=False)
polygon_ccod_defra_by_organisation_tenure.geometry = polygon_ccod_defra_by_organisation_tenure.geometry.make_valid()

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure = polygon_ccod_defra_by_organisation_tenure[['current_organisation', 'Tenure', 'geometry']]
polygon_ccod_defra_by_organisation_tenure['area_ha'] = polygon_ccod_defra_by_organisation_tenure.area/10000

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure.to_parquet(polygon_ccod_defra_by_organisation_tenure_path)

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure

# COMMAND ----------

# MAGIC %md
# MAGIC
