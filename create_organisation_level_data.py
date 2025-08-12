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

department_polygons = gpd.read_parquet(department_polygons_path)
department_polygons.geometry = department_polygons.geometry.make_valid()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create layer of total for each organisation

# COMMAND ----------

department_polygons_by_organisation = department_polygons.dissolve(by='current_organisation', as_index=False, aggfunc='count')
department_polygons_by_organisation.geometry = department_polygons_by_organisation.geometry.make_valid()

# COMMAND ----------

department_polygons_by_organisation = department_polygons_by_organisation[['current_organisation', 'geometry']]
department_polygons_by_organisation['area_ha'] = department_polygons_by_organisation.area/10000

# COMMAND ----------

department_polygons_by_organisation

# COMMAND ----------

department_polygons_by_organisation.to_parquet(department_polygons_by_organisation_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create total layer for each organisation by tenure

# COMMAND ----------

department_polygons_by_organisation_tenure = department_polygons.dissolve(by=['current_organisation', 'Tenure'], as_index=False)
department_polygons_by_organisation_tenure.geometry = department_polygons_by_organisation_tenure.geometry.make_valid()

# COMMAND ----------

department_polygons_by_organisation_tenure = department_polygons_by_organisation_tenure[['current_organisation', 'Tenure', 'geometry']]
department_polygons_by_organisation_tenure['area_ha'] = department_polygons_by_organisation_tenure.area/10000

# COMMAND ----------

department_polygons_by_organisation_tenure.to_parquet(department_polygons_by_organisation_tenure_path)

# COMMAND ----------

department_polygons_by_organisation_tenure

# COMMAND ----------

# MAGIC %md
# MAGIC
