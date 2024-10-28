# Databricks notebook source
# MAGIC %md
# MAGIC ### Data comparison postcode
# MAGIC To help validate alb vs defra records. Compare postcodes associated with alb land parcels with postcodes associated with defra records. This is to identify if/ help to disentangle defra and alb owned land parcels.

# COMMAND ----------

import re
import pandas as pd
import geopandas as gpd
import numpy as np

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# import selected ccod data
ccod_of_interest = pd.read_csv("/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/ccod_outputs/ccod_of_interest.csv")
display(ccod_of_interest)

# COMMAND ----------

# isolate and extract postcodes from proprietor address and add as new column
ccod_of_interest['Proprietor (1) Postcode (1)'] = ccod_of_interest["Proprietor (1) Address (1)"].str.extract(r"([a-z]{1,2}[\d]{1,2}[a-z]{0,1} [\d]{1}[a-z]{2})$", flags=re.IGNORECASE)

# COMMAND ----------

# get defra postcode list
defra_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Department for Environment, Food and Rural Affairs']
display(defra_ccod['Proprietor (1) Postcode (1)'].value_counts())
defra_postcodes = defra_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)
##display(np.sort(defra_postcodes))

# COMMAND ----------

# get ea postcode list
ea_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Environment Agency']
display(ea_ccod['Proprietor (1) Postcode (1)'].value_counts())
ea_postcodes = ea_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)

# COMMAND ----------

# compare ea and defra postcode lists
defra_postcodes_in_ea_list = np.intersect1d(defra_postcodes, ea_postcodes)
display(defra_postcodes_in_ea_list)

# COMMAND ----------

# get ne postcode list
ne_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Natural England']
display(ne_ccod['Proprietor (1) Postcode (1)'].value_counts())
ne_postcodes = ne_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)

# COMMAND ----------

# compare ne and defra postcode lists
defra_postcodes_in_ne_list = np.intersect1d(defra_postcodes, ne_postcodes)
display(defra_postcodes_in_ne_list)

# COMMAND ----------

# get fe postcode list
fc_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Forestry Commission']
# need to bring in fe provided data as fe ccod data very mixed in with defra
fe_title_polygons_with_ccod_data = gpd.read_file(fe_title_polygons_with_ccod_data_path)
# isolate and extract postcodes from proprietor address and add as new column
fe_title_polygons_with_ccod_data['Proprietor (1) Postcode (1)'] = fe_title_polygons_with_ccod_data["Proprietor (1) Address (1)"].str.extract(r"([a-z]{1,2}[\d]{1,2}[a-z]{0,1} [\d]{1}[a-z]{2})$", flags=re.IGNORECASE)
fe_postcodes = fe_title_polygons_with_ccod_data['Proprietor (1) Postcode (1)'].unique().astype(str)


# COMMAND ----------

display(fe_title_polygons_with_ccod_data['Proprietor (1) Postcode (1)'].value_counts().to_frame().reset_index())

# COMMAND ----------

# compare fe and defra postcode lists
defra_postcodes_in_fe_list = np.intersect1d(defra_postcodes, fe_postcodes)
display(defra_postcodes_in_fe_list)

# COMMAND ----------

# re-associate ccod data with overlapping postcodes
defra_ccod_postcodes_in_fe_list = defra_ccod.loc[defra_ccod['Proprietor (1) Postcode (1)'].str.contains('|'.join(defra_postcodes_in_fe_list), case=False, na=False)]
defra_ccod_postcodes_in_fe_list

# COMMAND ----------

# Get number of ccod records associated with defra which relate to the Bs16 1EJ postcode - quite a lot
defra_ccod_postcodes_associated_with_fe_headquarters = defra_ccod.loc[defra_ccod['Proprietor (1) Postcode (1)'].str.contains('|'.join(['BS16 1EJ','B16 1EJ','BS1 1EJ']), case=False, na=False)]
defra_ccod_postcodes_associated_with_fe_headquarters

# COMMAND ----------

# Get number of ccod records associated with defra which don't relate to the Bs16 1EJ postcode
defra_ccod_postcodes_in_fe_list_excl_fe_headquarters = defra_ccod.loc[~defra_ccod['Proprietor (1) Postcode (1)'].str.contains('|'.join(['BS16 1EJ','B16 1EJ','BS1 1EJ']), case=False, na=False)]
defra_ccod_postcodes_in_fe_list_excl_fe_headquarters
