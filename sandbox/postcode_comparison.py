# Databricks notebook source
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
defra_postcodes = defra_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)
display(np.sort(defra_postcodes))

# COMMAND ----------

# get and compare ea postcode list
ea_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Environment Agency']
ea_postcodes = ea_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)
defra_postcodes_in_ea_list = np.intersect1d(defra_postcodes, ea_postcodes)
display(defra_postcodes_in_ea_list)

# COMMAND ----------

# get and compare ne postcode list
ne_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Natural England']
ne_postcodes = ne_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)
defra_postcodes_in_ne_list = np.intersect1d(defra_postcodes, ne_postcodes)
display(defra_postcodes_in_ne_list)

# COMMAND ----------

# get and compare fe postcode list
fc_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Forestry Commission']
# need to bring in fe provided data as fe ccod data very mixed in with defra
fe_title_polygons_with_ccod_data = gpd.read_file(fe_title_polygons_with_ccod_data_path)
# isolate and extract postcodes from proprietor address and add as new column
fe_title_polygons_with_ccod_data['Proprietor (1) Postcode (1)'] = fe_title_polygons_with_ccod_data["Proprietor (1) Address (1)"].str.extract(r"([a-z]{1,2}[\d]{1,2}[a-z]{0,1} [\d]{1}[a-z]{2})$", flags=re.IGNORECASE)
fe_postcodes = fe_title_polygons_with_ccod_data['Proprietor (1) Postcode (1)'].unique().astype(str)
defra_postcodes_in_fe_list = np.intersect1d(defra_postcodes, fe_postcodes)
display(defra_postcodes_in_fe_list)

# COMMAND ----------

defra_ccod[defra_ccod['Proprietor (1) Postcode (1)'].str.contains()]
compiled_regex_str = regex_str.format(''.join(expression.format(word) for word in org_name.split(' ')))
ccod_filtered = ccod[ccod['Proprietor Name (1)'].str.contains(f'{compiled_regex_str}', case=False, na=False)]
