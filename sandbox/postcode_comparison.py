# Databricks notebook source
import re
import pandas as pd
import numpy as np

# COMMAND ----------

ccod_of_interest = pd.read_csv("/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/ccod_outputs/ccod_of_interest.csv")
display(ccod_of_interest)

# COMMAND ----------

ccod_of_interest['Proprietor (1) Postcode (1)'] = ccod_of_interest["Proprietor (1) Address (1)"].str.extract(r"([a-z]{1,2}[\d]{1,2}[a-z]{0,1} [\d]{1}[a-z]{2})$", flags=re.IGNORECASE)

# COMMAND ----------

fc_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Forestry Commission']
display(fc_ccod)

# COMMAND ----------

ea_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Environment Agency']
ea_postcodes = ea_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)
display(np.sort(ea_postcodes))

# COMMAND ----------

ne_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Natural England']
ne_postcodes = ne_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)
display(np.sort(ne_postcodes))

# COMMAND ----------

defra_ccod = ccod_of_interest[ccod_of_interest['Current_organisation']=='Department for Environment, Food and Rural Affairs']
defra_postcodes = defra_ccod['Proprietor (1) Postcode (1)'].unique().astype(str)
display(np.sort(defra_postcodes))
