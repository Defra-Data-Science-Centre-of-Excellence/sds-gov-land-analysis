# Databricks notebook source
# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

import pandas as pd
import openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get all ccod and identified ccod of interest data

# COMMAND ----------

# read in ccod data
ccod = pd.read_csv(
    ccod_path,    
    usecols=[
        "Title Number",
        "Tenure",
        "Proprietor Name (1)",
        "Company Registration No. (1)",
        "Proprietorship Category (1)",
        "Proprietor (1) Address (1)",
        "Date Proprietor Added",
        "Additional Proprietor Indicator"
        ]
)

ccod.head()

# COMMAND ----------

ccod_of_interest = pd.read_csv("/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/ccod_outputs/ccod_of_interest.csv")
display(ccod_of_interest)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comparison to Piumi's work

# COMMAND ----------

# MAGIC %md
# MAGIC #### EA data comparison

# COMMAND ----------

ea_titles = pd.read_excel(ea_titles_path, sheet_name='Freehold_titles_102023')
display(ea_titles)

# COMMAND ----------

# Get ea only
ea_ccod = ccod_of_interest[ccod_of_interest['Current_organisation'] == 'Environment Agency']

# COMMAND ----------

# join on title number to enable comparison
previous_current_defra_names_comparison = ea_ccod.merge(ea_titles, how='outer', left_on='Title Number', right_on='Title')

# COMMAND ----------

# Find in ccod ea titles which have not been identified by my script
unidentified_ea_titles = previous_current_defra_names_comparison[previous_current_defra_names_comparison['Title Number'].isna()]
unidentified_ea_titles_ccod = ccod.merge(unidentified_ea_titles, how='inner', left_on='Title Number', right_on='Title')
display(unidentified_ea_titles_ccod)
