# Databricks notebook source
# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

import pandas as pd
import openpyxl
import geopandas as gpd

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

# import title list provided by ea
ea_titles = pd.read_excel(ea_titles_path, sheet_name='Freehold_titles_102023')
display(ea_titles)

# COMMAND ----------

# Get ea records from ccod data selected by my methods
ea_ccod = ccod_of_interest[ccod_of_interest['Current_organisation'] == 'Environment Agency']

# COMMAND ----------

# join selected ea ccod data to ea title list (on title number) to enable comparison
ea_ccod_and_supplied_titles = ea_ccod.merge(ea_titles, how='outer', left_on='Title Number', right_on='Title', suffixes=('_filtered_ccod','_ea'))
display(ea_ccod_and_supplied_titles)

# COMMAND ----------

# Find ea titles which have not been identified by my script, by selecting for null Title Number (from selected ea ccod table) field. Then join to unfiltered ccod data to get attribute info.
unidentified_ea_titles = ea_ccod_and_supplied_titles[ea_ccod_and_supplied_titles['Title Number'].isna()]
unidentified_ea_titles_ccod = ccod.merge(unidentified_ea_titles, how='inner', left_on='Title Number', right_on='Title', suffixes=('_unfiltered_ccod','_comparison_table'))
display(unidentified_ea_titles_ccod)

# COMMAND ----------

# get unique list of proprietors for data not selected 
display(unidentified_ea_titles_ccod['Proprietor Name (1)_unfiltered_ccod'].unique())

# COMMAND ----------

# Find in ccod ea titles which have been identified by my script, but aren't in list from ea. Don't need to join back to unfiltered ccod data here as it should already be present from filtered ccod data.
extra_identified_ea_titles = ea_ccod_and_supplied_titles[ea_ccod_and_supplied_titles['Title'].isna()]
# Remove leasehold as the EA only provided Freehold titles in their list
extra_identified_ea_titles = extra_identified_ea_titles[extra_identified_ea_titles['Tenure_filtered_ccod']=='Freehold']
display(extra_identified_ea_titles)

# COMMAND ----------

# MAGIC %md
# MAGIC #### NE Data Comparison

# COMMAND ----------

# Import title list provided by NE
ne_titles = pd.read_csv(ne_titles_path)
# Rename Title NUmber field to 'Title' field (to match EA and help column tracking/maintenance)
ne_titles = ne_titles.rename(columns={'Title Number': 'Title'})
display(ne_titles)

# COMMAND ----------

# Get ne records from ccod data selected by my methods
ne_ccod = ccod_of_interest[ccod_of_interest['Current_organisation'] == 'Natural England']

# COMMAND ----------

# join selected ne ccod data to ne title list (on title number) to enable comparison
ne_ccod_and_supplied_titles = ne_ccod.merge(ne_titles, how='outer', left_on='Title Number', right_on='Title')
display(ne_ccod_and_supplied_titles)

# COMMAND ----------

# Find ne titles which have not been identified by my script, by selecting for null Title Number (from selected ne ccod table) field. Then join to unfiltered ccod data to get attribute info.
unidentified_ne_titles = ne_ccod_and_supplied_titles[ne_ccod_and_supplied_titles['Title Number'].isna()]
unidentified_ne_titles_ccod = ccod.merge(unidentified_ne_titles, how='inner', left_on='Title Number', right_on='Title')
display(unidentified_ne_titles)

# Joining back to unfiltered ccod data produces empty table error, suggesting title numbers aren't present in unfiltered ccod - need to check this


# COMMAND ----------

# Find in ccod ne titles which have been identified by my script, but aren't in list from ne. Don't need to join back to unfiltered ccod data here as it should already be present from filtered ccod data.
extra_identified_ne_titles = ne_ccod_and_supplied_titles[ne_ccod_and_supplied_titles['Title'].isna()]
display(extra_identified_ne_titles)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Forestry Commission data comparison

# COMMAND ----------

fe_title_polygons = gpd.read_file(fe_title_polygons_path)

# COMMAND ----------

display(fe_title_polygons)

# COMMAND ----------

import folium

# COMMAND ----------

fe_title_polygons_half = fe_title_polygons[1:1000]

# COMMAND ----------

m = folium.Map(location=(53, -3), zoom_start= 6, tiles="cartodb positron")
folium.GeoJson(fe_title_polygons_half).add_to(m)

# COMMAND ----------

m
