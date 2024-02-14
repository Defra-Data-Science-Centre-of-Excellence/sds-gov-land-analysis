# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #######Set file paths

# COMMAND ----------

#set file paths
ccod_path = '/dbfs/mnt/lab/restricted/ESD-Project/source_data_gov_hm_land_registry/dataset_use_land_and_property_data/format_CSV_use_land_and_property_data/LATEST_use_land_and_property_data/CCOD_FULL_2024_01.csv'

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

# exploring the ccod data
ccod_wo_registration_no = ccod[ccod['Company Registration No. (1)'].isna()]
ccod_proprietor_categories = ccod['Proprietorship Category (1)'].unique()
ccod_proprietor_categories

# COMMAND ----------

# find some clean defra data for reference using single string method
ccod_defra = ccod.loc[ccod['Proprietor Name (1)'].str.contains('secretary of state for environment', case=False, na=False)]
print(ccod_defra.size)
ccod_defra.head(n=50)

# COMMAND ----------

# find some clean defra data for reference using 'and' method
ccod_defra = ccod.loc[ccod['Proprietor Name (1)'].str.contains(r'^(?=.*secretary)(?=.*state)(?=.*environment)', case=False, na=False)]
print(ccod_defra.size)
ccod_defra.head(n=50)

# COMMAND ----------

# find some defra data for reference using 'or' method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(['secretary','state','minister','ministry','deprtment']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(['environment','food','rural']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
print(ccod_filtered.size)
print(ccod_filtered['Proprietor Name (1)'].unique())
ccod_filtered.head(20)

# COMMAND ----------

# find some environment agency data for reference using 'or' method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(['agency']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(['environment']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
print(ccod_filtered.size)
print(ccod_filtered['Proprietor Name (1)'].unique())
ccod_filtered.head(20)

# COMMAND ----------

# find some natural england data for reference using 'or' method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(['natural']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(['england']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
print(ccod_filtered.size)
print(ccod_filtered['Proprietor Name (1)'].unique())
ccod_filtered.head(20)

# COMMAND ----------

# list of ALB/department names of interest - construct dict where values are alternative name options?
alb_names = [
    'Natural England',
    'Joint Nature Conservation Committee',
    'Environment Agency',
    'Rural Payments Agency',
    'Royal Botanic Gardens, Kew',
    'Agriculture and Horiculture Development Board'
    'Animal, Plant Health Agency',
    'Marine Management Organisation'
]

# COMMAND ----------

postcodes = ccod.assign(
    postcode=ccod["pr_address"].str.extract(r"([a-z]{1,2}[\d]{1,2}[a-z]{0,1} [\d]{1}[a-z]{2})$")
    )

# COMMAND ----------

# 
