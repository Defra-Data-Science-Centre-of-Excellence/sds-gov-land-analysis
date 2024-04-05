# Databricks notebook source
from thefuzz import fuzz
import pandas as pd
from thefuzz import process

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

ccod["Proprietor Name (1)"] = ccod["Proprietor Name (1)"].astype(str)

# COMMAND ----------

match_options = ['environment', 'agency']

# COMMAND ----------

for match_option in match_options:
  ccod["match_" + match_option] = ccod["Proprietor Name (1)"].apply(
    lambda x: process.extractOne(match_option,x.split(' '), scorer=fuzz.ratio)
  )


# COMMAND ----------

display(ccod)

# COMMAND ----------

ccod["match"].str[1]

# COMMAND ----------

near_matches_and_matches = ccod[ccod["match_environment"].str[1]>80]
near_matches = near_matches_and_matches[near_matches_and_matches['match_agency'].str[1]>80]
display(near_matches['match_agency'].unique())

# COMMAND ----------

# Check the similarity score
name = "department for environment food and rural affairs"
full_name = "dept for environmnt food and rural affairs"

print(f"Similarity score: {fuzz.partial_ratio(name, full_name)}")
