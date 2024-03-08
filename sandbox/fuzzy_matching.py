# Databricks notebook source
from thefuzz import fuzz
import pandas as pd

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

ccod["match"] = ccod["Proprietor Name (1)"].apply(
  lambda x: fuzz.token_set_ratio(x, 'secretary')
)


# COMMAND ----------

ccod.head()

# COMMAND ----------

# Check the similarity score
name = "department for environment food and rural affairs"
full_name = "dept for environmnt food and rural affairs"

print(f"Similarity score: {fuzz.partial_ratio(name, full_name)}")
