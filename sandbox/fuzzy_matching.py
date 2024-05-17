# Databricks notebook source
# MAGIC %sh
# MAGIC pip install thefuzz

# COMMAND ----------

from thefuzz import fuzz
import pandas as pd
from thefuzz import process

# COMMAND ----------

string_to_check = ['Department', 'for', 'enviroment']
string_to_find = 'Environment'
process.extractOne(string_to_find, string_to_check)

# COMMAND ----------



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

def get_fuzzy_match_min_score(string_to_search, match_options):
  best_match_scores = list()
  for match_option in match_options:
    best_match = process.extractOne(match_option,string_to_search.split(' '), scorer=fuzz.ratio)
    best_match_scores.append(best_match[1])
  return(min(best_match_scores))

def filter_for_match_scores_above_threshold(string_to_search, match_options, match_threshold):
  best_match_scores = list()
  for match_option in match_options:
    best_match = process.extractOne(match_option,string_to_search.split(' '), scorer=fuzz.ratio)
    best_match_scores.append(best_match[1])
  if min(best_match_scores) > match_threshold:
    return True
  else:
    return False

#for match_option in match_options:
#  ccod["match_" + match_option] = ccod["Proprietor Name (1)"].apply(
#    lambda x: process.extractOne(match_option,x.split(' '), scorer=fuzz.ratio)
#  )


# COMMAND ----------

match_options = ['environment','agency']
ccod_filtered = ccod["Proprietor Name (1)"].filter(
    lambda x: filter_for_match_scores_above_threshold(x, match_options, 80))


# COMMAND ----------

match_options = ['environment','agency']
ccod['min_match_score'] = ccod["Proprietor Name (1)"].apply(
    lambda x: get_fuzzy_match_min_score(x, match_options))

# COMMAND ----------

ccod.drop(columns='min_match_score')

# COMMAND ----------

ccod_imperfect = ccod[ccod["min_match_ratio"] != 100]
ccod_near_matches = ccod_imperfect[ccod_imperfect["min_match_ratio"] > 80]

# COMMAND ----------

ccod_near_matches

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
