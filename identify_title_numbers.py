# Databricks notebook source
# MAGIC %md
# MAGIC ### Identify title numbers
# MAGIC This script uses the UK Company Proprietor dataset (ccod) to produce a filtered version of the ccod with only titles of interest (ie. those associated with DEFRA or its ALBs). <br>
# MAGIC Additional fields are added to the produced dataset to represent current and historic organisation of interest names. <br>
# MAGIC <b>Next steps:</b> Once the filtered version of the ccod has been produced this can be joined to the polygon geometries in the national polygon dataset for a spatial representation of DEFRA's land (use the identify_land_parcels script to do this)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set up

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Install/import required packages
# MAGIC - thefuzz: used for identifying word with typos
# MAGIC - pandas & numpy: general purpose data manipulation packages

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install thefuzz

# COMMAND ----------

import pandas as pd
import numpy as np
from thefuzz import fuzz
from thefuzz import process

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Run setup notebooks

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import CCOD (UK Companies which own properties in England and Wales) data

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
        "Additional Proprietor Indicator",
        "Proprietor Name (2)",
        "Proprietorship Category (2)",
        "Proprietor (2) Address (1)",
        "Proprietor Name (3)",
        "Proprietorship Category (3)",
        "Proprietor (3) Address (1)",
        "Proprietor Name (4)",
        "Proprietorship Category (4)",
        "Proprietor (4) Address (1)",
        ]
)
ccod["Proprietor Name (1)"] = ccod["Proprietor Name (1)"].astype(str)
ccod.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1: Get titles using generic and specific department identifiers
# MAGIC This method was made to identify department titles (not ALBs) and involves setting generic and specific department identifiers. <br>In general, the generic department identifiers should not need changing. <br>The specific department identifiers should be changed depending on the department of interest. These should be changes in the constants notebook (see below)
# MAGIC <b>Note:</b> This section uses 3 variables assigned in the constants notebook:
# MAGIC - standardised_department_name
# MAGIC - generic_department_identifiers
# MAGIC - specific_department_identifiers
# MAGIC <br><br>
# MAGIC If these are updated, ensure the following command has been run before proceding:<br>
# MAGIC `%run ./constants` <br>
# MAGIC (this is part of the standard script set run at the top of this notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find potential department proprietor names
# MAGIC Note: this initial search will not identify versions of the name with typos, this will be done later.

# COMMAND ----------

# find department proprietor names in ccod data using or-and-or method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(generic_department_identifiers), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(specific_department_identifiers), case=False, na=False)]
#ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
department_names_from_ccod = ccod_filtered['Proprietor Name (1)'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove spurious found proprietor names
# MAGIC The searching methods often select some incorrect names. These need to be manually checked, and then removed using the below code

# COMMAND ----------

# When run, this cell will display the names in the ccod data which have been identified as relevant
# Look for names which don't relate to the department of interest and make note of them. We will then remove them below.
department_names_from_ccod_df = pd.DataFrame(department_names_from_ccod)
display(department_names_from_ccod_df)

# COMMAND ----------

# Inspect identified names with 'ESTATE' in as this is a common match for 'STATE' with allowances made for typos
estate_names = department_names_from_ccod_df[department_names_from_ccod_df[0].str.contains('ESTATE')]
display(estate_names)

# COMMAND ----------

# If all of the 'estate' names are not relevant, they can be removed. This is probably useful for a lot of departments.
department_names_from_ccod_df = department_names_from_ccod_df[~department_names_from_ccod_df[0].isin(estate_names[0])]

# COMMAND ----------

# remove any other spurious names by adding to to_remove list
to_remove = ['STATESIDE FOODS LIMITED', 'OUR ENVIRONMENTAL DEPARTMENT LIMITED',]
department_names_from_ccod_df = department_names_from_ccod_df[~department_names_from_ccod_df[0].isin(to_remove)]

# COMMAND ----------

# Output identified names for manual QA
display(department_names_from_ccod_df)

# COMMAND ----------

# Optional step. This will help speed up run time of the following section
# remove any 'duplicates' within the name list (where the only discrepancy is a small typo), so the typo search does not search the same thing multiple times
to_remove = ['THE SECRETARY OF STATE FOR THE ENVIRONMENT',]
department_names_from_ccod_df = department_names_from_ccod_df[~department_names_from_ccod_df[0].isin(to_remove)]

# COMMAND ----------

# Final visual inspection of the list - this is the list of names which will be searched for using typo resilient methods
display(department_names_from_ccod_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Use created list of names to search for names with typos

# COMMAND ----------

# The last run of this took 8 hours - very slow!
# I recommend removing as many versions of the defra names above which have typos (as opposed to different, historic names) as possible, to reduce run-time. 
# Any names with typos should be re-identified by the script below.

# initiate empty dict to store found names
cs_department_found_name_translation_dict = {}
for department_name in department_names_from_ccod_df[0].tolist():
    if department_name != '':
        ccod["min_match_ratio"] = ccod["Proprietor Name (1)"].apply(
            lambda x: get_fuzzy_match_min_score(x, department_name.split(' ')))
        ccod_filtered = ccod[ccod['min_match_ratio'] > 80]
        found_names = ccod_filtered['Proprietor Name (1)'].unique()
        # add found potential name to translation dict
        cs_department_found_name_translation_dict[department_name] = found_names.tolist()
        # add all found names to a set - this will remove any duplicates
        
department_found_names = set()
for value in cs_department_found_name_translation_dict.values():
    department_found_names.update(value)

# COMMAND ----------

# have a look at the produced list, and make sure the listed named look relevant to your department of interest
print(department_found_names)
print(len(department_found_names))

# COMMAND ----------

# if needed, output to csv here
#department_found_names.to_csv(department_found_names_csv_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Populate (new) current organsition field in the CCOD data
# MAGIC Once we have a version of the ccod table with standardised names, we will be able to easily filter for our department of interest

# COMMAND ----------

# Populate new 'Current Organisation' field with Department name. For this step, using all proprietor fields although this didn't find any records based on non-primary proprietors anyway
for name in department_found_names:
    ccod.loc[ccod['Proprietor Name (1)'] == name, 'current_organisation'] = standardised_department_name
    ccod.loc[ccod['Proprietor Name (2)'] == name, 'current_organisation'] = standardised_department_name
    ccod.loc[ccod['Proprietor Name (3)'] == name, 'current_organisation'] = standardised_department_name
    ccod.loc[ccod['Proprietor Name (4)'] == name, 'current_organisation'] = standardised_department_name
display(ccod)

# COMMAND ----------

# filter based on newly identified defra records
display(ccod[ccod['current_organisation'].notnull()])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2: Get titles using full organisation name
# MAGIC This method is usually best when identifying ALB titles, but in some cases may be helpful for identifying department titles. <br>
# MAGIC If in doubt, both methods can be used and the identified names compared
# MAGIC
# MAGIC <b>Note:</b> this method relies on the alb_found_names_translation_dict variable. This is set in the constants notebook. If updated, ensure the following command has been run before proceding:<br>
# MAGIC `%run ./constants` <br>
# MAGIC (this is part of the standard script set run at the top of this notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find potential organisation names
# MAGIC Using typo resilient search methods, each organisation name instance in the found names translation dict is searched for.
# MAGIC The translation dict is then populated with these found names.

# COMMAND ----------

# need to convert proprietor name field to a string for the next step
ccod["Proprietor Name (1)"] = ccod["Proprietor Name (1)"].astype(str)

# COMMAND ----------

# approx 1 hour runtime
# get found names for all search terms in translation dict (typo resilient search)
for current_org, org_names in alb_found_names_translation_dict.items():
    for org_name in org_names:
        if org_name != '':
            ccod["min_match_ratio"] = ccod["Proprietor Name (1)"].apply(
                lambda x: get_fuzzy_match_min_score(x, org_name.split(' ')))
            ccod_filtered = ccod[ccod['min_match_ratio'] > 80]
            found_names = ccod_filtered['Proprietor Name (1)'].unique()
            # add found potential name to translation dict
            alb_found_names_translation_dict[current_org][org_name] = found_names.tolist()

# COMMAND ----------

# produce df from translation dict for manual QA - this could be exported to csv for editing if needed
alb_found_names_translation_df = pd.DataFrame.from_dict(alb_found_names_translation_dict, orient='columns')
display(alb_found_names_translation_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove spurious names
# MAGIC The typo resilient search methods used often select some incorrect names. These need to be manually checked and removed if necessary. This removal can be done using the code below, or by outputting to csv and re-uploading an edited version.
# MAGIC
# MAGIC If using the code below, not there are 2 functions which can be used for name removal:
# MAGIC - remove_found_name - requires 4 paramaters passed (current_organisation_name, organisation_instance_name, found_name_for_removal, alb_found_names_translation_dict). It will remove a specific found name.
# MAGIC - remove_all_found_names - requires 2 parameters passed (current_organisation_name, alb_found_names_translation_dict). It will remove all found names for an orgnaisation.

# COMMAND ----------

remove_all_found_names('Animal, Plant Health Agency', alb_found_names_translation_dict)

# COMMAND ----------

ea_wrong_names = ['THE ENVIRONMENT AGENCY (WALES)']
for name in ea_wrong_names:
    remove_found_name('Environment Agency', 'Environment Agency', name, alb_found_names_translation_dict)

# COMMAND ----------

# output translation dict for final manual QA
print(alb_found_names_translation_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Populate current and historic organsition fields (new) for records with identified ALB proprietor names

# COMMAND ----------

# Use translation dict to add two new columns, populated with current and historic organisation names, based on matches with found names in the translation dict
# As with department method, check all proprietor name fields for found names. As with department, only primary proprietor name results in identification
for current_org_name, org_names in alb_found_names_translation_dict.items():
        for org_name, found_names in org_names.items():
            for found_name in found_names:
                ccod.loc[ccod['Proprietor Name (1)'] == found_name, 'current_organisation'] = current_org_name
                ccod.loc[ccod['Proprietor Name (1)'] == found_name, 'historic_organisation'] = org_name
                ccod.loc[ccod['Proprietor Name (2)'] == found_name, 'current_organisation'] = current_org_name
                ccod.loc[ccod['Proprietor Name (2)'] == found_name, 'historic_organisation'] = org_name
                ccod.loc[ccod['Proprietor Name (3)'] == found_name, 'current_organisation'] = current_org_name
                ccod.loc[ccod['Proprietor Name (3)'] == found_name, 'historic_organisation'] = org_name
                ccod.loc[ccod['Proprietor Name (4)'] == found_name, 'current_organisation'] = current_org_name
                ccod.loc[ccod['Proprietor Name (4)'] == found_name, 'historic_organisation'] = org_name

# COMMAND ----------

display(ccod)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter to remove ccod records not relating to department

# COMMAND ----------

ccod_of_interest = (ccod[ccod['current_organisation'].notna()])
display(ccod_of_interest)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Export titles of interest to csv

# COMMAND ----------

# drop created min_match_ratio field - no longer needed and populated only with values for last organisation searched for, so mostly meaningless
ccod_of_interest = ccod_of_interest.drop(columns=['min_match_ratio'])

# COMMAND ----------

# display for a quick double check before output
display(ccod_of_interest)

# COMMAND ----------

ccod_of_interest.to_csv(department_ccod_path)
