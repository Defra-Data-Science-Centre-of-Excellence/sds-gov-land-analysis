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
# MAGIC ##### Define functions

# COMMAND ----------

#def get_fuzzy_match_max_score(string_to_search, match_options):
#  '''
#  Don't think this one is used -REMOVE?
#  '''
#  best_match_scores = list()
#  for match_option in match_options:
#    best_match = process.extractOne(match_option,string_to_search.split(' '), scorer=fuzz.ratio)
#    best_match_scores.append(best_match[1])
#  return(max(best_match_scores))

# COMMAND ----------

def get_fuzzy_match_min_score(string_to_search: str, match_options: list):
  '''
  Finds the best match and best match score for all match options in the search string,
  then returns the lowest best match score of all of these.

  Parameters:
    string_to_search (str): string to search and identify best matches from
    match_options (list): list of match options which need to be identified in the string_to_search

  Returns:
    minimum score of all the best match scores
  '''
  best_match_scores = list()
  for match_option in match_options:
    best_match = process.extractOne(match_option,string_to_search.split(' '), scorer=fuzz.ratio)
    best_match_scores.append(best_match[1])
  return(min(best_match_scores))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Set file paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./constants

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

ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(['diocese']), case=False, na=False)]
ccod_filtered['Proprietor Name (1)'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get DEFRA titles

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find potential Defra proprietor names (not typo resilient)

# COMMAND ----------

# generic department identifiers, these shouldn't need changing
cs_department_identifiers = ['state', 'secretary', 'ministry', 'minister', 'department']

# department specific identifiers
all_cs_department_name_identifiers = ['defence','aviation','air',]

# The alternative method to generate identifiers (below) produces too many outputs, as the search parameters become too wide, but could be more useful for other departments

#cs_department_names = {
#    'Department for Environment, Food and Rural Affairs':
#        ['environment', 'food', 'rural'],
#    'Ministry of Agriculture, Fisheries and Food':
#        ['agriculture', 'fisheries','food'],
#    'Department for Environment, Transport and the Regions':
#        ['environment', 'transport','regions']
#}
#all_cs_department_name_identifiers = set()
#for cs_department_name_identifiers in cs_department_names.values():
#    all_cs_department_name_identifiers.update(cs_department_name_identifiers)
#all_cs_department_name_identifiers = list(all_cs_department_name_identifiers)

# COMMAND ----------

# find defra proprietor names in ccod data using or-and-or method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(cs_department_identifiers), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(all_cs_department_name_identifiers), case=False, na=False)]
#ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
defra_names = ccod_filtered['Proprietor Name (1)'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove spurious found proprietor names

# COMMAND ----------

# display for visual inspection
defra_names_df = pd.DataFrame(defra_names)
display(defra_names_df)

# COMMAND ----------

# Inspect identified names with 'ESTATE' in as this is a common match for 'STATE' with allowances made for typos
estate_names = defra_names_df[defra_names_df[0].str.contains('ESTATE')]
display(estate_names)

# COMMAND ----------

# all of the 'estate' names are not defra names, so can be removed
defra_names_df = defra_names_df[~defra_names_df[0].isin(estate_names[0])]

# COMMAND ----------

# Inspect identified names with 'affair' in as this is a common match for 'AIR' with allowances made for typos
affair_names = defra_names_df[defra_names_df[0].str.contains('AFFAIR')]
display(affair_names)

# COMMAND ----------

# all of the 'affair' names are not mod names, so can be removed
defra_names_df = defra_names_df[~defra_names_df[0].isin(affair_names[0])]

# COMMAND ----------

# Output identified names for manual QA
display(defra_names_df)

# COMMAND ----------

# remove any 'duplicates' within the name list (where the only discrepancy is a small typo), so the typo search does not search the same thing multiple times
to_remove = ['PROPERTY REINSTATEMENT REPAIRS AND MANAGEMENT LIMITED', 'THE MINISTER OF TRANSPORT AND CIVIL AVIATION', 'FAIRSTATE LIMITED', 'MINISTRY OF HAIR LIMITED', 'MINISTRY OF HAIR LTD', 'HAIR MINISTRY LIMITED', 'THE PRINCIPAL OFFICIATING MINISTER AND CHURCHWARDENS OF PREESALL ST OSWALD AND THE APPOINTED HEADTEACHER OF PREESALL FLEETWOOD\'S CHARITY SCHOOL AND THE APPOINTED CHAIR OF THE GOVERNING BODY OF PREESALL FLEETWOOD\'S CHARITY SCHOOL','SOCIALIST FEDERAL REPUBLIC OF YUGOSLAVIA (MINISTRY OF DEFENCE)']
defra_names_df = defra_names_df[~defra_names_df[0].isin(to_remove)]

# COMMAND ----------

display(defra_names_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Use created list of names to search for names with typos

# COMMAND ----------

cs_department_found_name_translation_dict = {}

# COMMAND ----------

# The last run of this took 8 hours - very slow!
# I recommend removing as many versions of the defra names above which have typos (as opposed to different, historic names) as possible, to reduce run-time. 
# Any names with typos should be re-identified by the script below.
for defra_name in defra_names_df[0].tolist():
    if defra_name != '':
        ccod["min_match_ratio"] = ccod["Proprietor Name (1)"].apply(
            lambda x: get_fuzzy_match_min_score(x, defra_name.split(' ')))
        ccod_filtered = ccod[ccod['min_match_ratio'] > 80]
        found_names = ccod_filtered['Proprietor Name (1)'].unique()
        # add found potential name to translation dict
        cs_department_found_name_translation_dict[defra_name] = found_names.tolist()
display(cs_department_found_name_translation_dict)

# COMMAND ----------

# add all found names to set (placing in set will remove any duplicates)
defra_found_names = set()
for value in cs_department_found_name_translation_dict.values():
    defra_found_names.update(value)

# COMMAND ----------

# have a look at the produced list
print(defra_found_names)
print(len(defra_found_names))
defra_names = defra_found_names

# COMMAND ----------

defra_names = {'THE MINISTER OF AVIATION', 'THE SECRETARY OF STATE FOR DEFENCE', 'THE MINISTER OF TRANSPORT AND CIVIL AVIATION', 'THE SECRETARY OF STATE FOR DEFENCE OF THE UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND', 'SECRETARY OF STATE FOR DEFENCE', 'THE SECRETARY OF  STATE FOR DEFENCE', 'THE MINISTRY OF DEFENCE', 'THE MINISTRY OF DEFENCE OF THE GOVERMENT OF THE REPUBLIC OF ITALY', 'SECRETARY OF STATE FOR THE MINISTRY OF DEFENCE', 'THE SECRETARY OF STATE FOR AIR', 'THE SECRETARY OF STATE FOR DEFENCE FOR AND ON BEHALF OF HIS MAJESTY'}

# COMMAND ----------

defra_names.remove('THE MINISTER OF TRANSPORT AND CIVIL AVIATION')

# COMMAND ----------



# COMMAND ----------

# if needed, output to csv here
defra_names.to_csv(defra_names_csv_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Populate current organsition field (new) for records with identified Defra proprietor names

# COMMAND ----------

# Populate new 'Current Organisation' field with Department name. For this step, using all proprietor fields although this didn't find any records based on non-primary proprietors anyway
for name in defra_names:
    ccod.loc[ccod['Proprietor Name (1)'] == name, 'current_organisation'] = 'Ministry of Defense'
    ccod.loc[ccod['Proprietor Name (2)'] == name, 'current_organisation'] = 'Ministry of Defense'
    ccod.loc[ccod['Proprietor Name (3)'] == name, 'current_organisation'] = 'Ministry of Defense'
    ccod.loc[ccod['Proprietor Name (4)'] == name, 'current_organisation'] = 'Ministry of Defense'
display(ccod)

# COMMAND ----------

# filter based on newly identified defra records
display(ccod[ccod['current_organisation'].notnull()])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting ALB titles

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Populate a translation dict with found names which likely correspond to organisation of interest name instances

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
display(alb_found_names_translation_dict)

# COMMAND ----------

# OLD METHOD - DOESN'T ACCOUNT FOR TYPOS
# find likely names to populate translation dict
#regex_str = r'^{}'
#expression = '(?=.*{})'
#for current_org, org_names in alb_found_names_translation_dict.items():
#    for org_name in org_names:
#        if org_name != '':
#            # identify titles registered under current name and tag with current and current organisation
#            compiled_regex_str = regex_str.format(''.join(expression.format(word) for word in org_name.split(' ')))
#            ccod_filtered = ccod[ccod['Proprietor Name (1)'].str.contains(f'{compiled_regex_str}', case=False, na=False)]
#            found_names = ccod_filtered['Proprietor Name (1)'].unique()
#            # add found potential name to translation dict
#            alb_found_names_translation_dict[current_org][org_name] = found_names.tolist()
#display(alb_found_names_translation_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### QA produced translation dictionary for Proprietor Name - Organisation translation

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Output translation dict for manual QA

# COMMAND ----------

# produce df from translation dict for manual QA - this could be exported to csv for editing if needed
alb_found_names_translation_df = pd.DataFrame.from_dict(alb_found_names_translation_dict, orient='columns')
display(alb_found_names_translation_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Remove incorrect found names from translation dict

# COMMAND ----------

# Functions to remove found names from translation dict

def remove_found_name(current_organisation_name, organisation_instance_name, found_name_for_removal, alb_found_names_translation_dict):
    '''
    Remove specific found name for organsation/organisation instance
    Parameters:
        current_organisation (str): current name of the organisation the found name to remove is associated with
        organisation_instance_name (str): instance (current or historic) name of the organisation the found name to remove is associated with
        found_name_for_removal (str): found name to remove from translation dict # Could be useful to let this accept a list, which would remove the need for 'for loops' in the following cells
        alb_found_names_translation_dict: alb found proprietor name - organisation translation dict
    '''
    try:
        alb_found_names_translation_dict[current_organisation_name][organisation_instance_name].remove(found_name_for_removal)
        print(f'{found_name_for_removal} removed from current org: {current_organisation_name}, organisation instance: {organisation_instance_name}')
        print(f'This leaves the remaining found names: {alb_found_names_translation_dict[current_organisation_name][organisation_instance_name]}')
    except:
        print(f'WARNING: {found_name_for_removal} not found in current org: {current_organisation_name}, organisation instance: {organisation_instance_name}')

# Functions to remove all found names for single organisation from translation dict, created for easier manual qa implementation (use if on inspection all names for one organisation are incorrect. Otherwise use above function instead)
def remove_all_found_names(current_organisation_name, alb_found_names_translation_dict):
    '''
    Remove all found names for an organisation
        Parameters:
        current_organisation (str): current name of the organisation to remove all found names for
        alb_found_names_translation_dict: alb found proprietor name - organisation translation dict
    '''
    for organisation_instance_name in alb_found_names_translation_dict[current_organisation_name].keys():
        alb_found_names_translation_dict[current_organisation_name][organisation_instance_name] = []


# COMMAND ----------

remove_all_found_names('Animal, Plant Health Agency', alb_found_names_translation_dict)

# COMMAND ----------

ea_wrong_names = ['THE ENVIRONMENT AGENCY (WALES)']
for name in ea_wrong_names:
    remove_found_name('Environment Agency', 'Environment Agency', name, alb_found_names_translation_dict)

# COMMAND ----------

fc_wrong_names = ['NHS WALTHAM FOREST CLINICAL COMMISSIONING GROUP',]
for name in fc_wrong_names:
    remove_found_name('Forestry Commission', 'Forestry Commission', name, alb_found_names_translation_dict)

fe_wrong_names = ["HEART OF ENGLAND FOREST LIMITED","THE HEART OF ENGLAND FOREST LTD","HEART OF ENGLAND FOREST","ARDEN FOREST CHURCH OF ENGLAND MULTI ACADEMY TRUST","THE HEART OF ENGLAND FOREST","ARDEN FOREST CHURCH OF ENGLAND MULTI-ACADEMY TRUST","THE HEART OF ENGLAND FOREST LIMITED"]
for name in fe_wrong_names:
    remove_found_name('Forestry Commission', 'Forestry England', name, alb_found_names_translation_dict)

fr_wrong_names = ["FORRESTER RESEARCH LIMITED"]
for name in fr_wrong_names:
    remove_found_name('Forestry Commission', 'Forestry Research', name, alb_found_names_translation_dict)

# COMMAND ----------

british_wool_wrong_names = ['ROYAL BRITISH LEGION WOOLSTON WITH MARTINSCROFT EX SERVICEMANS CLUB LIMITED', 'WOOL ROYAL BRITISH LEGION CLUB LIMITED',"BRITISH WOOL COMPANY (WEMBLEY) LIMITED","BRITISH WOOL MARKETING BOARD"]
for name in british_wool_wrong_names:
    remove_found_name('British Wool', 'British Wool', name, alb_found_names_translation_dict)

# COMMAND ----------

flood_re_wrong_names = ['119 FLOOD STREET LIMITED', 'LONDON FLOOD PREVENTION LTD', 'FLOOD STREET MANAGEMENT COMPANY LIMITED', '109 FLOOD STREET LIMITED', 'FLOOD STREET LIMITED', 'FLOODLIGHT LEISURE LIMITED', '24 HR FIRE & FLOOD ASSISTANCE LIMITED', '111 FLOOD STREET LIMITED', '115 FLOOD STREET LIMITED']
for name in flood_re_wrong_names:
    remove_found_name('Flood Re', 'Flood Re', name, alb_found_names_translation_dict)

# COMMAND ----------

seafish_wrong_names = ["BRIXHAM SEAFISH COMPANY LIMITED","SEAFISH IMPORTERS LIMITED","SEAFISH U.K. LIMITED",'B SELFISH LIMITED', 'B SELFISH LTD']
for name in seafish_wrong_names:
    remove_found_name('Seafish', 'Seafish', name, alb_found_names_translation_dict)

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
# MAGIC #### Filter to remove non defra/ALB data

# COMMAND ----------

ccod_of_interest = (ccod[ccod['current_organisation'].notna()])
display(ccod_of_interest)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Disentangle forestry commission and DEFRA titles

# COMMAND ----------

### Note: the relationship between land owned by Forestry England, Forestry Commission and Defra is very complicated. So it may not always be appropriate to try to separate FE and DEFRA land. It has been done here for the purpose of better understanding the data and practical management of the land

# COMMAND ----------

# based on comparison to Forestry commission ownership data, search terms to delineate defra and Forestry England titles have been identified, although these are not comprehensive as other identified search terms would also identify defra properties
search_terms_list = [
    ['BS16','1EJ'],
    ['coldharbour'],
    ['SY8', '2HD']
]
for search_terms in search_terms_list:
    ccod_of_interest["fc_min_match_ratio"] = ccod_of_interest['Proprietor (1) Address (1)'].apply(
                    lambda x: get_fuzzy_match_min_score(x, search_terms))
    mask = ccod_of_interest['fc_min_match_ratio'] > 80
    ccod_of_interest['current_organisation'][mask] = 'Forestry Commission'
    ccod_of_interest = ccod_of_interest.drop(columns=['fc_min_match_ratio'])


# COMMAND ----------

display(ccod_of_interest[ccod_of_interest['current_organisation']=='Forestry Commission'])

# COMMAND ----------

fc_ccod = ccod_of_interest[ccod_of_interest['Proprietor (1) Address (1)'].str.contains('TA1 4AP', case=False, na=False)]
fc_ccod

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

ccod_of_interest.to_csv(ccod_defra_and_alb_path)
