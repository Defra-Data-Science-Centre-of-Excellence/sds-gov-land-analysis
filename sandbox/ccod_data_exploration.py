# Databricks notebook source
# MAGIC %sh
# MAGIC pip install thefuzz

# COMMAND ----------

import pandas as pd
from thefuzz import fuzz
from thefuzz import process

# COMMAND ----------

def get_fuzzy_match_min_score(string_to_search, match_options):
  best_match_scores = list()
  for match_option in match_options:
    best_match = process.extractOne(match_option,string_to_search.split(' '), scorer=fuzz.ratio)
    best_match_scores.append(best_match[1])
  return(min(best_match_scores))

# COMMAND ----------

# MAGIC %md
# MAGIC #######Set file paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

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

# list of department names of interest - construct dict where values are alternative name options?
cs_department_identifiers = ['state', 'secretary', 'ministry', 'minister', 'department']
cs_department_names = {
    'Department for Environment, Food and Rural Affairs':
        ['environment', 'food', 'rural'],
    'Ministry of Agriculture, Fisheries and Food':
        ['agriculture', 'fisheries','food'],
    'Department for Environment, Transport and the Regions':
        ['environment', 'transport','regions']
    
}

# COMMAND ----------

# Better format to match translation dict used for ALBs?
cs_department_translation_dict = {
    'Department for Environment, Food and Rural Affairs':
        ['environment', 'food', 'rural']
}

# COMMAND ----------



# COMMAND ----------

import numpy as np
# find some defra data for reference using or-and-or method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(cs_department_identifiers), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(['environment','food','rural']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
print(ccod_filtered.size)
defra_names = ccod_filtered['Proprietor Name (1)'].unique()
print(len(defra_names))
display(defra_names)

# COMMAND ----------

# Output identified names for manual QA
print(len(defra_names))
display(pd.DataFrame(defra_names))

# COMMAND ----------

# Populate new 'Current Organsiation' field with Department name
for name in defra_names:
    print(name)
    ccod.loc[ccod['Proprietor Name (1)'] == name, 'Current_organisation'] = 'Department for Environment, Food and Rural Affairs'
print(defra_names)

# COMMAND ----------

# get the addresses associated with defra propriator names
display(pd.DataFrame(ccod['Proprietor (1) Address (1)'].unique()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comparison to Piumi's work

# COMMAND ----------

# bring in defra name list created previously
previous_defra_names = pd.DataFrame(['the secretary of state for environment food and rural affairs',
       'secretary of state for environment food and rural affairs',
       'the secretary of state for environment, food and rural affairs',
       'secretary of state for environment food and rural  affairs',
       'the secretary of state for  environment food and rural affairs',
       'the secretary of state for the environment, food and rural affairs',
       'the secretary of state for the environment food and rural affairs',
       'the secretary of state for the department for environment, food and rural affairs',
       'secretary of state for environment  food and rural affairs',
       'the secretary of state for the department of the environment, food and rural affairs',
       'the secretary of state for the environment food & rural affairs',
       'secretary of state for the environment food and rural affairs',
       'secretary of state for environment food and rural',
       'the secretary of state for the department of the environment food and rural affairs',
       'the secretary of state for the department of the environment, food and rural affairs (defra)',
       'secretary  of state for environment food and rural affairs',
       'secretary of state for environment, food and rural affairs',
       'the secretary of state for the department of the environment for food and rural affairs',
       'secretary of state for  environment food and rural affairs',
       'secretary of state  for environment food and rural affairs',
       'the secretary of state for food environment and rural affairs',
       'secretary of  state for environment food and rural affairs',
       'secretary  of state for environment food and rural  affairs',
       'secretary of state for environment fisheries and food',
       'the secretary of state for the environment',
       'the secretary of state for the environment transport and the regions',
       'the secretary of state for the environment, transport and the regions',
       'secretary of state for the environment',
       'secretary of state for the environment, transport and the regions',
       'the secretary of state for environment',
       'secretary of state for the environment transport and the regions',
       'the secretary of state for the environment and the regions',
       'the secretary of state of the environment',
       'secretary of state for the environment transport and the regions>'], 
       columns=['lowercase_name']
       )

display(previous_defra_names)

# COMMAND ----------

# Make new name list lower case to enable comparison
defra_names_for_comparison = pd.DataFrame(defra_names, columns=['raw_name'])
defra_names_for_comparison['lowercase_name_new'] = defra_names_for_comparison['raw_name'].str.lower()
display(defra_names_for_comparison)

# COMMAND ----------

# join on lowercase name col
previous_current_defra_names_comparison = defra_names_for_comparison.merge(previous_defra_names, how='outer', left_on='lowercase_name_new', right_on='lowercase_name')
display(previous_current_defra_names_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting ALB titles

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Populate a translation dict with found names which likely correspond to organisation of interest name instances

# COMMAND ----------

''' 
hierarchical nested translation dict in the format:{
    'Current organisation name':{
        'organisation name instance (current or historic)': [list of found names associated with organisation name instance]
    }
}
'organisation name instance (current or historic) is split by word and used for searching
'''

alb_found_names_translation_dict = {
    'Natural England':{
        'Natural England': [],
        'English Nature': [],
        'Countryside Agency': [],
        'Rural Development Service': [],
        'Nature Conservancy Council': [],
    },
    'Joint Nature Conservation Committee':{
        'Joint Nature Conservation Committee': [],
    },
    'Environment Agency':{
        'Environment Agency': [],
    },
    'Rural Payments Agency':{
        'Rural Payments Agency': [],
    },
    'Royal Botanic Gardens Kew':{
        'Royal Botanic Gardens, Kew': [],
    },
    'Agriculture and Horiculture Development Board':{
        'Agriculture and Horiculture Development Board': [],
    },
    'Animal, Plant Health Agency':{
        'Animal Plant Health Agency': [],
    },
    'Marine Management Organisation':{
        'Marine Management Organisation': [],
    },
    'Forestry Commission':{
        'Forestry Commission': [],
        'Forestry England': [],
        'Forestry Research': [],
    },
    'The Water Services Regulation Authority':{
        'The Water Services Regulation Authority': [],
    },
    'Centre for Environment, Fisheries and Aquaculture Science':{
        'Centre for Environment, Fisheries and Aquaculture Science': [],
    },
    'Veterinary Medicines Directorate':{
        'Veterinary Medicines Directorate': [],
    },
    'Consumer Council for Water':{
        'Consumer Council for Water': [],
    },
    'Office for Environmental Protection':{
        'Office for Environmental Protection': [],
    },
    'Seafish':{
        'Seafish': [],
    },
    'Advisory Committee on Releases to the Environment':{
        'Advisory Committee on Releases to the Environment': [],
    },  
    # check this is okay
    'Defra\'s Science Advisory Council':{
        'Defra\'s Science Advisory Council': [],
    },
    'Independent Agricultural Appeals Panel':{
        'Independent Agricultural Appeals Panel': [],
    },
    'Veterinary Products Committee':{
        'Veterinary Products Committee': [],
    },
    'Plant Varieties and Seeds Tribunal':{
        'Plant Varieties and Seeds Tribunal': [],
    },
    'British Wool':{
        'British Wool': [],
    },
    'Broads Authority':{
        'Broads Authority': [],
    },
    'Covent Garden Market Authority':{
        'Covent Garden Market Authority': [],
    },
    'Dartmoor National Park Authority':{
        'Dartmoor National Park Authority': [],
    },
    'Exmoor National Park Authority':{
        'Exmoor National Park Authority': [],
    },
    'Flood Re':{
        'Flood Re': [],
    },
    'Lake District National Park Authority':{
        'Lake District National Park Authority': [],
    },
    'National Forest Company':{
        'National Forest Company': [],
    },
    'New Forest National Park Authority':{
        'New Forest National Park Authority': [],
    },
    'North York Moors National Park Authority':{
        'North York Moors National Park Authority': [],
    },
    'Northumberland National Park Authority':{
        'Northumberland National Park Authority': [],
    },
    'Peak District National Park Authority':{
        'Peak District National Park Authority': [],
    },
    'South Downs National Park Authority':{
        'South Downs National Park Authority': [],
    },
    'Yorkshire Dales National Park Authority':{
        'Yorkshire Dales National Park Authority': [],
    },
}

# COMMAND ----------

ccod["Proprietor Name (1)"] = ccod["Proprietor Name (1)"].astype(str)

# COMMAND ----------

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

# find likely names to populate translation dict
regex_str = r'^{}'
expression = '(?=.*{})'
for current_org, org_names in alb_found_names_translation_dict.items():
    for org_name in org_names:
        if org_name != '':
            # identify titles registered under current name and tag with current and current organisation
            compiled_regex_str = regex_str.format(''.join(expression.format(word) for word in org_name.split(' ')))
            ccod_filtered = ccod[ccod['Proprietor Name (1)'].str.contains(f'{compiled_regex_str}', case=False, na=False)]
            found_names = ccod_filtered['Proprietor Name (1)'].unique()
            # add found potential name to translation dict
            alb_found_names_translation_dict[current_org][org_name] = found_names.tolist()
display(alb_found_names_translation_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Output translation dict for manual QA

# COMMAND ----------

# produce df from translation dict for manual QA - this could be exported to csv for editing if needed
alb_found_names_translation_df = pd.DataFrame.from_dict(alb_found_names_translation_dict, orient='columns')
#alb_found_names_translation_df = pd.concat({k: pd.DataFrame(v, 'index') for k, v in alb_found_names_translation_dict.items()}, axis=0)
display(alb_found_names_translation_df)


# COMMAND ----------

# Remove found name from translation dict

def remove_found_name(current_organisation_name, organisation_instance_name, found_name_for_removal, alb_found_names_translation_dict):
    alb_found_names_translation_dict[current_organisation_name][organisation_instance_name].remove(found_name_for_removal)
    print(f'{found_name_for_removal} removed from current org: {current_organisation_name}, organisation instance: {organisation_instance_name}')
    print(f'This leaves the remaining found names: {alb_found_names_translation_dict[current_organisation_name][organisation_instance_name]}')


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

print(alb_found_names_translation_dict['Flood Re']['Flood Re'])

# COMMAND ----------

flood_re_wrong_names = ['119 FLOOD STREET LIMITED', 'LONDON FLOOD PREVENTION LTD', 'FLOOD STREET MANAGEMENT COMPANY LIMITED', '109 FLOOD STREET LIMITED', 'FLOOD STREET LIMITED', 'FLOODLIGHT LEISURE LIMITED', '24 HR FIRE & FLOOD ASSISTANCE LIMITED', '111 FLOOD STREET LIMITED', '115 FLOOD STREET LIMITED']
for name in flood_re_wrong_names:
    remove_found_name('Flood Re', 'Flood Re', name, alb_found_names_translation_dict)

# COMMAND ----------

seafish_wrong_names = ["BRIXHAM SEAFISH COMPANY LIMITED","SEAFISH IMPORTERS LIMITED","SEAFISH U.K. LIMITED"]
for name in seafish_wrong_names:
    remove_found_name('Seafish', 'Seafish', name, alb_found_names_translation_dict)

# COMMAND ----------

print(alb_found_names_translation_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add current and name instance columns to ccod data using QA'ed translation dict

# COMMAND ----------

# Use translation dict to add two new columns, populated with current and historic organisation names, based on matches with found names in the translation dict
for current_org_name, org_names in alb_found_names_translation_dict.items():
        for org_name, found_names in org_names.items():
            for found_name in found_names:
                ccod.loc[ccod['Proprietor Name (1)'] == found_name, 'Current_organisation'] = current_org_name
                ccod.loc[ccod['Proprietor Name (1)'] == found_name, 'historic_organisation'] = org_name

# COMMAND ----------

ccod_of_interest = (ccod[ccod['Current_organisation'].notna()])
display(ccod_of_interest)

# COMMAND ----------

ccod_of_interest.to_csv("/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/ccod_outputs/ccod_of_interest_defra_and_albs.csv")
