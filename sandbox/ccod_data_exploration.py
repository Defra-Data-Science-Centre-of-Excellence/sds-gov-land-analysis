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

import numpy as np
# find some defra data for reference using or-and-or method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(['secretary','state','minister','ministry','department']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(['environment','food','rural']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
print(ccod_filtered.size)
display(ccod_filtered['Proprietor Name (1)'].unique())
#np.savetxt('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/defra_names.csv',ccod_filtered['Proprietor Name (1)'].unique(), delimiter=',', fmt='%s')
#csv = pd.read_csv('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/defra_names.csv', sep=',')
#display(csv)

# COMMAND ----------

# find some environment agency data for reference using or-and-or method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(['agency']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(['environment']), case=False, na=False)]
# Should probably display Proprietor names removed due to corporate body filter
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
print(ccod_filtered.size)
print(ccod_filtered['Proprietor Name (1)'].unique())
ccod_filtered.head(20)

# COMMAND ----------

# list of ALB/department names of interest - construct dict where values are alternative name options?
cs_department_identifiers = ['state', 'secretary', 'ministry', 'minister', 'department']
cs_department_names = {
    'Department for Environment, Food and Rural Affairs':
        ['environement', 'food', 'rural']
}
alb_names = {
    'Natural England': ['English Nature', 'Countryside Agency', 'Rural Development Service', 'Nature Conservancy Council'],
    'Joint Nature Conservation Committee': [],
    'Environment Agency': [],
    'Rural Payments Agency': [],
    'Royal Botanic Gardens, Kew': [],
    'Agriculture and Horiculture Development Board': [],
    'Animal, Plant Health Agency': [],
    'Marine Management Organisation': [],
    'Forestry Commission': ['Forestry England'],
}

# COMMAND ----------

cs_department_translation_dict = {
    'Department for Environment, Food and Rural Affairs':
}

# COMMAND ----------

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
    'Animal Plant Health Agency':{
        'Animal, Plant Health Agency': [],
    },
    'Marine Management Organisation':{
        'Marine Management Organisation': [],
    },
    'Forestry Commission':{
        'Forestry Commission': [],
        'Forestry England': [],
        'Forestry Research': [],
    },
}

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
            alb_found_names_translation_dict[current_org][org_name] = found_names.tolist()
            # identify titles registed under historic names and tag with current and historic organisations
print(alb_found_names_translation_dict)

# COMMAND ----------

# produce df from translation dict for manual QA - this could be exported to csv for editing if needed
alb_found_names_translation_df = pd.DataFrame(alb_found_names_translation_dict)
alb_found_names_translation_df.head(20)


# COMMAND ----------

# find some environment agency data for reference using or-and-or method
regex_str = r'^{}'
expression = '(?=.*{})'
for current_org, org_names in alb_names.items():
    # identify titles registered under current name and tag with current and current organisation
    compiled_regex_str = regex_str.format(''.join(expression.format(word) for word in current_org.split(' ')))
    ccod.loc[ccod['Proprietor Name (1)'].str.contains(f'{compiled_regex_str}', case=False, na=False),'organisation']=current_org
    ccod.loc[ccod['Proprietor Name (1)'].str.contains(f'{compiled_regex_str}', case=False, na=False),'current_organisation']=current_org
    # identify titles registed under historic names and tag with current and historic organisations
    for historic_org in org_names:
        compiled_regex_str = regex_str.format(''.join(expression.format(word) for word in historic_org.split(' ')))
        ccod.loc[ccod['Proprietor Name (1)'].str.contains(f'{compiled_regex_str}', case=False, na=False),'organisation']=historic_org
        ccod.loc[ccod['Proprietor Name (1)'].str.contains(f'{compiled_regex_str}', case=False, na=False),'current_organisation']=current_org
ccod[ccod['organisation'].notnull()].head()  


# COMMAND ----------

ccod_identified = ccod[ccod['organisation'].notnull()]
print(ccod_identified['Proprietor Name (1)'].unique())

# COMMAND ----------

# find some natural england data for reference using 'or' method
ccod_filtered = ccod.loc[ccod['Proprietor Name (1)'].str.contains('|'.join(['natural']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietor Name (1)'].str.contains('|'.join(['england']), case=False, na=False)]
ccod_filtered = ccod_filtered.loc[ccod_filtered['Proprietorship Category (1)'].str.contains('Corporate Body', case=False, na=False)]
print(ccod_filtered.size)
print(ccod_filtered['Proprietor Name (1)'].unique())
ccod_filtered.head(20)

# COMMAND ----------

postcodes = ccod.assign(
    postcode=ccod["pr_address"].str.extract(r"([a-z]{1,2}[\d]{1,2}[a-z]{0,1} [\d]{1}[a-z]{2})$")
    )

# COMMAND ----------

# 
