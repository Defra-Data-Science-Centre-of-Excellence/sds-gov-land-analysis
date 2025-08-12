# Databricks notebook source
# MAGIC %md
# MAGIC ### Constants
# MAGIC Defining variables which are useful throughout the workflow.
# MAGIC This script is designed to be run from other scripts, so that variables can be defined once and changed easily.

# COMMAND ----------

# Set the name of the department of interest. The name set here will be used later to tag identified titles. So ensure you are happy with the format for your output.
standardised_department_name = 'Department for Environment, Food and Rural Affairs'

# generic department identifiers, these shouldn't need changing
generic_department_identifiers = ['state', 'secretary', 'ministry', 'minister', 'department']

# department specific identifiers
# this should be in the format ['name'] if only one identifier provided, otherwise in the format ['name_1', 'name_2', 'name_X'] if more than one identifier is provided
specific_department_identifiers = ['justice']

# COMMAND ----------

''' 
This is a hierarchical nested translation dict in the format:{
    'Current organisation name':{
        'Organisation name instance': [list of found names]
    }
}
Lines 3-5 above can be copied as a template for an additional organisation if required.

Current organisation name: This is the standardised version of the organisation name. This should be set for organisation and will be used to tag identified identified land parcels in the outputs. So ensure you are happy with the format before proceding.
organisation name instance (current or historic) is split by word and used for searching.
Organisation name instance: These are the names which will be searched for in the UK company proprietorship data. Both the 

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
    }

}
