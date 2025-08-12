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
specific_department_identifiers = ['environment','food','rural', 'agriculture', 'resources', 'fisheries']

# COMMAND ----------

''' 
This is a hierarchical nested translation dict in the format:{
    'Current organisation name':{
        'Organisation name instance': [list of found names]
    }
}

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
    'Agriculture and Horticulture Development Board':{
        'Agriculture and Horticulture Development Board': [],
    },
    'Animal, Plant Health Agency':{
        'Animal Plant Health Agency': [],
        'Animal Health and Veterinary Laboratories Agency': [],
        'Animal Health': [],
        'Plant Health Inspectorate': [],
        'Plant Varieties and Seeds': [],
        'National Bee Unit': [],
        'GM Inspectorate': [], 
    },
    'Marine Management Organisation':{
        'Marine Management Organisation': [],
    },
    'Forestry Commission':{
        'Forestry Commission': [],
        'Forestry England': [],
        'Forestry Research': [],
        'Forest Enterprise': [],
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
