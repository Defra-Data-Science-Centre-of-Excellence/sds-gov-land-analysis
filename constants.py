# Databricks notebook source
# MAGIC %md
# MAGIC ### Constants
# MAGIC Defining variables which are useful throughout the workflow.
# MAGIC This script is designed to be run from other scripts, so that variables can be defined once and changed easily.

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
    'Defense Equipment and Support':{
        'Defense Equipment and Support': [],
    },
    'Defence Science and Technology Laboratory':{
        'Defence Science and Technology Laboratory': [],
    },
    'Submarine Delivery Agency':{
        'Submarine Delivery Agency': [],
    },
    'UK Hydrographic Office':{
        'UK Hydrographic Office': [],
    },
    'Armed Forces Covenant Fund Trust':{
        'Armed Forces Covenant Fund Trust': [],
    },
    'Atomic Weapons Establishment':{
        'Atomic Weapons Establishment': [],
    },
    'National Army Museum':{
        'National Army Museum': [],
    },
    'National Museum of the Royal Navy':{
        'National Museum of the Royal Navy': [],
    },
    'Royal Airforce Museum':{
        'Royal Airforce Museum': [],
    },
    'Single Source Regulations Office':{
        'Single Source Regulations Office': [],
    },
    'Advisory Committee on Conscientious Objectors':{
        'Advisory Committee on Conscientious Objectors': [],
    },
    'Armed Forces Pay Review Body':{
        'Armed Forces Pay Review Body': [],
    },
    'Independent Medical Expert Group':{
        'Independent Medical Expert Group': [],
    },
    'Nuclear Research Advisory Council':{
        'Nuclear Research Advisory Council': [],
    },
    'Scientific Advisory Committee on the Medical Implications of Less-Lethal Weapons':{
        'Scientific Advisory Committee on the Medical Implications of Less-Lethal Weapons': [],
    },
    'Veterans Advisory and Pensions Committees':{
        'Veterans Advisory and Pensions Committees': [],
    },  
    # check this is okay
    'The Oil and Pipelines Agency':{
        'The Oil and Pipelines Agency': [],
    },
    'Sheffield Forgemasters International Ltd':{
        'Sheffield Forgemasters International Ltd': [],
    },
    'Advisory Group on Military and Emergency Response Medicine':{
        'Advisory Group on Military and Emergency Response Medicine': [],
    },
    'Central Advisory Committee on Compensation':{
        'Central Advisory Committee on Compensation': [],
    },
    'Defence Nuclear Safety Expert Committee':{
        'Defence Nuclear Safety Expert Committee': [],
    },
    'Defence and Security Media Advisory Committee':{
        'Defence and Security Media Advisory Committee': [],
    },
    'Reserve Forces\' and Cadets\' Associations (RFCA)':{
        'Reserve Forces\' and Cadets\' Associations (RFCA)': [],
        'Reserve Forces\' and Cadets\' Associations': [],
        'The Territotial Army': [],
        'Territorial and Army Volunteer Reserve': [],
        'The Territorial Force': [],
    },
    'Service Complaints Ombudsman':{
        'Service Complaints Ombudsman': [],
    },
    'Service Prosecuting Authority':{
        'Service Prosecuting Authority': [],
    },
    'Defence Lands Service':{
        'Defence Lands Service': [],
    },
    'Defence Council Instruction':{
        'Defence Council Instruction': [],
    },
    'The Comptroller of Defence Lands (CDL)':{
        'The Comptroller of Defence Lands (CDL)': [],
        'The Comptroller of Defence Lands': [],
    },
    'Chief Surveyor of Defence Lands (CSDL)':{
        'Chief Surveyor of Defence Lands (CSDL)': [],
        'Chief Surveyor of Defence Lands': [],
    }
}
