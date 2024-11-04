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
    'The Church Commissioners':{
        'The Church Commissioners': [],
        'Church Commissioners': [],
        'Ecclesiastical Commissioners': [],
        'Ecclesiastical and Church Estates Commissioners for England': [],
        'Queen Annes Bounty': [],
    },

}
