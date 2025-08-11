# Databricks notebook source
# MAGIC %md
# MAGIC ### Data comparison area
# MAGIC Script to compare identified proprietor names for defra to proprietor names previously produced for 30x30 work

# COMMAND ----------

# import pandas as pd

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
