# Databricks notebook source
'''
Script to pull together created area dfs (created in area calculations and data comparison scripts) and produce comparison tables
'''

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./constants

# COMMAND ----------

# get area df for produced dataset
polgyon_ccod_area_df = pd.read_csv()
# for epims
epims_area_df = pd.read_csv()
# for ALBs

