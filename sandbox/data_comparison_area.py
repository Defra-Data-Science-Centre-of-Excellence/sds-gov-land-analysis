# Databricks notebook source
'''
Script to pull together created area dfs (created in area calculations and data comparison scripts) and produce comparison tables
'''

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

# COMMAND ----------

# change the pandas float display for easy reading
pd.options.display.float_format = '{:.2f}'.format

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./constants

# COMMAND ----------

# get area df for produced dataset
polygon_ccod_area_df = pd.read_csv(csv_area_df_polygon_ccod_defra_path)
# for epims
epims_area_df = pd.read_csv(csv_area_df_epims_path)
# for ALBs


# COMMAND ----------

polygon_ccod_area_df = polygon_ccod_area_df[['organisation', 'total_area', 'freehold_area', 'leasehold_area']]

# COMMAND ----------

epims_area_df = epims_area_df[['organisation', 'total_area','land_area', 'land_buildings_area']]

# COMMAND ----------

epims_area_comparison = pd.merge(polygon_ccod_area_df, epims_area_df, how='outer', on=['organisation', 'organisation'], suffixes=['_hmlr','_epims'])
epims_area_comparison

# COMMAND ----------

epims_area_comparison['total_area_comparison'] = epims_area_comparison['total_area_hmlr'] - epims_area_comparison['total_area_epims']
epims_area_comparison = epims_area_comparison.sort_values(by='total_area_comparison', key=abs, ascending=False)
display(epims_area_comparison)

# COMMAND ----------

fig = None
ax = None

# COMMAND ----------

# total area plot
fig, ax1 = plt.subplots(figsize=(5, 10))
total_area_bar = ax1.barh(epims_area_comparison.organisation, (epims_area_comparison.total_area_comparison/epims_area_comparison.total_area_hmlr)*100)
ax1.set_xlabel('Percentage difference in area')
ax1.set_title('Percentage difference from HMLR total area compared to EPIMS area', loc='right')
ax1.set_xlim(left=-1000, right=1000)
#ax1.set_xticks(ticks=[-1000, -900])
ax1.set_xscale('linear')
no_change_line = ax1.hlines(y=15, xmin=-1.5, xmax=1.5, linewidth=1000, colors='0.5')
plt.show()
