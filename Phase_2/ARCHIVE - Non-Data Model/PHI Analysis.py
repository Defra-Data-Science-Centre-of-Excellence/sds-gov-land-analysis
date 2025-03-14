# Databricks notebook source
# MAGIC %pip install keplergl pydeck folium matplotlib mapclassify rtree pygeos geopandas==1.0.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from geopandas import GeoDataFrame, GeoSeries
import geopandas as gpd
import re
from glob import glob
import os
import pandas as pd
import rtree
import pygeos

import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

outdir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Intersection/'

# COMMAND ----------

defra_land_input = gpd.read_parquet("/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/phase_one_final_report_outputs/polygon_ccod_defra_by_organisation_tenure.parquet")

# COMMAND ----------

dgl_area = (defra_land_input.geometry.area/10000).sum()

# COMMAND ----------

dgl_phi = gpd.read_parquet('/dbfs'+outdir+'dgl_phi_gpd.parquet')

# COMMAND ----------

dgl_phi.display()

# COMMAND ----------

phi_habs = pd.unique(dgl_phi["Main_Habit"]).tolist()
len(phi_habs)

# COMMAND ----------

phi_area = []

# COMMAND ----------

phi_area = []

for hab in phi_habs:

    clipped_hab_subset = dgl_phi.loc[dgl_phi["Main_Habit"] == hab]
    clipped_area = clipped_hab_subset["ovrlp_area_ha"].sum()

    freehold = clipped_hab_subset.loc[clipped_hab_subset["Tenure"] == "Freehold"]
    freehold_area = freehold["ovrlp_area_ha"].sum()

    leasehold = clipped_hab_subset.loc[clipped_hab_subset["Tenure"] == "Leasehold"]
    leasehold_area = leasehold["ovrlp_area_ha"].sum()

    ne = clipped_hab_subset.loc[clipped_hab_subset["current_organisation"] == "Natural England"]
    ne_area = ne["ovrlp_area_ha"].sum()

    ea = clipped_hab_subset.loc[clipped_hab_subset["current_organisation"] == "Environment Agency"]
    ea_area = ea["ovrlp_area_ha"].sum()

    defra = clipped_hab_subset.loc[clipped_hab_subset["current_organisation"] == "Department for Environment, Food and Rural Affairs"]
    defra_defra = defra.loc[defra["land_management_organisation"] != "Forestry Commission or Forestry England"]
    defra_fc = defra.loc[defra["land_management_organisation"] == "Forestry Commission or Forestry England"]
    defra_area = defra_defra["ovrlp_area_ha"].sum()
    defra_fc_area = defra_fc["ovrlp_area_ha"].sum()

    npa_list = ['Dartmoor National Park Authority',
                'Exmoor National Park Authority',
                'South Downs National Park Authority',
                'Peak District National Park Authority',
                'Lake District National Park Authority',
                'Yorkshire Dales National Park Authority',
                'North York Moors National Park Authority', 'Broads Authority',
                'Northumberland National Park Authority',
                'New Forest National Park Authority']
    npa = clipped_hab_subset.loc[clipped_hab_subset["current_organisation"].isin(npa_list)]
    npa_area = npa["ovrlp_area_ha"].sum()

    kew = clipped_hab_subset.loc[clipped_hab_subset["current_organisation"] == "Royal Botanic Gardens Kew"]
    kew_area = kew["ovrlp_area_ha"].sum()

    forest = clipped_hab_subset.loc[clipped_hab_subset["current_organisation"] == "National Forest Company"]
    forest_area = forest["ovrlp_area_ha"].sum()

    phi_area.append((hab, clipped_area, freehold_area, leasehold_area, ne_area, ea_area, defra_area, defra_fc_area, npa_area, kew_area, forest_area))

# COMMAND ----------

phi_area_df = pd.DataFrame(phi_area, columns=["hab", "area_sum", "freehold_area", "leasehold_area","ne_area","ea_area","defra_area","defra_fc_area","npa_area","kew_area","forest_area"])

# COMMAND ----------

phi_area_df

# COMMAND ----------

phi_area_df['area_perc'] = (phi_area_df['area_sum'] / dgl_area) *100
phi_area_df['fh_perc'] = (phi_area_df['freehold_area'] / dgl_area) *100
phi_area_df['lh_perc'] = (phi_area_df['leasehold_area'] / dgl_area) *100
phi_area_df['ne_perc'] = (phi_area_df['ne_area'] / dgl_area) *100
phi_area_df['ea_perc'] = (phi_area_df['ea_area'] / dgl_area) *100
phi_area_df['defra_perc'] = (phi_area_df['defra_area'] / dgl_area) *100
phi_area_df['defra_fc_perc'] = (phi_area_df['defra_fc_area'] / dgl_area) *100
phi_area_df['npa_perc'] = (phi_area_df['npa_area'] / dgl_area) *100
phi_area_df['kew_perc'] = (phi_area_df['kew_area'] / dgl_area) *100
phi_area_df['forest_perc'] = (phi_area_df['forest_area'] / dgl_area) *100

# COMMAND ----------

na_area = dgl_area - phi_area_df['area_sum'].sum()
na_perc = (na_area/dgl_area)*100

new_row = pd.DataFrame({
    "hab": ["No Overlap"],
    "area_sum": [na_area],
    "area_perc": [na_perc]
})

# COMMAND ----------

phi_area_df = pd.concat([phi_area_df, new_row], ignore_index=True)

# COMMAND ----------

phi_area_df_sorted = phi_area_df.sort_values(by="area_perc", ascending=False)
phi_area_df_sorted = phi_area_df_sorted.reset_index(drop=True)

# COMMAND ----------

phi_area_df_sorted

# COMMAND ----------

plotting_df = phi_area_df_sorted.head(14)

# COMMAND ----------

plotting_df

# COMMAND ----------

fig, ax = plt.subplots()

y_pos = np.arange(len(plotting_df))

col_fh = plotting_df.iloc[:, -9]  
col_lh = plotting_df.iloc[:, -8]

bars1 = ax.barh(y_pos, col_fh, align='center', label="Freehold", color='forestgreen')

bars2 = ax.barh(y_pos, col_lh, align='center', left=col_fh, label="Leasehold", color='darkorange')

ax.tick_params(axis='y', labelsize=9)
ax.set_yticks(y_pos, labels=plotting_df['hab'])
ax.invert_yaxis()
ax.set_xlabel('Percentage of Defra Group Estate Covered')
ax.set_title('Defra Group Estate Land Cover: PHI Main Habitat (Top 14)\nSplit by Freehold and Leasehold Tenure')
ax.set_xlim([0, 17]) 

for i in range(len(plotting_df)):
    total_perc = plotting_df['area_perc'][i]
    ax.text(col_fh[i] + col_lh[i] + 0.3, y_pos[i], f'{total_perc:.2f}%', 
            va='center', ha='left', color='black')
    
ax.legend()

fig.text(0.5, -0.01, 'Note: 58.94% of Defra Group Estate does not overlap PHI.', 
         ha='center', va='center', fontsize=10, color="darkred")

plt.show()

# COMMAND ----------

fig, ax = plt.subplots()

y_pos = np.arange(len(plotting_df))

col_ne = plotting_df.iloc[:, -7]  
col_ea = plotting_df.iloc[:, -6]
col_defra = plotting_df.iloc[:, -5]  
col_defra_fc = plotting_df.iloc[:, -4] 
col_npa = plotting_df.iloc[:, -3]
col_kew = plotting_df.iloc[:, -2]  
col_forest = plotting_df.iloc[:, -1]

bars1 = ax.barh(y_pos, col_defra, align='center', label="Defra", color='black')
bars1 = ax.barh(y_pos, col_defra_fc, align='center', left=col_defra, label="Defra & Forestry Commission/England", color='darkgreen')
bars2 = ax.barh(y_pos, col_ea, align='center', left=col_defra+col_defra_fc, label="Environment Agency", color='darkorange')
bars3 = ax.barh(y_pos, col_ne, align='center', left=col_defra+col_defra_fc+col_ea,label="Natural England", color='royalblue')
bars4 = ax.barh(y_pos, col_npa, align='center', left=col_defra+col_defra_fc+col_ea+col_ne, label="National Park Authorities", color='tomato')
bars5 = ax.barh(y_pos, col_forest, align='center', left=col_defra+col_defra_fc+col_ea+col_ne+col_npa,label="National Forest Company", color='darkmagenta')
bars6 = ax.barh(y_pos, col_kew, align='center', left=col_defra+col_defra_fc+col_ea+col_ne+col_npa+col_forest, label="Royal Botanic Gardens Kew", color='powderblue')

ax.tick_params(axis='y', labelsize=9)
ax.set_yticks(y_pos, labels=plotting_df['hab'])
ax.invert_yaxis()
ax.set_xlabel('Percentage of Defra Group Estate Covered')
ax.set_title('Defra Group Estate Land Cover: PHI Main Habitat (Top 14)\nSplit by Management Organisation')
ax.set_xlim([0, 17]) 

for i in range(len(plotting_df)):
    total_perc = plotting_df['area_perc'][i]
    ax.text(col_ne[i] + col_ea[i] + col_defra[i] + col_defra_fc[i] + col_npa[i] + col_kew[i] + col_forest[i] + 0.3, y_pos[i], f'{total_perc:.2f}%', 
            va='center', ha='left', color='black')
    
ax.legend()

fig.text(0.5, -0.01, 'Note: 58.94% of Defra Group Estate does not overlap PHI.', 
         ha='center', va='center', fontsize=10, color="darkred")

plt.show()

# COMMAND ----------

defra_land_input

# COMMAND ----------

# Group by current_organisation and Tenure, summing the area
summary = defra_land_input.groupby(['current_organisation', 'Tenure'], as_index=False)['area_ha'].sum()

# Pivot the summary DataFrame to get area for Freehold and Leasehold as separate columns
pivot_df = summary.pivot(index='current_organisation', columns='Tenure', values='area_ha')

# Reset the index to turn the index into a column
pivot_df.reset_index(inplace=True)

# Fill NaN values with 0
pivot_df.fillna(0, inplace=True)

# Rename the columns for better clarity
pivot_df.columns.name = None  # Remove the category name
pivot_df.columns = ['organisation', 'area_freehold', 'area_leasehold']

# COMMAND ----------

pivot_df['fh_perc'] = (pivot_df['area_freehold'] / (pivot_df['area_freehold'] + pivot_df['area_leasehold'])) *100
pivot_df['lh_perc'] = (pivot_df['area_leasehold'] / (pivot_df['area_freehold'] + pivot_df['area_leasehold'])) *100

# COMMAND ----------

pivot_df

# COMMAND ----------

pivot_df = pivot_df.sort_values(by="area_leasehold", ascending=False)
pivot_df = pivot_df.reset_index(drop=True)

# COMMAND ----------

fig, ax = plt.subplots()

y_pos = np.arange(len(pivot_df))

col_fh = pivot_df.iloc[:, -2]  
col_lh = pivot_df.iloc[:, -1]

bars1 = ax.barh(y_pos, col_fh, align='center', label="Freehold", color='forestgreen')

bars2 = ax.barh(y_pos, col_lh, align='center', left=col_fh, label="Leasehold", color='darkorange')

ax.tick_params(axis='y', labelsize=9)
ax.set_yticks(y_pos, labels=pivot_df['organisation'])
ax.invert_yaxis()
ax.set_xlabel('Percentage of Land Freehold/Leasehold')
ax.set_title('Defra Group Estate Land Owners: Freehold - Leasehold Split\nOrdered by Leasehold area')
ax.set_xlim([0, 125]) 
ax.set_xticks(np.arange(0, 101, 20))

for i in range(len(pivot_df)):
    total_area = pivot_df['area_freehold'][i] + pivot_df['area_leasehold'][i]
    ax.text(col_fh[i] + col_lh[i] + 1, y_pos[i], f'{total_area:,.1f} ha', 
            va='center', ha='left', color='black')
    
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.13), ncol=2)

plt.show()

# COMMAND ----------


