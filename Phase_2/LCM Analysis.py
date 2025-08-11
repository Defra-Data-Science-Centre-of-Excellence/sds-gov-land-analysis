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

dgl_lcm = gpd.read_parquet('/dbfs'+outdir+'dgl_lcm_gpd.parquet')

# COMMAND ----------

lcm_dict = {
    1: 'Deciduous woodland',
    2: 'Coniferous woodland',
    3: 'Arable',
    4: 'Improved grassland',
    5: 'Neutral grassland',
    6: 'Calcareous grassland',
    7: 'Acid grassland',
    8: 'Fen',
    9: 'Heather',
    10: 'Heather grassland',
    11: 'Bog',
    12: 'Inland rock',
    13: 'Saltwater',
    14: 'Freshwater',
    15: 'Supralittoral rock',
    16: 'Supralittoral sediment',
    17: 'Littoral rock',
    18: 'Littoral sediment',
    19: 'Saltmarsh',
    20: 'Urban',
    21: 'Suburban'
}

# COMMAND ----------

dgl_lcm['hab'] = dgl_lcm['f_mode'].map(lcm_dict)

# COMMAND ----------

dgl_lcm.display()

# COMMAND ----------

lcm_habs = pd.unique(dgl_lcm["hab"]).tolist()
len(lcm_habs)

# COMMAND ----------

lcm_area = []

for hab in lcm_habs:

    clipped_hab_subset = dgl_lcm.loc[dgl_lcm["hab"] == hab]
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

    lcm_area.append((hab, clipped_area, freehold_area, leasehold_area, ne_area, ea_area, defra_area, defra_fc_area, npa_area, kew_area, forest_area))

# COMMAND ----------

lcm_area_df = pd.DataFrame(lcm_area, columns=["hab", "area_sum", "freehold_area", "leasehold_area","ne_area","ea_area","defra_area","defra_fc_area","npa_area","kew_area","forest_area"])

# COMMAND ----------

lcm_area_df

# COMMAND ----------

lcm_area_df['area_perc'] = (lcm_area_df['area_sum'] / dgl_area) *100
lcm_area_df['fh_perc'] = (lcm_area_df['freehold_area'] / dgl_area) *100
lcm_area_df['lh_perc'] = (lcm_area_df['leasehold_area'] / dgl_area) *100
lcm_area_df['ne_perc'] = (lcm_area_df['ne_area'] / dgl_area) *100
lcm_area_df['ea_perc'] = (lcm_area_df['ea_area'] / dgl_area) *100
lcm_area_df['defra_perc'] = (lcm_area_df['defra_area'] / dgl_area) *100
lcm_area_df['defra_fc_perc'] = (lcm_area_df['defra_fc_area'] / dgl_area) *100
lcm_area_df['npa_perc'] = (lcm_area_df['npa_area'] / dgl_area) *100
lcm_area_df['kew_perc'] = (lcm_area_df['kew_area'] / dgl_area) *100
lcm_area_df['forest_perc'] = (lcm_area_df['forest_area'] / dgl_area) *100

# COMMAND ----------

lcm_area_df_sorted = lcm_area_df.sort_values(by="area_perc", ascending=False)
lcm_area_df_sorted = lcm_area_df_sorted.reset_index(drop=True)

# COMMAND ----------

lcm_area_df_sorted

# COMMAND ----------

plotting_df = lcm_area_df_sorted.head(15)

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
ax.set_title('Defra Group Estate Land Cover: UKCEH Land Cover Map 2021 Habitat\nSplit by Freehold and Leasehold Tenure')
ax.set_xlim([0, 52]) 

for i in range(len(plotting_df)):
    total_perc = plotting_df['area_perc'][i]
    ax.text(col_fh[i] + col_lh[i] + 0.3, y_pos[i], f'{total_perc:.2f}%', 
            va='center', ha='left', color='black')
    
ax.legend(loc='lower right', bbox_to_anchor=(1, 0), fontsize=10)

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
ax.set_title('Defra Group Estate Land Cover: UKCEH Land Cover Map 2021 Habitat\nSplit by Management Organisation')
ax.set_xlim([0, 52]) 

for i in range(len(plotting_df)):
    total_perc = plotting_df['area_perc'][i]
    ax.text(col_ne[i] + col_ea[i] + col_defra[i] + col_defra_fc[i] + col_npa[i] + col_kew[i] + col_forest[i] + 0.3, y_pos[i], f'{total_perc:.2f}%', 
            va='center', ha='left', color='black')
    
ax.legend()

plt.show()

# COMMAND ----------


