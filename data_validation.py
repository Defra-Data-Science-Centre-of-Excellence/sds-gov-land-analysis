# Databricks notebook source
# MAGIC %md
# MAGIC ### Data validation overlaps
# MAGIC Assess overlaps which exist in the data (for freehold only as leasehold overlap is expected), both within defra land and between defra and non-defra land.<br>
# MAGIC <b>Prerequisites:</b> Before running this step, ensure you have produced a filtered version of the nps-ccod data, for your titles of interest (ie. have run 'identify_land_parcels')

# COMMAND ----------

import geopandas as gpd
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

polygon_ccod_defra = gpd.read_parquet(polygon_ccod_defra_path)
polygon_ccod = gpd.read_parquet(polygon_ccod_path)

# COMMAND ----------

polygon_ccod_defra.geometry = polygon_ccod_defra.geometry.make_valid()

# COMMAND ----------

freehold_polygon_ccod_defra = polygon_ccod_defra[polygon_ccod_defra['Tenure']=='Freehold']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get total defra land area (dissolved and undissolved), for context

# COMMAND ----------

print(f'total area of defra and alb land: {polygon_ccod_defra.dissolve().area.sum()/10000}')

# COMMAND ----------

undissolved_defra_freehold_area = freehold_polygon_ccod_defra.area.sum()/10000
print(f'total area of defra and alb freehold land when nbot flattened/dissolveed: {undissolved_defra_freehold_area}')

# COMMAND ----------

dissolved_defra_freehold_area = freehold_polygon_ccod_defra.dissolve().area.sum()/10000
print(f'total area of defra and alb freehold land: {dissolved_defra_freehold_area}')

# COMMAND ----------

print(f'area of freeholdings overlapping within defra: {undissolved_defra_freehold_area - dissolved_defra_freehold_area}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get self overlap inside defra estate

# COMMAND ----------

overlaps = gpd.overlay(freehold_polygon_ccod_defra, freehold_polygon_ccod_defra, how='intersection')

# COMMAND ----------

overlaps[overlaps['POLY_ID_1']!=overlaps['POLY_ID_2']]

# COMMAND ----------

overlaps.area.sum()/10000

# COMMAND ----------

overlaps.dissolve().area.sum()/10000

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get overlap of freehold defra estate with freehold non-defra estate

# COMMAND ----------

# remove defra parcels from polygon ccod by title number
polygon_ccod_defra_titles = polygon_ccod_defra['Title Number'].unique()
polygon_ccod_defra_removed = polygon_ccod[~polygon_ccod['Title Number'].isin(polygon_ccod_defra_titles)]
freehold_polygon_ccod_defra_removed = polygon_ccod_defra_removed[polygon_ccod_defra_removed['Tenure'] == 'Freehold']

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unbuffered - so my include small technical overlaps

# COMMAND ----------

overlap_with_non_defra_estate = gpd.overlay(freehold_polygon_ccod_defra, freehold_polygon_ccod_defra_removed, how='intersection', make_valid=True)

# COMMAND ----------

overlap_with_non_defra_estate

# COMMAND ----------

overlap_with_non_defra_estate.to_parquet(overlap_with_non_defra_estate_path)

# COMMAND ----------

overlap_with_non_defra_estate.area.sum()/10000


# COMMAND ----------

overlap_with_non_defra_estate.dissolve().area.sum()/10000

# COMMAND ----------

len(overlap_with_non_defra_estate['Title Number_1'].unique())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Negative buffer - aiming to remove small technical overlaps

# COMMAND ----------

# change buffer value to play around with removing small technical overlaps, but not removing actual conflicts
buffered_freehold_polygon_ccod_defra = freehold_polygon_ccod_defra.copy()
buffered_freehold_polygon_ccod_defra.geometry = buffered_freehold_polygon_ccod_defra.geometry.buffer(-0.5)

# COMMAND ----------

buffered_overlaps = gpd.overlay(buffered_freehold_polygon_ccod_defra, freehold_polygon_ccod_defra_removed, how='intersection')

# COMMAND ----------

#buffered_overlaps.to_parquet('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/validation/overlap_with_non_defra_estate_buffer_01.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summarise proprietors with overlap

# COMMAND ----------

overlap_count_by_proprietor = pd.DataFrame(buffered_overlaps['Proprietor Name (1)_2'].value_counts()).reset_index(drop=False)

# COMMAND ----------

overlap_count_by_proprietor

# COMMAND ----------

overlap_area_by_proprietor = pd.DataFrame(buffered_overlaps.dissolve(by='Proprietor Name (1)_2').area/10000).reset_index(drop=False).sort_values(by='Proprietor Name (1)_2', ascending=False)

# COMMAND ----------

overlap_area_by_proprietor

# COMMAND ----------

overlap_count_by_proprietor = overlap_count_by_proprietor.rename(columns={'index': 'Proprietor Name (1)_2', 'Proprietor Name (1)_2': 'Count'})

# COMMAND ----------

overlap_count_by_proprietor

# COMMAND ----------

summary_of_conflicting_proprietors = pd.merge(overlap_area_by_proprietor, overlap_count_by_proprietor, how='outer', on = 'Proprietor Name (1)_2').sort_values(by=0, ascending=False)

# COMMAND ----------

summary_of_conflicting_proprietors = summary_of_conflicting_proprietors.rename(columns={'Proprietor Name (1)_2': 'Proprietor Name', 0: 'Area (Ha)'})

# COMMAND ----------

display(summary_of_conflicting_proprietors)

# COMMAND ----------

len(buffered_overlaps['Title Number_1'].unique())

# COMMAND ----------

buffered_overlaps.dissolve().area.sum()/10000

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get overlap of freehold defra estate with any leasehold estate

# COMMAND ----------

polygon_ccod_defra_titles = polygon_ccod_defra['Title Number'].unique()
polygon_ccod_defra_removed = polygon_ccod[~polygon_ccod['Title Number'].isin(polygon_ccod_defra_titles)]
leasehold_polygon_ccod = polygon_ccod_defra_removed[polygon_ccod_defra_removed['Tenure'] == 'Leasehold']

# COMMAND ----------

overlap_with_non_leasehold_defra_estate = gpd.overlay(freehold_polygon_ccod_defra, leasehold_polygon_ccod, how='intersection', make_valid=True)

# COMMAND ----------

overlap_with_non_leasehold_defra_estate

# COMMAND ----------

overlap_with_non_leasehold_defra_estate.area.sum()/10000

# COMMAND ----------

overlap_with_non_leasehold_defra_estate.dissolve().area.sum()/10000

# COMMAND ----------

leasehold_overlap_area_by_proprietor = pd.DataFrame(overlap_with_non_leasehold_defra_estate.dissolve(by='Proprietor Name (1)_2').area/10000).reset_index(drop=False).sort_values(by=0, ascending=False)

# COMMAND ----------

display(leasehold_overlap_area_by_proprietor)

# COMMAND ----------

leasehold_overlap_area_by_defra_proprietor = pd.DataFrame(overlap_with_non_leasehold_defra_estate.dissolve(by=['current_organisation', 'land_management_organisation'], dropna=False).area/10000).reset_index(drop=False).sort_values(by=0, ascending=False)

# COMMAND ----------

display(leasehold_overlap_area_by_defra_proprietor)

# COMMAND ----------

leasehold_undissolved_overlap_area_by_proprietor = pd.DataFrame(overlap_with_non_leasehold_defra_estate.dissolve(by='Proprietor Name (1)_2').area/10000).reset_index(drop=False).sort_values(by=0, ascending=False)
