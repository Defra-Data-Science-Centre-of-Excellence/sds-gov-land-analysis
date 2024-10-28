# Databricks notebook source
# MAGIC %md
# MAGIC ### Plotter
# MAGIC Produce spatial plots of the land identified as owned by the organisations of interest. <br>
# MAGIC <b> Prerequisites: </b> Before running this step, ensure you have produced a filtered version of the nps-ccod data, for your titles of interest (ie. have run 'identify_land_parcels') <br>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install geodatasets

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install contextily

# COMMAND ----------

import geopandas as gpd
import matplotlib.pyplot as plt
import geodatasets
import contextily as cx
import matplotlib.colors

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

england_boundary = gpd.read_file('/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/LATEST_england_boundary/england_boundary_line.gpkg')

# COMMAND ----------

england_boundary

# COMMAND ----------

# MAGIC %md
# MAGIC #### All defra land

# COMMAND ----------

all_defra_land = gpd.read_file(polygon_ccod_defra_path)

# COMMAND ----------

all_defra_land.current_organisation.unique()

# COMMAND ----------

# add new column for plot display
organisation_translation_dict = {
    'Environment Agency': 'Environment Agency', 
    'Forestry Commission': 'Forestry Commission',
    'Peak District National Park Authority': 'National Park Authority', 
    'Natural England': 'Natural England',
    'Lake District National Park Authority': 'National Park Authority',
    'Department for Environment, Food and Rural Affairs': 'Department for Environment, Food and Rural Affairs',
    'North York Moors National Park Authority': 'National Park Authority',
    'National Forest Company': 'Other', 
    'Broads Authority': 'Other',
    'Northumberland National Park Authority': 'National Park Authority',
    'Covent Garden Market Authority': 'Other', 
    'Exmoor National Park Authority': 'National Park Authority',
    'Dartmoor National Park Authority': 'National Park Authority',
    'Yorkshire Dales National Park Authority': 'National Park Authority',
    'South Downs National Park Authority': 'National Park Authority',
    'New Forest National Park Authority': 'National Park Authority',
    'Royal Botanic Gardens Kew': 'Royal Botanic Gardens Kew',
    'Agriculture and Horiculture Development Board': 'Other'
}

rank_dict = {
    'Department for Environment, Food and Rural Affairs': 0,
    'Environment Agency': 1,
    'Forestry Commission': 2,
    'Royal Botanic Gardens Kew': 3,
    'National Park Authority': 4,
    'Natural England': 5,
    'Other': 6
}

all_defra_land['plot_organisation'] = all_defra_land['current_organisation']
all_defra_land['plot_organisation'] = all_defra_land['plot_organisation'].map(organisation_translation_dict)

all_defra_land['rank'] = all_defra_land['plot_organisation'].map(rank_dict)
all_defra_land = all_defra_land.sort_values(by='rank', ascending=True)

# COMMAND ----------

fig, ax = plt.subplots(layout='constrained', figsize=(15,10))
ax2 = england_boundary.plot(legend=True, figsize=(20, 15), color='0.95', ax=ax)
colors = ['blue', 'white', 'red', 'white','white','forestgreen', 'white', 'rebeccapurple', 'cadetblue', 'cornflowerblue', 'gold', 'lightpink', 'deepskyblue', 'yellow', 'indianred']
cmap1 = matplotlib.colors.ListedColormap(colors, name='from_list', N=None)
ax1 = all_defra_land.plot(column='plot_organisation', legend=True, figsize=(20, 15), cmap=cmap1, ax=ax)
leg = ax1.get_legend()
leg.get_frame().set_alpha(0)
leg.set_loc('upper left')
leg.frameon = False
#leg.set_bbox_to_anchor((0., 0., 1.5, 1.009))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Overlaps - graduated symbol spatial plot

# COMMAND ----------

overlaps_non_defra_land = gpd.read_parquet('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/validation/overlap_with_non_defra_estate_buffer_05.parquet')

# COMMAND ----------

overlaps_non_defra_land = overlaps_non_defra_land.explode(drop=True)

# COMMAND ----------

# get area
overlaps_non_defra_land['area_ha'] = overlaps_non_defra_land.area/10000

# COMMAND ----------

overlaps_non_defra_land.geom_type.unique()

# COMMAND ----------

# convert geometry to centroid for graduated symbol plot
overlaps_non_defra_land.geometry = overlaps_non_defra_land.geometry.centroid

# COMMAND ----------

fig, ax = plt.subplots(layout='constrained', figsize=(17,11))
ax2 = england_boundary.plot(legend=True, figsize=(20, 15), color='0.95', ax=ax)
colors = ['saddlebrown', 'deeppink', 'palevioletred', 'darkorange', 'red', 'palegreen', 'forestgreen', 'mediumorchid', 'mediumvioletred', 'rebeccapurple', 'cornflowerblue', 'gold', 'lightpink', 'deepskyblue', 'yellow', 'indianred']
cmap1 = matplotlib.colors.ListedColormap(colors, name='from_list', N=None)
ax1 = overlaps_non_defra_land.plot(column='current_organisation', legend=True, figsize=(40,20), cmap=cmap1, ax=ax, markersize=overlaps_non_defra_land.area_ha, alpha=0.4)
leg = ax1.get_legend()
leg.get_frame().set_alpha(0)
leg.set_loc('upper left')
leg.frameon = False
#leg.set_bbox_to_anchor((0., 0., 1.5, 1.009))
