# Databricks notebook source
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
import pandas as pd
import matplotlib.lines as mlines
import numpy as np

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %run
# MAGIC ./constants

# COMMAND ----------

england_boundary = gpd.read_file('/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/LATEST_england_boundary/england_boundary_line.gpkg')

# COMMAND ----------

england_boundary

# COMMAND ----------

# display changes to organisations

organisation_translation_dict = {
    'Environment Agency': 'Environment Agency', 
    'Forestry Commission': 'Other',
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

# COMMAND ----------

alb_found_names_translation_dict.update({'Department for Environment, Food and Rural Affairs': None, 'Department for Environment, Food and Rural Affairs - managed by Forestry England or Forestry England': None})
organisations_of_interest = alb_found_names_translation_dict.keys()

# COMMAND ----------

# MAGIC %md
# MAGIC #### All defra land

# COMMAND ----------

all_defra_land = gpd.read_parquet(polygon_ccod_defra_path)

# COMMAND ----------

# add new column for plot display

all_defra_land['plot_organisation'] = all_defra_land['current_organisation']
all_defra_land['plot_organisation'] = all_defra_land['plot_organisation'].map(organisation_translation_dict)

all_defra_land['rank'] = all_defra_land['plot_organisation'].map(rank_dict)
all_defra_land = all_defra_land.sort_values(by='rank', ascending=True)

# COMMAND ----------

# for these area calculations, we're in DEFRA land in the context of who manages it. So setting the current organisation to reflect this for 
polygon_ccod_defra_fcfe = all_defra_land[all_defra_land['land_management_organisation']=='Forestry Commission or Forestry England']
polygon_ccod_defra_fcfe['plot_organisation'] = 'DEFRA (managed by Forestry England or Forestry England)'
polygon_ccod_defra_not_fcfe = all_defra_land[all_defra_land['land_management_organisation']!='Forestry Commission or Forestry England']
all_defra_land = pd.concat([polygon_ccod_defra_fcfe, polygon_ccod_defra_not_fcfe])

# COMMAND ----------

all_defra_land.plot_organisation.unique()

# COMMAND ----------

import matplotlib.patches as mpatches

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Report plot

# COMMAND ----------

fig, ax = plt.subplots(layout='constrained', figsize=(15,10))
ax.set_xticks([])
ax.set_yticks([])
ax = england_boundary.plot(legend=True, figsize=(20, 15), color='0.95', ax=ax)
print(ax)
colour_dict = {
    'DEFRA (managed by Forestry Commission or Forestry England)': 'forestgreen',
    'Department for Environment Food and Rural Affairs':'blue',
    'Environment Agency':'red',
    'Natural England':'gold',
    'National Park Authority':'rebeccapurple',
    'Royal Botanic Gardens, Kew': 'indianred',
    'Other': 'cornflowerblue'
 }
colors = ['forestgreen', 'blue', 'red', 'rebeccapurple', 'gold','cornflowerblue', 'indianred']#, 'lightpink', 'deepskyblue', 'yellow', 'indianred']
#ideally would be using colour dict values for this, but something is weird with the colour map order
# colors = colour_dict.values()
cmap1 = matplotlib.colors.ListedColormap(colors, name='from_list', N=None)
ax = all_defra_land.plot(column='plot_organisation', legend=True, figsize=(20, 15), cmap=cmap1, ax=ax)
#leg = ax.get_legend()
#print(leg)
#handles, labels = ax.get_legend_handles_labels()
# create custom legend swatches/labels
legend_patches = [mpatches.Patch(color=colour, label=organisation) for organisation, colour in colour_dict.items()]
leg = ax.legend(handles=legend_patches)
leg.get_frame().set_alpha(0)
leg.set_loc('upper left')
leg.frameon = False
#leg.set_bbox_to_anchor((0., 0., 1.5, 1.009))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Poster plot

# COMMAND ----------

from textwrap import fill

# COMMAND ----------

all_defra_land.geometry = all_defra_land.geometry.buffer(200)

# COMMAND ----------

fig, ax = plt.subplots(layout='constrained', figsize=(15,10))
ax.set_xticks([])
ax.set_yticks([])
ax = england_boundary.plot(legend=True, figsize=(20, 15), color='0.95', ax=ax)
print(ax)
colour_dict = {
    'Department for Environment Food and Rural Affairs':'blue',
    'DEFRA (managed by Forestry Commission or Forestry England)': 'forestgreen',
    'Environment Agency':'red',
    'Natural England':'gold',
    'National Park Authority':'rebeccapurple',
    'Royal Botanic Gardens, Kew': 'indianred',
    'Other': 'cornflowerblue'
 }
colors = ['forestgreen', 'blue', 'red', 'rebeccapurple', 'gold','cornflowerblue', 'indianred']
#ideally would be using colour dict values for this, but something is weird with the colour map order
# colors = colour_dict.values()
cmap1 = matplotlib.colors.ListedColormap(colors, name='from_list', N=None)
ax = all_defra_land.plot(column='plot_organisation', legend=True, figsize=(20, 15), cmap=cmap1, ax=ax)
#leg = ax.get_legend()
#print(leg)
#handles, labels = ax.get_legend_handles_labels()
# create custom legend swatches/labels
legend_patches = [mpatches.Patch(color=colour, label = fill(organisation, width=30)) for organisation, colour in colour_dict.items()]
leg = ax.legend(handles=legend_patches, fontsize=14, labelspacing=1.5)
leg.get_frame().set_alpha(0)
leg.set_loc('upper left')
leg.frameon = False
#leg.set_bbox_to_anchor((0., 0., 1.5, 1.009))

# COMMAND ----------

fig.savefig('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/outputs/poster_plot.png')

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

organisation_translation_dict = {
    'Environment Agency': 'Environment Agency', 
    'Forestry Commission': 'DEFRA (managed by Forestry Commission or Forestry England)',
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

# COMMAND ----------

# add new column for plot display

overlaps_non_defra_land['plot_organisation'] = overlaps_non_defra_land['current_organisation']
overlaps_non_defra_land['plot_organisation'] = overlaps_non_defra_land['plot_organisation'].map(organisation_translation_dict)

overlaps_non_defra_land['rank'] = overlaps_non_defra_land['plot_organisation'].map(rank_dict)
overlaps_non_defra_land = overlaps_non_defra_land.sort_values(by='rank', ascending=True)

# COMMAND ----------

overlaps_non_defra_land.sort_values('area_ha', ascending=False)

# COMMAND ----------

# convert geometry to centroid for graduated symbol plot
overlaps_non_defra_land.geometry = overlaps_non_defra_land.geometry.centroid

# COMMAND ----------



# COMMAND ----------

fig, ax = plt.subplots(layout='constrained', figsize=(17,11))
ax.set_xticks([])
ax.set_yticks([])
ax2 = england_boundary.plot(legend=True, figsize=(20, 15), color='0.95', ax=ax)
colors = ['forestgreen','blue', 'red', 'rebeccapurple', 'gold', 'deepskyblue', 'indianred']
#colors = ['blue', 'white', 'red', 'white','white','forestgreen', 'white', 'rebeccapurple', 'cadetblue', 'cornflowerblue', 'gold', 'lightpink', 'deepskyblue', 'yellow', 'indianred']
cmap1 = matplotlib.colors.ListedColormap(colors, name='from_list', N=None)
ax1 = overlaps_non_defra_land.plot(column='plot_organisation', legend=True, figsize=(40,20), cmap=cmap1, ax=ax, markersize=overlaps_non_defra_land.area_ha, alpha=0.4)
leg1 = ax1.get_legend()
leg1.get_frame().set_alpha(0)
leg1.set_loc('upper left')
leg1.frameon = False



# COMMAND ----------

plt.rcParams['legend.title_fontsize'] = 14

fig, ax = plt.subplots(layout='constrained', figsize=(17,11))
ax2 = england_boundary.plot(legend=True, figsize=(20, 15), color='0.95', ax=ax)
colors = ['forestgreen','blue', 'red', 'rebeccapurple', 'gold', 'deepskyblue', 'indianred']
cmap1 = matplotlib.colors.ListedColormap(colors, name='from_list', N=None)
ax1 = overlaps_non_defra_land.plot(column='plot_organisation', legend=True, figsize=(40,20), cmap=cmap1, ax=ax, markersize=overlaps_non_defra_land.area_ha, alpha=0.4)
ax.set_xticks([])
ax.set_yticks([])
leg1 = ax1.get_legend()
leg1.get_frame().set_alpha(0)
leg1.set_loc('upper left')
leg1.frameon = False
leg1.set_title('Organisation')

# some bins to indicate size in legend
bins = [10, 100, 1000]
# create second legend
ax.add_artist(
    ax.legend(
        handles=[
            mlines.Line2D(
                [],
                [],
                color="grey",
                lw=0,
                marker="o",
                markersize=np.sqrt(b),
                label=str(int(b)),
            )
            for i, b in enumerate(bins)
        ],
        loc=1,
        frameon=False,
        labelspacing=2,
        handleheight=1,
        handletextpad=2,
        title='Area (Ha)',
        #borderpad=0.1
    )
)
# restore original legend
ax.add_artist(leg1)
#leg.set_bbox_to_anchor((0., 0., 1.5, 1.009))
