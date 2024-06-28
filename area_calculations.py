# Databricks notebook source
import geopandas as gpd
import pandas as pd
import numpy as np
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

# MAGIC %md
# MAGIC ##### Set organisations to calculate area for

# COMMAND ----------

alb_found_names_translation_dict.update({'Department for Environment, Food and Rural Affairs': None})
organisations_of_interest = alb_found_names_translation_dict.keys()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import polygon data

# COMMAND ----------

#import defra ccod-polygon data
polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculate area for all organisations of interest

# COMMAND ----------

area_df = pd.DataFrame(columns=['organisation', 'total_area', 'freehold_area', 'leasehold_area', 'overlap_freehold_leasehold'])
#For each organisation of interest
for organisation in organisations_of_interest:
    organisation_polygon_ccod = polygon_ccod_defra[polygon_ccod_defra['current_organisation'] == organisation]
    # get total area
    organisation_polygon_ccod.geometry = organisation_polygon_ccod.geometry.make_valid()
    total_area = organisation_polygon_ccod.dissolve().area.sum()/10000
    #dissolve by freehold/leasehold
    holding_dissolved_polygon_ccod = organisation_polygon_ccod.dissolve(by='Tenure', as_index=False)
    freehold = holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['Tenure'] == 'Freehold']
    freehold_area = freehold.area.sum()/10000
    leasehold = holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['Tenure'] == 'Leasehold']
    leasehold_area = leasehold.area.sum()/10000
    # sense check that freehold and leasehold add to total area
    if freehold_area + leasehold_area != total_area:
        print(f'Sense check: freehold area ({freehold_area}) and leasehold area ({leasehold_area}) do not add to total area ({total_area})')
    overlap_freehold_leasehold = gpd.overlay(freehold, leasehold, how='intersection', make_valid=True)
    overlap_freehold_leasehold_area = overlap_freehold_leasehold.area.sum()/10000
    # add values to dataframe
    df_row = pd.DataFrame(data={'organisation': organisation, 'total_area': total_area, 'freehold_area': freehold_area, 'leasehold_area': leasehold_area, 'overlap_freehold_leasehold': overlap_freehold_leasehold_area}, index=[0])
    area_df = pd.concat([area_df, df_row], ignore_index=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get all non-zero areas for summary table in report

# COMMAND ----------

non_zero_area_df = area_df[area_df['total_area']>0]

# COMMAND ----------

display(non_zero_area_df.sort_values(by='total_area', ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get list of all zero area organisations for inclusion in report

# COMMAND ----------

zero_area_df = area_df[area_df['total_area']==0].sort_values(by='organisation')
zero_area_organisations = zero_area_df['organisation'].unique()
print(zero_area_organisations)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Output to csv

# COMMAND ----------

area_df.to_csv(csv_area_df_polygon_ccod_defra_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Area plotting

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Seperate bar charts

# COMMAND ----------

# total area plot
area_df_non_zero = area_df[area_df['total_area']!=0]
area_df_non_zero = area_df_non_zero.sort_values(by='total_area')
fig, ax = plt.subplots()
total_area_bar = ax.barh(area_df_non_zero.organisation, area_df_non_zero.total_area, )
ax.set_xlabel('Area (hectares)')
ax.set_title('Total Freehold and Leasehold area for DEFRA and ALBs')
ax.set_xlim(left=0, right=255000)
plt.show()

# COMMAND ----------

#leasehold area plot
area_df_non_zero = area_df[area_df['leasehold_area']!=0]
area_df_non_zero = area_df_non_zero.sort_values(by='leasehold_area')
fig, ax = plt.subplots()
total_area_bar = ax.barh(area_df_non_zero.organisation, area_df_non_zero.leasehold_area, )
ax.set_xlabel('Area (hectares)')
ax.set_title('Total Leasehold area for DEFRA and ALBs')
ax.set_xlim(left=0, right=255000)
plt.show()

# COMMAND ----------

#Freehold area plot
area_df_non_zero = area_df[area_df['freehold_area']!=0]
area_df_non_zero = area_df_non_zero.sort_values(by='freehold_area')
fig, ax = plt.subplots()
total_area_bar = ax.barh(area_df_non_zero.organisation, area_df_non_zero.freehold_area, )
ax.set_xlabel('Area (hectares)')
ax.set_title('Total Freehold area for DEFRA and ALBs')
ax.set_xlim(left=0, right=255000)
plt.show()

# COMMAND ----------

area_df_big_landowners = area_df[area_df['total_area']>100]
area_df_small_landowners = area_df[area_df['total_area']<=100]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Clustered bar chart

# COMMAND ----------

plt.rcParams.update({'font.size': 14})

area_df_non_zero = area_df[area_df['freehold_area']!=0]
area_df_non_zero = area_df_non_zero.sort_values(by='total_area', ascending=True)
organisations = area_df_non_zero['organisation']
areas = {
    'Leasehold': area_df_non_zero['leasehold_area'],
    'Freehold': area_df_non_zero['freehold_area'],
    'Total': area_df_non_zero['total_area'],
}

x = np.arange(len(organisations))  # the label locations
width = 0.25  # the width of the bars
multiplier = 0


fig, ax = plt.subplots(layout='constrained', figsize=(15,10))

for category, measurement in areas.items():
    offset = width * multiplier
    rects = ax.barh(x + offset, measurement, width, label=category)
    #ax.bar_label(rects, padding=3)
    multiplier += 1

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Area (Ha)')
ax.set_title(' Total, freehold and leasehold  land area for DEFRA and ALBs', y=1.03)
ax.set_yticks(x + width, organisations, rotation=0)
handles, labels = ax.get_legend_handles_labels()
order = [2,1,0]
ax.legend([handles[idx] for idx in order],[labels[idx] for idx in order], loc='upper center', ncols=3)
#ax.legend(loc='upper center', ncols=3)
ax.set_ylim(0, 18)

plt.show()

# COMMAND ----------

holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['Tenure'] == 'Freehold']
