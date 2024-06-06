# Databricks notebook source
import geopandas as gpd
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

# MAGIC %md
# MAGIC Set organisations to calculate area for

# COMMAND ----------

alb_found_names_translation_dict.update({'Department for Environment, Food and Rural Affairs': None})
organisations_of_interest = alb_found_names_translation_dict.keys()


# COMMAND ----------

#import defra ccod-polygon data
polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)

# COMMAND ----------

defra_freehold = polygon_ccod_defra[polygon_ccod_defra['Tenure']=='Freehold']
defra_freehold_area = defra_freehold.dissolve().area.sum()/10000
defra_freehold_area

# COMMAND ----------

fc_freehold = defra_freehold[defra_freehold['current_organisation']=='Forestry Commission']
fc_freehold_area = fc_freehold.dissolve().area.sum()/10000
fc_freehold_area

# COMMAND ----------

polygon_ccod_defra.Tenure.unique()

# COMMAND ----------

area_df = pd.DataFrame(columns=['organisation', 'total_area', 'freehold_area', 'leasehold_area', 'sense_check', 'overlap_freehold_leasehold'])
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
        sense_check = f'Difference of: {total_area - freehold_area - leasehold_area}'
    else:
        sense_check = 0
    overlap_freehold_leasehold = gpd.overlay(freehold, leasehold, how='intersection', make_valid=True)
    overlap_freehold_leasehold_area = overlap_freehold_leasehold.area.sum()/10000
    # add values to dataframe
    df_row = pd.DataFrame(data={'organisation': organisation, 'total_area': total_area, 'freehold_area': freehold_area, 'leasehold_area': leasehold_area, 'sense_check': sense_check, 'overlap_freehold_leasehold': overlap_freehold_leasehold_area}, index=[0])
    area_df = pd.concat([area_df, df_row], ignore_index=True)


# COMMAND ----------

overlap_freehold_leasehold.area

# COMMAND ----------

area_df

# COMMAND ----------

display(area_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Output to csv

# COMMAND ----------

area_df.to_csv(csv_area_df_polygon_ccod_defra_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Area plotting

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

holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['Tenure'] == 'Freehold']
