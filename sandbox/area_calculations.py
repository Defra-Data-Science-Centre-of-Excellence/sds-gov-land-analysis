# Databricks notebook source
import geopandas as gpd
import pandas as pd

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

polygon_ccod_defra.Tenure.unique()

# COMMAND ----------

create_area_df(polygon_data, organisations_of_interest):
area_df = pd.DataFrame(columns=['organisation', 'total_area', 'freehold_area', 'leasehold_area', 'sense_check', 'overlap_freehold_leasehold'])
#For each organisation of interest
for organisation in organisations_of_interest:
    organisation_polygon_ccod = polygon_ccod_defra[polygon_ccod_defra['current_organisation'] == organisation]
    # get total area
    organisation_polygon_ccod.geometry = organisation_polygon_ccod.geometry.make_valid()
    total_area = organisation_polygon_ccod.dissolve().area.sum()
    #dissolve by freehold/leasehold
    holding_dissolved_polygon_ccod = organisation_polygon_ccod.dissolve(by='Tenure', as_index=False)
    freehold = holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['Tenure'] == 'Freehold']
    freehold_area = freehold.area.sum()
    leasehold = holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['Tenure'] == 'Leasehold']
    leasehold_area = leasehold.area.sum()
    # sense check that freehold and leasehold add to total area
    if freehold_area + leasehold_area != total_area:
    print(f'Sense check: freehold area ({freehold_area}) and leasehold area ({leasehold_area}) do not add to total area ({total_area})')
    sense_check = f'Difference of: {total_area - freehold_area - leasehold_area}'
    else:
        sense_check = 0
    overlap_freehold_leasehold = gpd.overlay(freehold, leasehold, how='intersection', make_valid=True)
    overlap_freehold_leasehold_area = overlap_freehold_leasehold.area.sum()
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

holding_dissolved_polygon_ccod[holding_dissolved_polygon_ccod['Tenure'] == 'Freehold']
