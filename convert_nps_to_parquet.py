# Databricks notebook source
# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

import geopandas as gpd

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import data

# COMMAND ----------

# import unfiltered national polygon dataset
national_polygon_dfs = []
for national_polygon_path in national_polygon_paths:
    national_polygon_df = gpd.read_file(national_polygon_path)#, where = f'TITLE_NO IN {title_numbers_of_interest_sql_string}')
    national_polygon_dfs.append(national_polygon_df)
    print(f'loaded into dataframe: {national_polygon_path}')
national_polygon = pd.concat(national_polygon_dfs, ignore_index=True)

# COMMAND ----------

# import unfiltered ccod data
# limited to these defined fields as they were the ones which seemed useful, but more fields are available and could be added if needed
ccod = pd.read_csv(
    ccod_path,    
    usecols=[
        "Title Number",
        "Tenure",
        "Proprietor Name (1)",
        "Company Registration No. (1)",
        "Proprietorship Category (1)",
        "Proprietor (1) Address (1)",
        "Date Proprietor Added",
        "Additional Proprietor Indicator",
        "Proprietor Name (2)",
        "Proprietor Name (3)",
        "Proprietor Name (4)"
        ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join ccod to national polygon dataset

# COMMAND ----------

# join polygon dataset with unfiltered ccod data - need this to look at adjascent polyogon info etc. The join is done on title number, which is present in both datasets for the purpose of a join key.
polygon_ccod = national_polygon.merge(ccod, how='inner', left_on='TITLE_NO', right_on='Title Number')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Export polygon-ccod as parquet

# COMMAND ----------

polygon_ccod.to_parquet(polygon_ccod_path)
