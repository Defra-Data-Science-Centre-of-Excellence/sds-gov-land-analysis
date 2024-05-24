# Databricks notebook source
import geopandas as gpd
import pandas as pd

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

organisations_of_interest = alb_found_names_translation_dict.keys()


# COMMAND ----------

import defra ccod-polygon data

# COMMAND ----------

#For each organisation of interest
#filter by organisation
#dissolve
#get area
#filter by leasehold
#get area
#filter by freehold
#get area
