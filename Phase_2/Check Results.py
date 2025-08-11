# Databricks notebook source
# MAGIC %pip install keplergl pydeck folium matplotlib mapclassify rtree pygeos geopandas==1.0.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import expr
from geopandas import GeoDataFrame, GeoSeries
import geopandas as gpd
from sedona.spark import *
from pyspark.sql.functions import monotonically_increasing_id
import re
from glob import glob
import os
import pandas as pd
import rtree
import pygeos

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

# COMMAND ----------

defra_land_input = gpd.read_parquet("/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/phase_one_final_report_outputs/polygon_ccod_defra_by_organisation_tenure.parquet")

# COMMAND ----------

outdir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Intersection/'

# COMMAND ----------

dgl_phi = gpd.read_parquet('/dbfs'+outdir+'dgl_phi_gpd.parquet')
dgl_le = gpd.read_parquet('/dbfs'+outdir+'dgl_le_gpd.parquet')

# COMMAND ----------

(dgl_phi.geometry.area / 10000).sum()

## Latest version of PHI in desktop produces ~4000 ha more - version mismatch with data in DASH?

# COMMAND ----------

(dgl_le.geometry.area / 10000).sum()

## 297.3 ha outside LE extent
## Total 329,200.8

# COMMAND ----------

(defra_land_input.geometry.area / 10000).sum()

# COMMAND ----------

dgl_le

# COMMAND ----------


