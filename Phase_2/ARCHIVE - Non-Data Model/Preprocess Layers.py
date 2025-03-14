# Databricks notebook source
dbutils.fs.mkdirs('dbfs:/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/LCM/')

# COMMAND ----------

dbutils.fs.mv('dbfs:/FileStore/miles_clement/LCM_Poly_2021/','dbfs:/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/LCM/',True)

# COMMAND ----------

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

tempdir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Temp/'
preppeddir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Prepped_Inputs/'

# COMMAND ----------

# MAGIC %md
# MAGIC PHI

# COMMAND ----------

# Set input filepath constant
filepath = "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"

# COMMAND ----------

phi_north = gpd.read_file(filepath+"dataset_priority_habitat_inventory_north/format_SHP_priority_habitat_inventory_north/LATEST_priority_habitat_inventory_north/PHI_v2_3_North.shp")
phi_central = gpd.read_file(filepath+"dataset_priority_habitat_inventory_central/format_SHP_priority_habitat_inventory_central/LATEST_priority_habitat_inventory_central/PHI_v2_3_Central.shp")
phi_south = gpd.read_file(filepath+"dataset_priority_habitat_inventory_south/format_SHP_priority_habitat_inventory_south/LATEST_priority_habitat_inventory_south/PHI_v2_3_South.shp")

phi_comb = pd.concat([phi_north, phi_central, phi_south], ignore_index=True)

# COMMAND ----------

columns = ["Main_Habit", "geometry"]

phi_comb = phi_comb[columns]

# COMMAND ----------

phi_comb.to_parquet('/dbfs'+tempdir+'gpd_temp.parquet')
phi_sed = spark.read.parquet(tempdir + 'gpd_temp.parquet').withColumn("geometry", expr("st_makevalid(st_setsrid(ST_GeomFromWKB(geometry),27700))"))

# COMMAND ----------

phi_sed = phi_sed.repartition(350)
phi_sed = phi_sed.withColumn("geometry", expr("EXPLODE(ST_DUMP(geometry))")).repartition(350)
phi_sed.write.format("geoparquet").mode('overwrite').save(tempdir+'sed_temp.parquet')

# COMMAND ----------

phi_sed = spark.read.parquet(tempdir + "sed_temp.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))
phi_sed = phi_sed.repartition(350)
phi_sed = phi_sed.withColumn("geometry", expr("ST_SubDivideExplode(geometry, 256)")).repartition(350)
phi_sed.write.format("geoparquet").mode('overwrite').save(preppeddir+'phi_prepped.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC Defra Group Land

# COMMAND ----------

defra_land = gpd.read_parquet("/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/phase_one_final_report_outputs/polygon_ccod_defra_by_organisation_tenure.parquet")

# COMMAND ----------

defra_land

# COMMAND ----------

defra_land.to_parquet('/dbfs'+tempdir+'gpd_temp.parquet')
dgl_sed = spark.read.parquet(tempdir + 'gpd_temp.parquet').withColumn("geometry", expr("st_makevalid(st_setsrid(ST_GeomFromWKB(geometry),27700))"))

# COMMAND ----------

dgl_sed = dgl_sed.repartition(350)
dgl_sed = dgl_sed.withColumn("geometry", expr("EXPLODE(ST_DUMP(geometry))")).repartition(350)
dgl_sed.write.format("geoparquet").mode('overwrite').save(tempdir+'sed_temp.parquet')

# COMMAND ----------

dgl_sed = spark.read.parquet(tempdir + "sed_temp.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))
dgl_sed = dgl_sed.repartition(350)
dgl_sed = dgl_sed.withColumn("geometry", expr("ST_SubDivideExplode(geometry, 256)")).repartition(350)
dgl_sed.write.format("geoparquet").mode('overwrite').save(preppeddir+'dgl_prepped.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC LE

# COMMAND ----------

le = gpd.read_parquet("/dbfs/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/LE/LE2223.parquet")

# COMMAND ----------

columns = ["Prmry_H", "Relblty", "Mixd_Sg","geometry"]

le = le[columns]

# COMMAND ----------

le.to_parquet('/dbfs'+tempdir+'gpd_temp.parquet')
le_sed = spark.read.parquet(tempdir + 'gpd_temp.parquet').withColumn("geometry", expr("st_makevalid(st_setsrid(ST_GeomFromWKB(geometry),27700))"))

# COMMAND ----------

le_sed = le_sed.repartition(350)
le_sed = le_sed.withColumn("geometry", expr("EXPLODE(ST_DUMP(geometry))")).repartition(350)
le_sed.write.format("geoparquet").mode('overwrite').save(tempdir+'sed_temp.parquet')

# COMMAND ----------

le_sed = spark.read.parquet(tempdir + "sed_temp.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))
le_sed = le_sed.repartition(350)
le_sed = le_sed.withColumn("geometry", expr("ST_SubDivideExplode(geometry, 256)")).repartition(350)
le_sed.write.format("geoparquet").mode('overwrite').save(preppeddir+'le_prepped.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC LCM

# COMMAND ----------

lcm = gpd.read_file("/dbfs/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/LCM/LCM_Eng.shp")

# COMMAND ----------

lcm.display()

# COMMAND ----------

columns = ["f_mode", "f_purity", "f_conf", "geometry"]

lcm = lcm[columns]

# COMMAND ----------

lcm.to_parquet('/dbfs'+tempdir+'gpd_temp.parquet')
lcm_sed = spark.read.parquet(tempdir + 'gpd_temp.parquet').withColumn("geometry", expr("st_makevalid(st_setsrid(ST_GeomFromWKB(geometry),27700))"))

# COMMAND ----------

lcm_sed = lcm_sed.repartition(350)
lcm_sed = lcm_sed.withColumn("geometry", expr("EXPLODE(ST_DUMP(geometry))")).repartition(350)
lcm_sed.write.format("geoparquet").mode('overwrite').save(tempdir+'sed_temp.parquet')

# COMMAND ----------

lcm_sed = spark.read.parquet(tempdir + "sed_temp.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))
lcm_sed = lcm_sed.repartition(350)
lcm_sed = lcm_sed.withColumn("geometry", expr("ST_SubDivideExplode(geometry, 256)")).repartition(350)
lcm_sed.write.format("geoparquet").mode('overwrite').save(preppeddir+'lcm_prepped.parquet')
