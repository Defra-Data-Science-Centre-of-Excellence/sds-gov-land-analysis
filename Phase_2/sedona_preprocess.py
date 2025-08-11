# Databricks notebook source
# MAGIC %md
# MAGIC #Sedona Pre-process
# MAGIC
# MAGIC
# MAGIC ###Function for converting spatial data formats (readable by geopandas) into sedona optimised parquet
# MAGIC ###This includes EXPLODE(ST_DUMP), ST_SubDivideExplode and repartitioning of data
# MAGIC
# MAGIC Work in progress
# MAGIC
# MAGIC Author: Miles Clement, Defra
# MAGIC
# MAGIC Created 17/10/24, Edited 17/10/24

# COMMAND ----------

# MAGIC %pip install keplergl pydeck mapclassify rtree pygeos geopandas==1.0.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
import os
from glob import glob
import re
import pygeos
import rtree

from sedona.spark import *
from pyspark.sql.functions import expr
from pyspark.sql.functions import monotonically_increasing_id

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

# COMMAND ----------

def sedona_preprocess(input_data,
                      temp_dir,
                      output_dir,
                      outname,
                      set_partition = 'default',
                      num_partitions = 350):
    
    ### input_data - geopandas dataframe
    ### set_partition - define which column to base partitioning on if available, otherwise default sedona repartitioning used (default='default')
    ### num_partitions - if default sedona partitioning used, then define number of partitions. Ignored is other partitioning used
    ### temp_dir - directory to save temporary dataset in. These are deleted at the end of the script
    ### output_dir - directory to save outputs 
    ### outname - naming to call output parquets

    # Save data in tempdir as parquet
    input_data.to_parquet('/dbfs'+temp_dir+'gpd_temp3.parquet')

    if set_partition == 'default':
        partition = num_partitions
    else:
        partition = set_partition

    # Explode geometries and subdivide polygons, and repartition
    sed_temp = spark.read.parquet(temp_dir + 'gpd_temp3.parquet').withColumn("geometry", expr("st_makevalid(st_setsrid(ST_GeomFromWKB(geometry),27700))"))
    sed_temp_dump = sed_temp.withColumn("geometry", expr("EXPLODE(ST_DUMP(geometry))")).repartition(50).cache()
    sed_temp_subdivide = sed_temp_dump.withColumn("geometry", expr("ST_SubDivideExplode(geometry, 256)")).repartition(50)
    
    sed_temp_subdivide.write.format("geoparquet").mode('overwrite').save(output_dir+outname+'.parquet')

    return



# COMMAND ----------

# MAGIC %md
# MAGIC Testing

# COMMAND ----------

filepath = "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"

# COMMAND ----------

phi_north = gpd.read_file(filepath+"dataset_priority_habitat_inventory_north/format_SHP_priority_habitat_inventory_north/LATEST_priority_habitat_inventory_north/PHI_v2_3_North.shp")
#phi_central = gpd.read_file(filepath+"dataset_priority_habitat_inventory_central/format_SHP_priority_habitat_inventory_central/LATEST_priority_habitat_inventory_central/PHI_v2_3_Central.shp")
#phi_south = gpd.read_file(filepath+"dataset_priority_habitat_inventory_south/format_SHP_priority_habitat_inventory_south/LATEST_priority_habitat_inventory_south/PHI_v2_3_South.shp")

#phi_comb = pd.concat([phi_north, phi_central, phi_south], ignore_index=True)

# COMMAND ----------

columns = ["Main_Habit", "geometry"]
## NOTE - sedona can't write columns of type TimestampNTZType
## This means within PHI, the LastModDat column has to be removed as a minimum
phi_comb = phi_north[columns]

# COMMAND ----------

temp_dir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Sedona_Test_Prepped_Inputs/'
output_dir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Sedona_Test_Prepped_Inputs/'

# COMMAND ----------

sedona_preprocess(phi_comb, temp_dir, output_dir, 'sedona_prep_test')

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Sedona_Test_Prepped_Inputs/ | wc -l

# COMMAND ----------

water_link_10k = sedona.sql(
"
SELECT water_link.osid, water_link.description, water_link.geometry_source, water_link.width, water_link.permanence, water_link.class, water_link.bluespace_length_m, water_link.geometry, os_tenk_grid.tile_name FROM water_link, os_tenk_grid WHERE ST_Intersects(os_tenk_grid.geometry, water_link.geometry)
"
)
