# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Intersect Natural England Priority Habitat Inventory with Data Model Grid (10m)
# MAGIC - This code iterates through all habitats and creates the relevant assets
# MAGIC
# MAGIC Miles Clement (miles.clement@defra.gov.uk)
# MAGIC
# MAGIC Last Updated 17/03/25

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC ####Packages

# COMMAND ----------

from pathlib import Path
import geopandas as gpd
from geopandas import read_file
import pandas as pd
import os

# COMMAND ----------

from sedona.spark import *
from sedona.sql import st_constructors as cn
from pyspark.sql.functions import lit, expr, col, like, sum
from sedona.sql import st_functions as fn

sedona = SedonaContext.create(spark)
sqlContext.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC ####User-defined Variables

# COMMAND ----------

# DBTITLE 1,USER INPUT
# Define size of grid square
grid_square_size = 10

# COMMAND ----------

# DBTITLE 1,USER INPUT
# location for input grid/centroids
in_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Model_Grids"
)

alt_in_path = str(in_path).replace("/dbfs", "dbfs:")

# location for outputs
out_path = Path(
    "/dbfs/mnt/lab-res-a1001005/esd_project/sds-assets/10m/PHI"
)

alt_out_path = str(out_path).replace("/dbfs", "dbfs:")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ####Load in core datasets

# COMMAND ----------

# read in grid points
eng_combo_centroids = (
    sedona.read.format("parquet")
    .load(f"{alt_in_path}/{grid_square_size}m_england_grid_centroids.parquet")
    .repartition(500)
)
eng_combo_centroids.createOrReplaceTempView("eng_combo_centroids")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load in asset dataset
# MAGIC - PHI is stored in three subsets - load in and combine

# COMMAND ----------

filepath = "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"

phi_north = gpd.read_file(filepath+"dataset_priority_habitat_inventory_north/format_SHP_priority_habitat_inventory_north/LATEST_priority_habitat_inventory_north/PHI_v2_3_North.shp")
phi_central = gpd.read_file(filepath+"dataset_priority_habitat_inventory_central/format_SHP_priority_habitat_inventory_central/LATEST_priority_habitat_inventory_central/PHI_v2_3_Central.shp")
phi_south = gpd.read_file(filepath+"dataset_priority_habitat_inventory_south/format_SHP_priority_habitat_inventory_south/LATEST_priority_habitat_inventory_south/PHI_v2_3_South.shp")

phi_comb = pd.concat([phi_north, phi_central, phi_south], ignore_index=True)

# COMMAND ----------

# Save to parquet
phi_comb.to_parquet('/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Layers/phi_comb.parquet')

# COMMAND ----------

# Reload data
focal_path = "/dbfs/mnt/lab-res-a1001005/esd_project/Defra_Land/Layers/phi_comb.parquet"
focal_layer = sedona.read.format("geoparquet").load(focal_path)

# Define naming and filter vaiables
dataset_prefix = 'phi'
focal_column = 'Main_Habit'


# COMMAND ----------

# DBTITLE 1,USER INPUT
# Create dictionary of habitat names and desired output names
asset_dict = {'Blanket bog':'blanket_bog',
 'Calaminarian grassland':'grassland_calaminarian',
 'Coastal and floodplain grazing marsh':'coast_floodplain_grazing_marsh',
 'Coastal saltmarsh':'saltmarsh',
 'Coastal sand dunes':'sand_dunes',
 'Coastal vegetated shingle':'vegetated_shingle',
 'Deciduous woodland':'deciduous_woodland',
 'Fragmented heath':'fragmented_heath',
 'Good quality semi-improved grassland':'grassland_semi_improved_good',
 'Grass moorland':'grass_moorland',
 'Limestone pavement':'limestone_pavement',
 'Lowland calcareous grassland':'grassland_calcareous_lowland',
 'Lowland dry acid grassland':'grassland_dry_acid_lowland',
 'Lowland fens':'lowland_fens',
 'Lowland heathland':'lowland_heathland',
 'Lowland meadows':'lowland_meadows',
 'Lowland raised bog':'lowland_raised_bog',
 'Maritime cliff and slope':'maritime_cliff',
 'Mountain heaths and willow scrub':'mountain_heath_scrub',
 'Mudflats':'mudflats',
 'No main habitat but additional habitats present':'no_main_hab',
 'Purple moor grass and rush pastures':'purple_moor_rush_pastures',
 'Reedbeds':'reedbeds',
 'Saline lagoons':'saline_lagoons',
 'Traditional orchard':'traditional_orchard',
 'Upland calcareous grassland':'grassland_calcareous_upland',
 'Upland flushes, fens and swamps':'upland_fens_swamps',
 'Upland hay meadow':'upland_hay_meadow',
 'Upland heathland':'upland_heathland'}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Processing

# COMMAND ----------

# Iterate through dictionary
for asset, outname in asset_dict.items(): 

  print(f"Processing: {asset}")

  # Set outname
  focal_name = f"{dataset_prefix}_{outname}"

  # Filter dataset for habitat
  asset_layer = focal_layer.filter(focal_layer[focal_column] == asset) 
  asset_layer.createOrReplaceTempView("asset_layer")

  # Explode polygons
  asset_layer_exploded = spark.sql(
      "SELECT ST_SubDivideExplode(asset_layer.geometry, 12) AS geometry FROM asset_layer"
  ).repartition(500)

  asset_layer_exploded.createOrReplaceTempView("asset_layer_exploded")

  # Find cells that intersect asset layer and assign them a 1
  out = spark.sql(
      "SELECT eng_combo_centroids.id FROM eng_combo_centroids, asset_layer_exploded WHERE ST_INTERSECTS(eng_combo_centroids.geometry, asset_layer_exploded.geometry)"
  ).withColumn(focal_name, lit(1))

  out.write.format("parquet").mode("overwrite").save(
      f"{alt_out_path}/10m_x_{focal_name}.parquet"
  )

  # Drop duplicates
  out2 = spark.read.format("parquet").load(f"{alt_out_path}/10m_x_{focal_name}.parquet").groupBy("id").count().drop("count").withColumn(focal_name, lit(1))

  out2.write.format("parquet").mode("overwrite").save(
      f"{alt_out_path}/10m_x_{focal_name}.parquet"
  )


# COMMAND ----------


