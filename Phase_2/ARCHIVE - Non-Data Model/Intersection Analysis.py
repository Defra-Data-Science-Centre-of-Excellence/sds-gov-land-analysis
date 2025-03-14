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

preppeddir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Prepped_Inputs/'
outdir = '/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Intersection/'

# COMMAND ----------

le = spark.read.parquet(preppeddir + "le_prepped.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))
dgl = spark.read.parquet(preppeddir + "dgl_prepped.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))
phi = spark.read.parquet(preppeddir + "phi_prepped.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))
lcm = spark.read.parquet(preppeddir + "lcm_prepped.parquet").withColumn("geometry", expr("st_makevalid(st_setsrid(geometry,27700))"))

# COMMAND ----------

le.createOrReplaceTempView("le_temp")
dgl.createOrReplaceTempView("dgl_temp")
phi.createOrReplaceTempView("phi_temp")
lcm.createOrReplaceTempView("lcm_temp")

# COMMAND ----------

overlap_dgl_phi = spark.sql("""
            SELECT d1.Main_Habit,
                d2.current_organisation,
                d2.land_management_organisation,
                d2.Tenure,
                ST_Intersection(d1.geometry, d2.geometry) AS geometry,
                ST_Area(ST_Intersection(d1.geometry, d2.geometry)) / 10000 AS ovrlp_area_ha
            FROM phi_temp AS d1
            JOIN dgl_temp AS d2 ON ST_Intersects(d1.geometry, d2.geometry)
        """)

# COMMAND ----------

overlap_dgl_phi = overlap_dgl_phi.repartition(350)
overlap_dgl_phi.write.format("parquet").mode('overwrite').save(outdir + 'dgl_phi_sedona.parquet')

# COMMAND ----------

overlap_dgl_le = spark.sql("""
            SELECT d1.Prmry_H,
                d1.Relblty,
                d1.Mixd_Sg,
                d2.current_organisation,
                d2.land_management_organisation,
                d2.Tenure,
                ST_Intersection(d1.geometry, d2.geometry) AS geometry,
                ST_Area(ST_Intersection(d1.geometry, d2.geometry)) / 10000 AS ovrlp_area_ha
            FROM le_temp AS d1
            JOIN dgl_temp AS d2 ON ST_Intersects(d1.geometry, d2.geometry)
        """)

# COMMAND ----------

overlap_dgl_le = overlap_dgl_le.repartition(350)
overlap_dgl_le.write.format("parquet").mode('overwrite').save(outdir + 'dgl_le_sedona.parquet')

# COMMAND ----------

overlap_dgl_lcm = spark.sql("""
            SELECT d1.f_mode,
                d1.f_purity,
                d1.f_conf,
                d2.current_organisation,
                d2.land_management_organisation,
                d2.Tenure,
                ST_Intersection(d1.geometry, d2.geometry) AS geometry,
                ST_Area(ST_Intersection(d1.geometry, d2.geometry)) / 10000 AS ovrlp_area_ha
            FROM lcm_temp AS d1
            JOIN dgl_temp AS d2 ON ST_Intersects(d1.geometry, d2.geometry)
        """)

# COMMAND ----------

overlap_dgl_lcm = overlap_dgl_lcm.repartition(350)
overlap_dgl_lcm.write.format("parquet").mode('overwrite').save(outdir + 'dgl_lcm_sedona.parquet')

# COMMAND ----------

dgl_le = spark.read.parquet( outdir + 'dgl_le_sedona.parquet')
df_without_geometry = dgl_le.drop("geometry")
pandas_df = df_without_geometry.toPandas()
pandas_df.to_csv('/dbfs/'+outdir+'dgl_le_noGeom.csv', index=False)

# COMMAND ----------

dgl_phi = spark.read.parquet( outdir + 'dgl_phi_sedona.parquet')
df_without_geometry = dgl_phi.drop("geometry")
pandas_df = df_without_geometry.toPandas()
pandas_df.to_csv('/dbfs/'+outdir+'dgl_phi_noGeom.csv', index=False)

# COMMAND ----------

dgl_le = spark.read.parquet( outdir + 'dgl_le_sedona.parquet')
pandas_df = dgl_le.toPandas()
gdf = gpd.GeoDataFrame(pandas_df, geometry='geometry')
gdf.set_crs(epsg=27700, inplace=True)
gdf.to_parquet('/dbfs'+outdir+'dgl_le_gpd.parquet')

# COMMAND ----------

dgl_phi = spark.read.parquet( outdir + 'dgl_phi_sedona.parquet')
pandas_df = dgl_phi.toPandas()
gdf = gpd.GeoDataFrame(pandas_df, geometry='geometry')
gdf.set_crs(epsg=27700, inplace=True)
gdf.to_parquet('/dbfs'+outdir+'dgl_phi_gpd.parquet')

# COMMAND ----------

dgl_lcm = spark.read.parquet( outdir + 'dgl_lcm_sedona.parquet')
df_without_geometry = dgl_lcm.drop("geometry")
pandas_df = df_without_geometry.toPandas()
pandas_df.to_csv('/dbfs/'+outdir+'dgl_lcm_noGeom.csv', index=False)

# COMMAND ----------

dgl_lcm = spark.read.parquet( outdir + 'dgl_lcm_sedona.parquet')
pandas_df = dgl_lcm.toPandas()
gdf = gpd.GeoDataFrame(pandas_df, geometry='geometry')
gdf.set_crs(epsg=27700, inplace=True)
gdf.to_parquet('/dbfs'+outdir+'dgl_lcm_gpd.parquet')

# COMMAND ----------


