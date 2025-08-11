# Databricks notebook source
# MAGIC %pip install keplergl pydeck folium matplotlib mapclassify rtree pygeos geopandas==1.0.0 rasterio
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import geopandas as gpd

# COMMAND ----------

def download_link(filepath):
    # NB filepath must be in the format dbfs:/ not /dbfs/
    # Get filename
    filename = filepath[filepath.rfind("/") :]
    # Move file to FileStore
    dbutils.fs.cp(filepath, f"dbfs:/FileStore/{filename}")
    # Construct download url
    url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    # Return html snippet
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"

# COMMAND ----------

displayHTML(download_link('dbfs:/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Intersection/dgl_lcm_gpd.parquet'))

# COMMAND ----------

displayHTML(download_link('dbfs:/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Intersection/dgl_le_gpd.parquet'))

# COMMAND ----------

displayHTML(download_link('dbfs:/mnt/lab/restricted/ESD-Project/miles.clement@defra.gov.uk/Defra_Land/Intersection/dgl_phi_gpd.parquet'))
