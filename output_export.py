# Databricks notebook source
import geopandas as gpd

# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

def download_link(filepath, convert_filepath=False):
    '''
    Copy file on dbfs mount to Filestore and return download link for file.

    Parameters:
        filepath (str): filepath of file for download. Can be in dbfs: or dbfs/ format
        convert_filepath (bool): If True filepath will be converted to dbfs: format by function. If false the filepath passed should already be in dbfs: format. Default = False
    '''
    # NB filepath must be in the format dbfs:/ not /dbfs/
    if convert_filepath == True:
        filepath = 'dbfs:/' + filepath.lstrip('/dbfs')
    # Get filename
    filename = filepath[filepath.rfind("/") :]
    # Move file to FileStore
    dbutils.fs.cp(filepath, f"dbfs:/FileStore/{filename}")
    # Construct download url
    url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    # Return html snippet
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"

# COMMAND ----------

display(download_link('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/validation/overlap_with_non_defra_estate_buffer_05.parquet', convert_filepath=True))

# COMMAND ----------

# read in datasets
polygon_ccod_defra = gpd.read_file(polygon_ccod_defra_path)
polygon_ccod_defra.geometry = polygon_ccod_defra.geometry.make_valid()
polygon_ccod_defra_by_organisation = gpd.read_parquet(polygon_ccod_defra_by_organisation_path)
polygon_ccod_defra_by_organisation.geometry = polygon_ccod_defra_by_organisation.geometry.make_valid()
polygon_ccod_defra_by_organisation_tenure = gpd.read_parquet(polygon_ccod_defra_by_organisation_tenure_path)
polygon_ccod_defra_by_organisation_tenure.geometry = polygon_ccod_defra_by_organisation_tenure.geometry.make_valid()

# COMMAND ----------

polygon_ccod_defra_by_organisation_tenure

# COMMAND ----------

polygon_ccod_defra_by_organisation

# COMMAND ----------

polygon_ccod_defra = polygon_ccod_defra[['POLY_ID','Title Number','Tenure', 'current_organisation', 'historic_organisation', 'Proprietor Name (1)', 'Proprietor Name (2)', 'Proprietor Name (3)', 'Proprietor Name (4)', 'geometry']]

# COMMAND ----------

# output to geopackage in temp
polygon_ccod_defra.to_file(f'/tmp/hmlr_defra_estate.gpkg', driver='GPKG', layer='defra_estate_by_land_parcel')
polygon_ccod_defra_by_organisation.to_file(f'/tmp/hmlr_defra_estate.gpkg', driver='GPKG', layer='defra_estate_by_organisation')
polygon_ccod_defra_by_organisation_tenure.to_file(f'/tmp/hmlr_defra_estate.gpkg', driver='GPKG', layer='defra_estate_by_organisation_and_tenure')


# COMMAND ----------

# MAGIC %sh
# MAGIC mv /tmp/hmlr_defra_estate.gpkg /dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/nps_outputs/hmlr_defra_estate.gpkg

# COMMAND ----------

display(download_link('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/nps_outputs/hmlr_defra_estate.gpkg', convert_filepath=True))
