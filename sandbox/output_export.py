# Databricks notebook source
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

download_link(fc_polygons_not_overlapping_potential_fc_polygon_ccod_buffered_path, convert_filepath=True)

# COMMAND ----------

display(download_link(epims_defra_with_no_overlapping_polygon_ccod_defra_path, convert_filepath=True))
display(download_link(polygon_ccod_defra_with_no_overlapping_epims_defra_path, convert_filepath=True))

# COMMAND ----------

display(download_link(hmlr_epims_gaps_ccod_info_path, convert_filepath=True))
display(download_link(hmlr_fe_gaps_ccod_info_path, convert_filepath=True))

# COMMAND ----------

display(download_link(hmlr_fe_buffer_minus05_gaps_ccod_info_path, convert_filepath=True))

# COMMAND ----------

display(download_link('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/15_minute_greenspace/open_access_land/study_area_nps/salisbury/nps_ccod.geojson', convert_filepath=True))
