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

data = gpd.read_file(polygon_ccod_defra_path)

# COMMAND ----------

estate_names = data[data['Proprietor Name (1)'].str.contains('ESTATE')]
estate_names = estate_names['Proprietor Name (1)'].unique()
display(estate_names)
defra_names_df = data[~data['Proprietor Name (1)'].isin(estate_names)]

# COMMAND ----------

defra_data = defra_names_df

# COMMAND ----------

data_selected = defra_data[['POLY_ID','Title Number','Tenure', 'current_organisation', 'historic_organisation', 'Proprietor Name (1)', 'Proprietor Name (2)', 'Proprietor Name (3)', 'Proprietor Name (4)', 'geometry']]

# COMMAND ----------

data_selected

# COMMAND ----------

data_selected.to_file(f'/tmp/hmlr_defra_estate.gpkg', driver='GPKG', layer='hmlr_defra_estate')

# COMMAND ----------

# MAGIC %sh
# MAGIC mv /tmp/hmlr_defra_estate.gpkg /dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/nps_outputs/hmlr_defra_estate.gpkg

# COMMAND ----------

display(download_link('/dbfs/mnt/lab/restricted/ESD-Project/jasmine.elliott@defra.gov.uk/gov_land_analysis/nps_outputs/hmlr_defra_estate.gpkg', convert_filepath=True))
