# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/mnt/lab/restricted/ESD-Project/sds-assets/10m/PHI/")

# COMMAND ----------

source_dir = "dbfs:/mnt/lab/restricted/ESD-Project/sds-assets/10m/"
destination_dir = "dbfs:/mnt/lab/restricted/ESD-Project/sds-assets/10m/LE/"


# COMMAND ----------

files = dbutils.fs.ls(source_dir)
files

# COMMAND ----------

for file_info in files:
  print(file_info.path)
    #if file_info.path.endswith(".parquet"):  
    #    source_path = file_info.path
    #    dest_path = destination_dir + source_path.split("/")[-1]  
    #    dbutils.fs.mv(source_path, dest_path)
    #    print(f"Moved: {source_path} -> {dest_path}")

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/lab/restricted/ESD-Project/sds-assets/10m/","dbfs:/mnt/lab/restricted/ESD-Project/sds-assets/10m/LE/", True)

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/lab/restricted/ESD-Project/sds-assets/10m',True)

# COMMAND ----------


