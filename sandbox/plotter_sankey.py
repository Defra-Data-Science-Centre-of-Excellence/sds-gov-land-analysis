# Databricks notebook source
# MAGIC %sh
# MAGIC pip install pysankey

# COMMAND ----------

import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go


# COMMAND ----------

# MAGIC %run
# MAGIC ./paths

# COMMAND ----------

data = gpd.read_file(hmlr_epims_buffer_minus05_gaps_ccod_info_path)

# COMMAND ----------

data_grouped = data.dissolve(by=['PropertyCentre', 'Proprietor Name (1)'], as_index=False)

# COMMAND ----------

data_grouped

# COMMAND ----------

data_grouped['area'] = data_grouped.area

# COMMAND ----------


links = data_grouped[['PropertyCentre', 'Proprietor Name (1)', 'area']]

# COMMAND ----------

links

# COMMAND ----------

unique_source_target = list(pd.unique(links[['PropertyCentre', 'Proprietor Name (1)']].values.ravel('K')))

# COMMAND ----------

unique_source_target

# COMMAND ----------


mapping_dict = {k: v for v, k in enumerate(unique_source_target)}

# COMMAND ----------

mapping_dict

# COMMAND ----------

links['PropertyCentre'] = links['PropertyCentre'].map(mapping_dict)
links['Proprietor Name (1))'] = links['Proprietor Name (1)'].map(mapping_dict)

# COMMAND ----------

links

# COMMAND ----------

links_dict = links.to_dict(orient='list')

# COMMAND ----------

data_grouped = data_grouped[0:10]

# COMMAND ----------

data_grouped

# COMMAND ----------

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = ,
      color = "blue"
    ),
    link = dict(
      source = data_grouped["PropertyCentre"],
      target = data_grouped["Proprietor Name (1)"],
      #value = data_grouped["area"]
  ))])
fig.show()

# COMMAND ----------

fig.show()

# COMMAND ----------

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/titanic.csv')

# COMMAND ----------

df1 = df.groupby(['Pclass', 'Sex'])['Name'].count().reset_index()
df1.columns = ['source', 'target', 'value']
df1['source'] = df1.source.map({1: 'Pclass1', 2: 'Pclass2', 3: 'Pclass3'})
df2 = df.groupby(['Sex', 'Survived'])['Name'].count().reset_index()
df2.columns = ['source', 'target', 'value']
df2['target'] = df2.target.map({1: 'Survived', 0: 'Died'})
links = pd.concat([df1, df2], axis=0)
unique_source_target = list(pd.unique(links[['source', 'target']].values.ravel('K')))
mapping_dict = {k: v for v, k in enumerate(unique_source_target)}
links['source'] = links['source'].map(mapping_dict)
links['target'] = links['target'].map(mapping_dict)
links_dict = links.to_dict(orient='list')

# COMMAND ----------

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = unique_source_target,
      color = "blue"
    ),
    link = dict(
      source = links_dict["source"],
      target = links_dict["target"],
      value = links_dict["value"]
  ))])
     

# COMMAND ----------

fig.show()
