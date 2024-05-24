# sds-gov-land-analysis
This repo was created to extract DEFRA (and DEFRA Arms-Length Body) land parcel data from the land registry National Polygon Service. it can be used alongside the document 'Identifying government land using land registry data: DEFRA case study'.
It includes the following scripts:
#### General
- paths: all data paths used for the analysis, including inputs and outputs
- output_exports: required specifically for databricks, this script enables easy download of files from the dbfs
- controls: space to allow user to set global variables (primarily used to set organisation names of interest)
#### Data production
- identify_title_numbers: working with UK Company Proprietor dataset (ccod), produce a version with only titles of interest, and additional fields to represent current and historic organisation of interes names
- convert_nps_to_parquet: working with the national polygon dataset as shapefiles is slow, converting them to parquet once for use speeds up future use
- identify_land_parcels: using output from 'identify_title_numbers', filter the national polygon dataset for land parcels of interest
- area_calculations:
#### Data validation
- data_comparison_epims:
- data_comparison_alb:
- data_comparison_gaps: for gaps identified by data comparisons, get the UK Company Proprietor data for the gaps
- data_comparison_postcode:
- 