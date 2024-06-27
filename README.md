# sds-gov-land-analysis
This repo was created to extract DEFRA (and DEFRA Arms-Length Body) land parcel data from the land registry National Polygon Service. it can be used alongside the document 'Identifying government land using land registry data: DEFRA case study'.
It includes the following scripts:
#### General
- paths: all data paths used for the analysis, including inputs and outputs
- output_export: required specifically for databricks, this script enables easy download of files from the dbfs
- constants: space to allow user to set global variables (primarily used to set organisation names of interest, which should be edited as required depending on the organisations land is being investigated for)
#### Data production
- identify_title_numbers: working with UK Company Proprietor dataset (ccod), produce a version with only titles of interest, and additional fields to represent current and historic organisation of interes names
- convert_nps_to_parquet: working with the national polygon dataset as shapefiles is slow, converting them to parquet once for use speeds up future processing
- identify_land_parcels: using output from 'identify_title_numbers', filter the national polygon dataset for land parcels of interest
- create_organisation_level_data:
#### Data summary
- area_calculations: calculate freehold, leasehold and total area of land for each organisation (output is a table/csv)
- plotter: produce spatial plot of outputs
#### Data validation
- data_comparison_epims: compare dataset produced using hmlr data to epims, organisation by organisation
- data_comparison_alb: compare dataset produced using hmlr data to land ownership datasets supplied by albs
- data_comparison_defra: compare identified proprietor names for defra to proprietor names previously produced for 30x30 work
- data_comparison_area: compare caluclated area figures from produced dataset to calculated epims and alb area figures
- data_comparison_gaps: for gaps identified by data comparisons, get the UK Company Proprietor data for the gaps
- data_comparison_postcode: validating alb vs defra records. Compare postcodes associated with alb land parcels with postcodes associated with defra records. This is to identify if/ help to disentangle defra and alb owned land parcels.
- create_study_boundary: create a small sample polygon area (national polgon service data can then be clipped to this for manual QA)
- data_validation_overlaps: assess overlaps which exist in the data (for freehold only as leasehold overlap is expected), both within defra land and between defra and non-defra land
