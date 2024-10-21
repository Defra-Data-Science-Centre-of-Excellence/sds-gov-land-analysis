Project Owner: Jazz Elliott

Project Contact: jasmine.elliott@defra.gov.uk

Project Start Date: 03/2024

Project Completion Date: 08/2024

Project Sharepoint Link: [Land Ownership (Spatial Data Science)](https://defra.sharepoint.com/:f:/r/teams/Team1608/ESA%20Team%20Resources/Spatial%20Data%20Science/Workstreams/land_ownership/Outputs?csf=1&web=1&e=vbPSCa)


# sds-gov-land-analysis
This repo was created to extract DEFRA (and DEFRA Arms-Length Body) land parcel data from the land registry National Polygon Service. It can be used alongside the report 'Identifying government land using land registry data: DEFRA case study' (found in the [project sharepoint folder](https://defra.sharepoint.com/:f:/r/teams/Team1608/ESA%20Team%20Resources/Spatial%20Data%20Science/Workstreams/land_ownership/Outputs?csf=1&web=1&e=vbPSCa)).

## Data

[National Polygon Service (NPS)](https://use-land-property-data.service.gov.uk/datasets/nps#polygon) datasets was used in this project. They are licenced to core DEFRA only, and were uploaded to a restricted workspace on the [DASH platform](https://defra.sharepoint.com/:u:/r/sites/Defraintranet/SitePages/The-Data-Analytics-and-Science-Hub-(DASH)-for-the-Defra-group.aspx?csf=1&web=1&e=nN4ncF) for analysis.

[UK companies that own property in England and Wales (CCOD)](https://use-land-property-data.service.gov.uk/datasets/ccod) dataset was used in this project to provide information on the ownership of titles. This data can be downloaded and accessed using the link provided, and is shareable under the terms of the [licence](https://use-land-property-data.service.gov.uk/datasets/ccod/licence/view). Throughout the repo, this is referred to by it associated acronym, CCOD, for brevity.

[EPIMS](https://www.gov.uk/guidance/government-property-unit-electronic-property-information-mapping-service) was used for validation of the data product. Some of this data is available on [data.gov](https://www.data.gov.uk/dataset/c186e17f-654d-4134-aed7-b3f13469546a/central-government-welsh-ministers-and-local-government-including-property-and-land), but a more complete version was obtained from the cabinet office directly and added to the Restricted DASH Platform workspace.

Forestry England Land Registrations dataset was supplied directly by Forestry England to halp validate the data product. 

Forestry England Ownerships dataset was supplied directly by Forestry England to help validate the data product.

Natural England title number list was supplied directly by Natural England to help validate the data product.

Environment Agency title number list was supplied directly by the Environment Agency to help validate the data product.

### Licences

The data product is licenced for full use by Core DEFRA staff only. To share the data product outside Core DEFRA a separate end user licence must be completed and shared with Naomi Lees. Details on how to do this can be found in the [Project Overview document](https://defra.sharepoint.com/:w:/r/teams/Team1608/ESA%20Team%20Resources/Spatial%20Data%20Science/Workstreams/land_ownership/Outputs/Project%20overview.docx?d=wabee27591785443b81a9cd869553824b&csf=1&web=1&e=kvz0rY) on sharepoint.

## Scripts

Below are details of the scripts found within this repo.

### General

- `paths`: all data paths used for the analysis, including inputs and outputs
- `output_export`: required specifically for databricks, this script enables easy download of files from the dbfs
- `constants`: space to allow user to set global variables (primarily used to set organisation names of interest, which should be edited as required depending on the organisations land is being investigated for)

### Data production

- `identify_title_numbers`: working with UK Company Proprietor dataset (ccod), produce a version with only titles of interest, and additional fields to represent current and historic organisation of interes names
- `convert_nps_to_parquet`: working with the national polygon dataset as shapefiles is slow, converting them to parquet once for use speeds up future processing. This script also produces a joined version of the NPS and CCOD data, linking polygon geometries to title numbers and ownership companies.
- `identify_land_parcels`: using output from 'identify_title_numbers', filter the national polygon dataset for land parcels of interest
- `create_organisation_level_data`: dissolve dataset to provide a single flat multipolygon record for each organisation, and each organisation-tenure combination

### Data summary

- `area_calculations`: calculate freehold, leasehold and total area of land for each organisation (output is a table/csv)
- `plotter`: produce spatial plot of outputs

### Data validation

- `data_comparison_epims`: compare dataset produced using hmlr data to epims, organisation by organisation
- `data_comparison_alb`: compare dataset produced using hmlr data to land ownership datasets supplied by albs
- `data_comparison_defra`: compare identified proprietor names for defra to proprietor names previously produced for 30x30 work
- `data_comparison_area`: compare calculated area figures from produced dataset to calculated epims and alb area figures
- `data_comparison_gaps`: for any gaps identified by data comparisons, get the UK Company Proprietor data for the gaps
- `data_comparison_postcode`: validating alb vs defra records. Compare postcodes associated with alb land parcels with postcodes associated with defra records. This is to identify if/ help to disentangle defra and alb owned land parcels.
- `create_study_boundary`: create a small sample polygon area (national polgon service data can then be clipped to this for manual QA)
- `data_validation_overlaps`: assess overlaps which exist in the data (for freehold only as leasehold overlap is expected), both within defra land and between defra and non-defra land
