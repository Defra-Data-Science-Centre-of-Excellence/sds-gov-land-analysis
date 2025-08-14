# Databricks notebook source
# MAGIC %md
# MAGIC ### Constants
# MAGIC Defining variables which are useful throughout the workflow.
# MAGIC This script is designed to be run from other scripts, so that variables can be defined once and changed easily.

# COMMAND ----------

# Set the name of the department of interest. The name set here will be used later to tag identified titles. So ensure you are happy with the format for your output.
standardised_department_name = 'Ministry of Justice'

# generic department identifiers, these shouldn't need changing
generic_department_identifiers = ['state', 'secretary', 'ministry', 'minister', 'department']

# department specific identifiers
# this should be in the format ['name'] if only one identifier provided, otherwise in the format ['name_1', 'name_2', 'name_X'] if more than one identifier is provided
specific_department_identifiers = ['justice', 'prison']

# COMMAND ----------

''' 
This is a hierarchical nested translation dict in the format:{
    'Current organisation name':{
        'Organisation name instance': [list of found names]
    }
}
Lines 3-5 above can be copied as a template for an additional organisation if required.

Current organisation name: This is the standardised version of the organisation name. This should be set for organisation and will be used to tag identified identified land parcels in the outputs. So ensure you are happy with the format before proceding.
organisation name instance (current or historic) is split by word and used for searching.
Organisation name instance: These are the names which will be searched for in the UK company proprietorship data. Both the 

'''

alb_found_names_translation_dict = {
    'Ministry of Justice':{
        'Criminal Injuries Compensation Authority': [],
        'HM Courts & Tribunals Service': [],
        'HM Prison and Probation Service': [],
        'Legal Aid Agency': [],
        'Office of the Public Guardian': [],
        'Cafcass': [],
        'Criminal Cases Review Commission': [],
        'Independent Monitoring Authority for the Citizens Rights Agreements': [],
        'Judicial Appointments Commission': [],
        'Legal Services Board': [],
        'Parole Board': [],
        'Youth Justice Board for England and Wales': [],
        'Advisory Committees on Justices of the Peace': [],
        'Civil Justice Council': [],
        'Civil Procedure Rule Committee': [],
        'Criminal Procedure Rule Committee': [],
        'Family Justice Council': [],
        'Family Procedure Rule Committee': [],
        'Independent Advisory Panel on Deaths in Custody': [],
        'Insolvency Rules Committee': [],
        'Law Commission': [],
        'Online Procedure Rule Committee': [],
        'Prison Service Pay Review Body': [],
        'Sentencing Council for England and Wales': [],
        'Tribunal Procedure Committee': [],
        'Academy for Social Justice': [],
        'HM Inspectorate of Prisons': [],
        'HM Inspectorate of Probation': [],
        'Independent Monitoring Boards': [],
        'Judicial Appointments and Conduct Ombudsman': [],
        'Judicial Office': [],
        'The Legal Ombudsman': [],
        'Official Solicitor and Public Trustee': [],
        'Prisons and Probation Ombudsman': [],
        'Victims Commissioner': [],
        'Lord Chancellor': [],
        'Department for Constitutional Affairs': [],
        
    },
               
}
