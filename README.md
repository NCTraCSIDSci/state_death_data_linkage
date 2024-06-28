# state_death_data_linkage

*This repository contains code developed by the TraCS Data Science Lab, which is part of the School of Medicine at the University of North Carolina at Chapel Hill. 
This code has been modified from its original form to protect proprietary information and improve interpretability out of context.  
For example, most paths have been removed, some table field names have been changed, and some sections of code that were redundant across files have been removed.*


## Code Source Environment Notes
The original versions of these files were created in Azure Databricks notebooks, which support Markdown annotations and the use of multiple languages within a code notebook via magic commands.
Thus, these files retain indicators of breaks between code cells (indicated by -- COMMAND ----------) and indicators of Markdown (-- MAGIC %md) or Python language cells (-- MAGIC %python) within SQL notebooks.


## Description
This code is part of a pipeline that uses state death data files to supplement the patient death data available in EHR.

death_data_linkage_prod.sql is the finalized version of the linkage code that has been employed in a scheduled pipeline. This code takes patient data from an EHR table and state death data files and links these records. Positive matches are written to an output table.

death_data_linkage_dev.sql contains some additional code used in the development of the code in death_data_linkage_prod.sql, including extracting some data for training (which was then manually labeled), training the ML model used in the prod code, and evaluating model performance on the training data using cross-validation.


## Authors
Peter J. Leese and John P. Powers developed this code.  
