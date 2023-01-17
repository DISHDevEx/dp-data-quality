# Data Validation Module - Version 1

This is Version 1 of Data Validation module. This module is intended to be used by developers who wish to check quality of their data. The module requires metadata information like attribute name (column name), datatype, datatype limit, nullable, etc on data. Based on this information, the module runs generic and datatype specific validations and generates Data Quality report on the most granular level. The report can be used by data owners and data stewards to fix quality issues. This report can also be used by developers to run analytics and create dashboards for decision-making.

## Components of Validation Module -

1. Generic Validation - This includes an initial examination of data to checks for completeness (nulls, missing values, mandatory fields), uniqueness (data duplication), accuracy (attribute/column names in data and metadata) and validaty (range of values, unique values).

2. Datatype Specific Validation - This includes validating data against metadata to check for conformity (conform with standard definitions of datatype, size, format, etc specified in metadata).

## Inputs

* S3 object filepath of data to be validated
* S3 object filepath of corresponding metadata
* S3 object filepath of where data quality report generated will be saved
* Bucket name where data and metadata are stored

## Output

* Data Quality report in CSV format saved as S3 object

## Dependencies

* Numpy
* Pandas
* Pyspark
* Boto3

To run:
```
python run.py <data_filepath> <metadata_filepath> <report_filepath> <bucket_name>
```
