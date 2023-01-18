# Data Validation Module - Version 1

This is Version 1 of Data Validation module. This module is intended to be used by developers check data quality. The module requires metadata information on data like attribute name (column name), datatype, datatype limit, nullable, etc. Based on this information, the module runs generic and datatype specific validations and generates Data Quality report on the most granular level. The report can be used by data owners and data stewards to fix quality issues. This report can be utilized in multiple ways, for example:
* Data owners and data stewards can fix quality issues
* Developers can run analytics and create dashboards for decision-making by executives


## Components of Validation Module -

1. Generic Validation - This includes an initial examination of data to checks for completeness (nulls, missing values, mandatory fields), uniqueness (data duplication), accuracy (attribute/column names in data and metadata) and validaty (range of values, unique values).

2. Datatype Specific Validation - This includes validating data against metadata to check for conformity (conform with standard definitions of datatype, size, format, etc specified in metadata).


## Features of Data Quality Report -

1. DQ_REPORT_ID - Unique ID for report
2. AWS_ACCOUNT_NAME - AWS account that contains data to be validated
3. S3_BUCKET - Bucket that contains data to be validated
4. TABLE_NAME - Table that is being validated
5. COLUMN_NAME - Column from the table that that failed validation check
6. VALIDATION_CATEGORY - Category of data validation - Generic, Datatype Specific, Sensitive Data
7. VALIDATION_ID - Unique ID assigned to each validation check
8. VALIDATION_MESSAGE - Validation error message
9. PRIMARY_KEY_COLUMN - Column that is dataset’s Primary Key or Unique Identifier. In the absence of Primary Key in the table, module assigns ROW_ID as table’s Primary Key
10. PRIMARY_KEY_VALUE - Primary Key value that failed validation check
11. TIMESTAMP - Timestamp of when data validation was performed


## Dependencies

* Numpy
* Pandas
* Pyspark
* Boto3


## Inputs

* S3 object filepath of data to be validated
* S3 object filepath of corresponding metadata
* S3 object filepath where data quality report generated will be saved
* Bucket name where data and metadata are stored

## Output

* Data Quality report in CSV format saved as S3 object

To run:
```
python run.py <data_filepath> <metadata_filepath> <report_filepath> <bucket_name>
```
