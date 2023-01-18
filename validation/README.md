# Data Validation - Version 1

The Data Validation is package developed by Members of Scientific Staff's MetaDQ team. The package is intended to be used by developers to check data quality. The package requires metadata information on data like attribute name (column name), datatype, datatype limit, nullable, etc. Based on this information, it runs generic and datatype specific validations and generates Data Quality report on the most granular level. This report can be utilized in multiple ways, for example:
* Data owners and data stewards can fix quality issues
* Developers can run analytics and create dashboards for decision-making by executives


## Modules of Data Validation -

1. Generic Validation - This module includes an initial examination of data to check for completeness (nulls, missing values, mandatory fields), uniqueness (data duplication), accuracy (attribute/column names in data and metadata) and validaty (range of values, unique values).

2. Datatype Specific Validation - This module includes checking data for conformity (conform with standard definitions of datatype, size, format, etc specified in metadata).


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

* [NumPy](https://numpy.org/)
* [Pandas](https://pandas.pydata.org/)
* [Pyspark](https://spark.apache.org/docs/latest/api/python/)
* [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)


## Inputs

* S3 object filepath of data to be validated
* S3 object filepath of corresponding metadata
* S3 object filepath where data quality report generated will be saved
* Bucket name where data and metadata are stored

## Output

* Data Quality report in CSV format saved as S3 object

## Using Data Validation package

1. Upload validation folder into Amazon SageMaker from AWS account that stores data and metadata.
2. Install package dependencies by running the following command in SageMaker Terminal:
```
pip install -r requirements.txt
```
3. Use the package to run data quality checks on your data by running the following command in SageMaker Terminal. Replace data_filepath, metadata_filepath, report_filepath and bucket_name with corresponding S3 object filepaths and bucket that stores the data.
```
python run.py <data_filepath> <metadata_filepath> <report_filepath> <bucket_name>
```
