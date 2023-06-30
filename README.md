# DISHDataQuality
# Data Validation - Version 1.0.0

DISHDataQuality is a package developed by Members of Scientific Staff's MetaDQ team. The package is intended to be used by developers to check data quality. It requires information on data like attribute names (column names), datatypes, datatype limits, etc called as metadata. Based on this information, it runs generic and datatype specific validations and generates Data Quality report on the most granular level. This package is also creates rich source of the validation checks aka rulebooks. This report can be utilized in multiple ways, for example:
* Data owners and data stewards can fix quality issues
* Developers can run analytics and create dashboards for decision-making

## Modules of Data Qualities

* data_quality_validation (Generic Data Quality, Data Type Specific Quality)
* glue_validation - To validate the auto cataloging of the S3 buckets by Glue Crawler
* naming_validaion - This module will help in validating S3 naming convention with the custom published standards
* payload_validation - This module will help in validating JSON structure withn custom published JSON structures
* s3_s3_validation - This module will validate whether all the files are moved correctly from one s3 to another be it cross account, cross region

## Dependencies

* [NumPy](https://numpy.org/)
* [Pandas](https://pandas.pydata.org/)
* [Pyspark](https://spark.apache.org/docs/latest/api/python/)
* [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)


## Inputs

* S3 object filepath of data to be validated
* S3 object filepath of corresponding metadata
* AWS Account ID of vendor whose data is being validated
* Bucket name where data and metadata are stored

## Output

* Data Quality report in CSV format saved in S3 bucket


## Using Data Validation package

1. Upload validation folder into Amazon SageMaker from AWS account that stores data and metadata.
2. Install package dependencies by running the following command in SageMaker Terminal:
```
pip install -r requirements.txt
```
3. Use the package to run data quality checks on your data by running the following command in SageMaker Terminal. Replace data_filepath, metadata_filepath, account_id and bucket_name with corresponding S3 object filepaths, vendor's AWS account ID and bucket that stores the data. When using S3 url in data_filepath and metadata_filepath, add an 'a' after 's3' in the url. Example url: 's3a://<bucket_name>/\<filepath_in_bucket\>'
```
python run.py <data_filepath> <metadata_filepath> <account_id> <bucket_name>
```
4. Quality Report will be saved in `qualityreport` folder on the root level of `<bucket_name>`.
5. Logs will be saved in `logfile` file in the `data_quality_validation` folder.
