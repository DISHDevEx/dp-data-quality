In progress
# Data Validation Module - Version 1

This is Version 1 of Data Validation module. This module is intended to be used by developers who wish to check quality of their data. It requires Metadata information. The module runs generic and datatype specific validations based on information from Metadata and generates Data Quality report on a granular level. The report can be used by data owners and data stewards to fix quality issues. This report can also be used by developers to run analytics and create dashboards for decision-making.

## Components of Validation Module -

The module is broken down into two companents - generic and datatype specific.
Generic Validations - These contain vaidation checks that are generic to any dataset.

Datatype Specific Validations

## Inputs

* Filepath of data to be validated in S3 bucket 1
* Filepath of corresponding metadata in S3 bucket 1
* Filepath of where data quality report generated will be saved in S3 bucket 2
* Bucket name that where data and metadata is stored

## Outputs

* Data Quality report in CSV format

## Dependencies 

* Numpy
* Pandas
* Pyspark
* Boto3

To run:
```
python run.py <data_filepath> <metadata_filepath> <report_filepath> <bucket_name>
```