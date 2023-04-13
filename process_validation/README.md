# Payload Topic Validation - Version 1

Payload Validation module is developed by MetaDQ team. This module can to be used by developers to check topic data json format w.r.t to standard json format. It compares the input nested json file structure with standard json structure which was pre-defined for inventory by experts.
* it can be used to figure out missing attributes for incoming message

## Features of JSONSchemaReport -

1. ID - Unique identifier 
2. ACCOUNT_ID - AWS Account ID
3. BUCKET_NAME - where the data and report stored
4. VALIDATION_MESSAGE - error message
5. VALIDATION_TYPE - type of validation performed
6. FILE_NAME - input json file name
7. TIMESTAMP - Timestamp of when json schema validation report generated

## Dependencies

* [NumPy](https://numpy.org/)
* [Pandas](https://pandas.pydata.org/)
* [Pyspark](https://spark.apache.org/docs/latest/api/python/)
* [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)


## Inputs

* s3 filepath for standard json file
* s3 filepath for input json file
* bucketname where standard and input files are stored
* standard json file name 
* input json file name

## Output

* JSON schema report in CSV format saved to S3 bucket


## Using Process Validation Module

1. Upload process_validation folder into Amazon SageMaker from AWS account.
2. Install dependencies by running the following command in SageMaker Terminal:
```
pip install -r requirements.txt
```
```
python payloadvalidation.py <bucket_folder_path_standard> <bucket_folder_path_input> <standard_file> <input_file> <bucket_name>
```
4. Logs will be saved in `logfile` file in the same folder.

