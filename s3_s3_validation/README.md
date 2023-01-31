___
# What is S3 to S3 Validation:
## This is a processing validation. When someone needs to check they have gotten required objects with correct size in Target S3 bucket under a specific folder, a .csv file needs to provided for this validation.
## This version mainly support .csv in format provided by DISH Drone team: no header, 4 columns, 
1st column: Site
2nd column: Accessment
3rd column: Path
4th column: Size
## 3rd and 4th columns will be used for validation in one dataframe
## Actual values for objects on Path, Size and Date under Target folder in Target S3 bucket will be used for validation in another PySpark dataframe
## Two results dataframe:
missing_df -> values in this dataframe exist in provided file, but not in target S3 bucket's folder.
wrong_size_df -> values in this dataframe exist in both file and target S3 bucket's folder, but file size is inconsistent.

___
# How to use S3 to S3 Validation:
## Deploy resources in the same account in the same region in AWS by create CloudFormation stacks using three yaml files below.
s3validation_new.yaml -> upload and generate stack for Glue Job (function), Lambda (as a trigger from S3) and SNS (for notification)
s3validation_import.yaml -> import existing Target S3 bucket into stack above
s3validation_update.yaml -> update stack above by adding the trigger from S3 to Lambda
## Upload s3_to_s3_validation_script.py to Target folder under Target S3 bucket
## The provided file should be uploaded to trigger folder with Target S3 bucket to start the validation
## This file's name must be s3_to_s3_validation.csv (s3_to_s3_validation.csv in this folder is an example template)
## The results will be saved in S3 at this path: <TargetS3>/<TargetFolder>/<TargetS3_TargetFolder>/<results>
## Alert emails will be sent to subscribers of SNS (Target S3 name without symbol of dot, e.g.: '.')

___
# Future work:
1. File template will be more generic, not only for DISH Drone team
2. Currenly executing by AWS console, AWS CLI should be doable in order to generate work flow automatically
3. Need to explore cross accounts execution
4. Place code in GitHub and fetch by an initial trigger automatically from AWS, if cross accounts is realized
5. Be packed up as a module, so this can be imported by other developers if possible

___
# Pylint and Pytest results:
## Pylint:
Your code has been rated at 6.25/10 (previous run: 6.67/10, -0.42)
## Pytest:
venv) sagemaker-user@studio$ pytest test_s3_to_s3_validation_script.py --verbose
======================================================================== test session starts ========================================================================
platform linux -- Python 3.9.15, pytest-7.2.1, pluggy-1.0.0 -- /home/sagemaker-user/git/skynet/venv/bin/python
cachedir: .pytest_cache
rootdir: /home/sagemaker-user/git/skynet
collected 12 items                                                                                                                                                  

test_s3_to_s3_validation_script.py::test_get_current_denver_time PASSED                                                                                       [  8%]
test_s3_to_s3_validation_script.py::test_generate_result_location PASSED                                                                                      [ 16%]
test_s3_to_s3_validation_script.py::test_initial_boto3_client PASSED                                                                                          [ 25%]
test_s3_to_s3_validation_script.py::test_initial_boto3_resource PASSED                                                                                        [ 33%]
test_s3_to_s3_validation_script.py::test_get_sns_name PASSED                                                                                                  [ 41%]
test_s3_to_s3_validation_script.py::test_get_sns_arn PASSED                                                                                                   [ 50%]
test_s3_to_s3_validation_script.py::test_rename_bucket_df PASSED                                                                                              [ 58%]
test_s3_to_s3_validation_script.py::test_get_missing_objects PASSED                                                                                           [ 66%]
test_s3_to_s3_validation_script.py::test_get_df_count PASSED                                                                                                  [ 75%]
test_s3_to_s3_validation_script.py::test_get_match_objects PASSED                                                                                             [ 83%]
test_s3_to_s3_validation_script.py::test_get_wrong_size_objects PASSED                                                                                        [ 91%]
test_s3_to_s3_validation_script.py::test_save_result PASSED                                                                                                   [100%]

========================================================================= warnings summary ==========================================================================
venv/lib/python3.9/site-packages/botocore/httpsession.py:41
  /home/sagemaker-user/git/skynet/venv/lib/python3.9/site-packages/botocore/httpsession.py:41: DeprecationWarning: 'urllib3.contrib.pyopenssl' module is deprecated and will be removed in a future release of urllib3 2.x. Read more in this issue: https://github.com/urllib3/urllib3/issues/2680
    from urllib3.contrib.pyopenssl import orig_util_SSLContext as SSLContext

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
================================================================== 12 passed, 1 warning in 35.17s ===================================================================
(venv) sagemaker-user@studio$ 
___
