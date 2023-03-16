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
# Diagram:
![s3_to_s3_validation_diagram](images/s3_to_s3_validation.drawio.png)
___
# How to use S3 to S3 Validation:
## Deploy resources in the same account in the same region in AWS by create CloudFormation stacks using three yaml files below.
s3validation_new.yaml -> upload and generate stack for Glue Job (function), Lambda (as a trigger from S3) and SNS (for notification)
s3validation_import.yaml -> import existing Target S3 bucket into stack above
s3validation_update.yaml -> update stack above by adding the trigger from S3 to Lambda
## Upload s3_to_s3_validation_script.py to Target folder under Target S3 bucket
## The provided file should be uploaded to trigger folder with Target S3 bucket to start the validation
## This file's name must be s3_to_s3_validation.csv (s3_to_s3_validation.csv in this folder is an example template)
## The results will be saved in S3 at this path: <TargetS3>/s3_to_s3_validation_result_<TargetS3_TargetFolder>/<results>
## Alert emails will be sent to subscribers of SNS (Target S3 name without symbol of dot, e.g.: '.')

___
# Steps in validation Python code:

1. Setup basic arguements for s3 to s3 validation
2. Get initial arguements from Glue Job sys and other helper functions
3. Read file into PySpark dataframe
4. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe
5. remove validation script from PySpark dataframe
6. Prepare and do comparisons on two dataframes
7. Save validation result to Target S3 with the same level as the Target folder
8. Send out notification to SNS subscribers

Please comment from awsglue.utils import getResolvedOptions and from awsglue.context import GlueContext,
if using pytest with this file
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
************* Module s3_to_s3_validation_script
s3_to_s3_validation_script.py:14:0: E0401: Unable to import 'awsglue.utils' (import-error)
s3_to_s3_validation_script.py:15:0: E0401: Unable to import 'awsglue.context' (import-error)
s3_to_s3_validation_script.py:819:0: R0913: Too many arguments (7/5) (too-many-arguments)
s3_to_s3_validation_script.py:836:8: R0916: Too many boolean expressions in if statement (6/5) (too-many-boolean-expressions)
s3_to_s3_validation_script.py:873:0: R0914: Too many local variables (37/15) (too-many-locals)

------------------------------------------------------------------
Your code has been rated at 9.76/10 (previous run: 9.66/10, +0.10)

## Pytest:
collected 82 items                                                         

test_s3_to_s3_validation_script.py::test_get_current_denver_time_correct[US/Mountain-%Y%m%d_%H%M%S_%Z_%z] PASSED [  1%]
test_s3_to_s3_validation_script.py::test_get_current_denver_time_incorrect[fake_timezone-%Y%m%d_%H%M%S_%Z_%z] PASSED [  2%]
test_s3_to_s3_validation_script.py::test_bucket_validation_correct[s3-validation-demo-fixture_initialize_boto3_resource] PASSED [  3%]
test_s3_to_s3_validation_script.py::test_bucket_validation_incorrect[s3-validation-demo-fake_s3_resource] PASSED [  4%]
test_s3_to_s3_validation_script.py::test_bucket_validation_incorrect[fake_s3_bucket-fixture_initialize_boto3_resource] PASSED [  6%]
test_s3_to_s3_validation_script.py::test_prefix_to_list_correct[s3-validation-demo-test-fixture_initialize_boto3_resource] PASSED [  7%]
test_s3_to_s3_validation_script.py::test_prefix_to_list_incorrect[fake bucket-test-fixture_initialize_boto3_resource] PASSED [  8%]
test_s3_to_s3_validation_script.py::test_prefix_to_list_incorrect[s3-validation-demo-fake_prefix-fixture_initialize_boto3_resource] PASSED [  9%]
test_s3_to_s3_validation_script.py::test_prefix_to_list_incorrect[s3-validation-demo-test-fake_s3_resource] PASSED [ 10%]
test_s3_to_s3_validation_script.py::test_prefix_validation_correct[test-s3_prefix_list0] PASSED [ 12%]
test_s3_to_s3_validation_script.py::test_prefix_validation_correct[test/-s3_prefix_list1] PASSED [ 13%]
test_s3_to_s3_validation_script.py::test_prefix_validation_incorrect[test-s3_prefix_list0] PASSED [ 14%]
test_s3_to_s3_validation_script.py::test_prefix_validation_incorrect[test/-s3_prefix_list1] PASSED [ 15%]
test_s3_to_s3_validation_script.py::test_generate_result_location_correct[s3-validation-demo-consilience-export-manifest-files/2022] PASSED [ 17%]
test_s3_to_s3_validation_script.py::test_generate_result_location_incorrect[target_bucket0-consilience-export-manifest-files/2022] PASSED [ 18%]
test_s3_to_s3_validation_script.py::test_generate_result_location_incorrect[s3-validation-demo-target_prefix1] PASSED [ 19%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_client_correct[s3] PASSED [ 20%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_client_correct[sns] PASSED [ 21%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_client_incorrect[ooo] PASSED [ 23%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_client_incorrect[ppp] PASSED [ 24%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_client_incorrect[aws_service2] PASSED [ 25%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_resource_correct[s3] PASSED [ 26%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_resource_correct[sns] PASSED [ 28%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_resource_incorrect[ooo] PASSED [ 29%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_resource_incorrect[ppp] PASSED [ 30%]
test_s3_to_s3_validation_script.py::test_initialize_boto3_resource_incorrect[aws_service2] PASSED [ 31%]
test_s3_to_s3_validation_script.py::test_get_sns_name_correct[example_bucket] PASSED [ 32%]
test_s3_to_s3_validation_script.py::test_get_sns_name_correct[test_bucket] PASSED [ 34%]
test_s3_to_s3_validation_script.py::test_get_sns_name_incorrect[target_bucket0] PASSED [ 35%]
test_s3_to_s3_validation_script.py::test_get_sns_name_incorrect[target_bucket1] PASSED [ 36%]
test_s3_to_s3_validation_script.py::test_get_sns_arn_correct[fixture_initialize_boto3_client-s3-validation-demo] PASSED [ 37%]
test_s3_to_s3_validation_script.py::test_get_sns_arn_incorrect[fake_client-s3-validation-demo] PASSED [ 39%]
test_s3_to_s3_validation_script.py::test_get_sns_arn_incorrect[fixture_initialize_boto3_client-fake_bucket] PASSED [ 40%]
test_s3_to_s3_validation_script.py::test_rename_columns_correct[fixture_second_df-renames0] PASSED [ 41%]
test_s3_to_s3_validation_script.py::test_rename_columns_incorrect_df[fake_df-renames0] PASSED [ 42%]
test_s3_to_s3_validation_script.py::test_rename_columns_incorrect_dict[fixture_second_df-renames0] PASSED [ 43%]
test_s3_to_s3_validation_script.py::test_file_to_pyspark_df_correct[fixture_setup_spark-s3-validation-demo-test/s3_to_s3_validation.csv-fixture_schema] PASSED [ 45%]
test_s3_to_s3_validation_script.py::test_file_to_pyspark_df_incorrect[fake_spark-s3-validation-demo-test-fixture_schema] PASSED [ 46%]
test_s3_to_s3_validation_script.py::test_file_to_pyspark_df_incorrect[fixture_setup_spark-fake_bucket-test-fixture_schema] PASSED [ 47%]
test_s3_to_s3_validation_script.py::test_file_to_pyspark_df_incorrect[fixture_setup_spark-s3-validation-demo-fake_prefix-fixture_schema] PASSED [ 48%]
test_s3_to_s3_validation_script.py::test_file_to_pyspark_df_incorrect[fixture_setup_spark-s3-validation-demo-test-fake_schema] PASSED [ 50%]
test_s3_to_s3_validation_script.py::test_s3_obj_to_list_correct[fixture_initialize_boto3_resource-s3-validation-demo-test-%Y%m%d_%H%M%S_%Z_%z] PASSED [ 51%]
test_s3_to_s3_validation_script.py::test_s3_obj_to_list_incorrect[fake_resource-s3-validation-demo-test-%Y%m%d_%H%M%S_%Z_%z] PASSED [ 52%]
test_s3_to_s3_validation_script.py::test_s3_obj_to_list_incorrect[fixture_initialize_boto3_resource-fake_bucket-test-%Y%m%d_%H%M%S_%Z_%z] PASSED [ 53%]
test_s3_to_s3_validation_script.py::test_s3_obj_to_list_incorrect[fixture_initialize_boto3_resource-s3-validation-demo-fake_prefix-%Y%m%d_%H%M%S_%Z_%z] PASSED [ 54%]
test_s3_to_s3_validation_script.py::test_s3_obj_to_list_incorrect[fixture_initialize_boto3_resource-s3-validation-demo-test-time_format3] PASSED [ 56%]
test_s3_to_s3_validation_script.py::test_list_to_pyspark_df_correct[fixture_setup_spark-obj_list0] PASSED [ 57%]
test_s3_to_s3_validation_script.py::test_list_to_pyspark_df_incorrect[fake_spark-obj_list0] PASSED [ 58%]
test_s3_to_s3_validation_script.py::test_list_to_pyspark_df_incorrect[fixture_setup_spark-fake_list] PASSED [ 59%]
test_s3_to_s3_validation_script.py::test_list_to_pyspark_df_incorrect[fixture_setup_spark-obj_list2] PASSED [ 60%]
test_s3_to_s3_validation_script.py::test_get_script_prefix_correct[test_prefix-test_file_name] PASSED [ 62%]
test_s3_to_s3_validation_script.py::test_get_script_prefix_incorrect[target_prefix0-test_file_name] PASSED [ 63%]
test_s3_to_s3_validation_script.py::test_get_script_prefix_incorrect[test_prefix-script_file_name1] PASSED [ 64%]
test_s3_to_s3_validation_script.py::test_remove_script_from_df_correct[fixture_file_to_df-consilience-export-manifest-files/2022/duplicate-sites.csv-path] PASSED [ 65%]
test_s3_to_s3_validation_script.py::test_remove_script_from_df_incorrect_df[fake_df-consilience-export-manifest-files/2022/duplicate-sites.csv-path] PASSED [ 67%]
test_s3_to_s3_validation_script.py::test_remove_script_from_df_incorrect_filter[fixture_file_to_df-remove_value0-path] PASSED [ 68%]
test_s3_to_s3_validation_script.py::test_remove_script_from_df_incorrect_filter[fixture_file_to_df-consilience-export-manifest-files/2022/duplicate-sites.csv-column_name1] PASSED [ 69%]
test_s3_to_s3_validation_script.py::test_get_missing_objects_correct[fixture_file_to_df-fixture_second_df_renamed-path-b_path] PASSED [ 70%]
test_s3_to_s3_validation_script.py::test_get_missing_objects_incorrect[fake_df-fixture_second_df_renamed-path-b_path] PASSED [ 71%]
test_s3_to_s3_validation_script.py::test_get_missing_objects_incorrect[fixture_file_to_df-fake_second_df_renamed-path-b_path] PASSED [ 73%]
test_s3_to_s3_validation_script.py::test_get_missing_objects_incorrect[fixture_file_to_df-fixture_second_df_renamed-fake_path-b_path] PASSED [ 74%]
test_s3_to_s3_validation_script.py::test_get_missing_objects_incorrect[fixture_file_to_df-fixture_second_df_renamed-path-_fake_b_path] PASSED [ 75%]
test_s3_to_s3_validation_script.py::test_get_df_count_correct[fixture_file_to_df] PASSED [ 76%]
test_s3_to_s3_validation_script.py::test_get_df_count_incorrect[fake_df] PASSED [ 78%]
test_s3_to_s3_validation_script.py::test_get_df_count_incorrect[pyspark_df1] PASSED [ 79%]
test_s3_to_s3_validation_script.py::test_get_match_objects_correct[fixture_file_to_df-fixture_second_df_renamed-path-b_path-columns_dict0] PASSED [ 80%]
test_s3_to_s3_validation_script.py::test_get_match_objects_incorrect[fake_df-fixture_second_df_renamed-path-b_path-columns_dict0] PASSED [ 81%]
test_s3_to_s3_validation_script.py::test_get_match_objects_incorrect[fixture_file_to_df-fake_second_df-path-b_path-columns_dict1] PASSED [ 82%]
test_s3_to_s3_validation_script.py::test_get_match_objects_incorrect[fixture_file_to_df-fixture_second_df_renamed-fake_path-b_path-columns_dict2] PASSED [ 84%]
test_s3_to_s3_validation_script.py::test_get_match_objects_incorrect[fixture_file_to_df-fixture_second_df_renamed-path-fake_b_path-columns_dict3] PASSED [ 85%]
test_s3_to_s3_validation_script.py::test_get_match_objects_incorrect[fixture_file_to_df-fixture_second_df_renamed-path-b_path-columns_dict4] PASSED [ 86%]
test_s3_to_s3_validation_script.py::test_get_match_objects_incorrect[fixture_file_to_df-fixture_second_df_renamed-path-b_path-fake_dict] PASSED [ 87%]
test_s3_to_s3_validation_script.py::test_get_wrong_size_objects_correct[fixture_get_match_objects-size-b_size] PASSED [ 89%]
test_s3_to_s3_validation_script.py::test_get_wrong_size_objects_incorrect[fake_df-size-b_size] PASSED [ 90%]
test_s3_to_s3_validation_script.py::test_get_wrong_size_objects_incorrect[fixture_get_match_objects-fake_size-b_size] PASSED [ 91%]
test_s3_to_s3_validation_script.py::test_get_wrong_size_objects_incorrect[fixture_get_match_objects-size-fake_b_size] PASSED [ 92%]
test_s3_to_s3_validation_script.py::test_save_result_to_s3_correct[s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/-fixture_get_current_denver_time-fixture_file_to_df-test_object] PASSED [ 93%]
test_s3_to_s3_validation_script.py::test_save_result_to_s3_no_object[s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/-fixture_get_current_denver_time-fixture_empty_dataframe-test_object] PASSED [ 95%]
test_s3_to_s3_validation_script.py::test_save_result_to_s3_incorrect[this is a fake s3 bucket-fixture_get_current_denver_time-fixture_file_to_df-test_object] PASSED [ 96%]
test_s3_to_s3_validation_script.py::test_save_result_to_s3_incorrect[s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/-current1-fixture_file_to_df-test_object] PASSED [ 97%]
test_s3_to_s3_validation_script.py::test_save_result_to_s3_incorrect[s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/-fixture_get_current_denver_time-this is a fake pyspark dataframe-test_object] PASSED [ 98%]
test_s3_to_s3_validation_script.py::test_save_result_to_s3_incorrect[s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/-fixture_get_current_denver_time-fixture_file_to_df-obj_name3] PASSED [100%]
___
