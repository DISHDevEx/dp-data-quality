"""
Pytest file to test s3_to_s3_validation_script.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""

import pytest
from datetime import datetime, timezone
import boto3
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from s3_to_s3_validation_script import *

# def test_get_target_location():
#     # From glue job instance, not able to test in sagemaker

# def test_get_file_location():
#     # From glue job instance, not able to test in sagemaker

def test_bucket_validation(s3_bucket, s3_resource):
    # get a dict or none
    pass

def test_prefix_to_list(s3_bucket, s3_prefix, s3_resource):
    # get a list or none
    pass

def test_prefix_validation(s3_prefix, s3_prefix_list):
    # get a string or none
    pass

def test_get_file_location(trigger_s3_bucket, trigger_s3_path):
    # get 
    pass

def test_get_current_denver_time(time_zone, time_format):
    """
    Test if time gotten is the right one
    Functions are executed on the same second, the result is reliable
    May need execute several time to get assertions true
    """
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    current_den = datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)
    current_not_den = datetime.now().astimezone(timezone.utc).strftime(time_format)
    assert get_current_denver_time(time_zone, time_format) == current_den
    assert get_current_denver_time(time_zone, time_format) != current_not_den

def test_generate_result_location(target_bucket, target_prefix):
    target_bucket = "s3-validation-demo"
    target_prefix = "consilience-export-manifest-files/2022"
    expected_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_consilience-export-manifest-files_2022/"
    incorrect_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_{target_prefix}/"
    actual_result_target_prefix = generate_result_location(target_bucket, target_prefix)
    assert actual_result_target_prefix == expected_result_target_prefix
    assert actual_result_target_prefix != incorrect_result_target_prefix

def test_initial_boto3_client(aws_service):
    aws_service = "sns"
    incorrect_client = None
    expected_client = initial_boto3_client(aws_service)
    # Next line will not be executed in SageMaker, because it's not a valid API call.
    # incorrect_s3_client = validation_obj.initial_boto3_client(incorrect_aws_service)
    assert expected_client != None
    assert incorrect_client == None

def test_initial_boto3_resource():
    aws_service = "s3"
    incorrect_resource = None
    expected_resource = initial_boto3_resource(aws_service)
    # Next line will not be executed in SageMaker, because it's not a valid API call.
    # incorrect_s3_resource = validation_obj.initial_boto3_resource(incorrect_aws_service)
    assert expected_resource != None
    assert incorrect_resource == None

def test_get_sns_name():
    target_bucket = "s3-validation-demo"
    expected_output_name = "s3-validation-demo"
    incorrect_output_name = "a.b.c.d"
    actual_output_name = get_sns_name(target_bucket)
    assert actual_output_name == expected_output_name
    assert actual_output_name != incorrect_output_name

def test_get_sns_arn(test_initial_boto3_client_fixture):
    # This is an existing sns_name while coding, however our SageMaker instance is not able to reach out there.
    sns_name = "s3-validation-demo"
    # wront_sns_name = "fake_name"
    expected_response = "arn:aws:sns:us-west-2:064047601590:s3-validation-demo"
    response = get_sns_arn(test_initial_boto3_client_fixture, sns_name)
    # fail_response = get_sns_arn(test_initial_boto3_client_fixture, wront_sns_name)
    assert response == expected_response
    # assert fail_response != expected_response
       
# def test_sns_send(test_object):
# import boto3
# import json
# sns_client = boto3.client('sns')
# try:
#     response = sns_client.publish(
#         TargetArn='arn:aws:sns:us-east-1:064047601590:s3-validation-demoww',
#         Message=json.dumps({'default': json.dumps('afawer', indent = 6)}),
#         Subject='awerawer',
#         MessageStructure='json')
# except sns_client.exceptions.InvalidParameterException as e:
#     print('catch this error')
#     print(e)
# except sns_client.exceptions.NotFoundException as e:
#     print('catch not found error')
#     print(e)

def test_rename_columns(pyspark_df, **kwargs, test_second_df_fixture):
    original_column_names = test_second_df_fixture.columns
    rename_cols = {"Size": "bSize","Path": "bPath"}
    renamed_df_column_names = rename_columns(test_second_df_fixture, **rename_cols).columns
    expected_names = ["bPath", "bSize", "Date"]
    assert set(renamed_df_column_names) == set(expected_names)
    assert set(renamed_df_column_names) != set(original_column_names)

def test_file_to_pyspark_df(spark, file_bucket, file_prefix, schema, test_setup_spark_fixture):
    result = None
    spark = test_setup_spark_fixture
    file_bucket = "s3-validation-demo"
    file_prefix = "test"
    schema_str = 'Site string, Assessment string, Path string, Size long'
    result = file_to_pyspark_df(spark, file_bucket, file_prefix, schema_str)
    
    # fake_spark = test_setup_spark_fixture
    # fake_file_bucket = "s3-validation-demo-foo"
    # fake_file_prefix = "test-foo"
    # fake_schema_str = 'Site string, Path string, Size long'
    # fake_result = file_to_pyspark_df(fake_spark, fake_file_bucket, fake_file_prefix, fake_schema_str)
    
    assert result != None
    # assert fake_result == None

def test_s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format):
    result = None
    s3_resource = boto3.resource('s3')
    target_bucket = "s3-validation-demo"
    target_prefix = "consilience-export-manifest-files/2022"
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    result = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)

    # fake_s3_resource = boto3.resource('s3') 
    # fake_target_bucket = "s3-validation-demo-foo"
    # fake_target_prefix = "consilience-export-manifest-files/2022-foo"
    # fake_time_format = '%Y%m%d_%H%M%S'
    # fake_result = s3_obj_to_list(fake_s3_resource, fake_target_bucket, fake_target_prefix, fake_time_format)

    assert result != None
    # assert fake_result == None

def test_list_to_pyspark_df(spark, obj_list, test_setup_spark_fixture):
    obj_list = [{"name":"alice", "age":19},{"name":"bob", "age":20},{"name":"cindy", "age":21} ]
    spark = test_setup_spark_fixture
    result = list_to_pyspark_df(spark, obj_list)

    fake_obj_list = 'fake_list'
    fake_spark = test_setup_spark_fixture
    fake_result = list_to_pyspark_df(fake_spark, fake_obj_list)

    assert result.count() == 3
    assert fake_result == None

def test_get_script_prefix(target_prefix, script_file_name):
    target_prefix = "consilience-export-manifest-files/2022"
    expected_result = "consilience-export-manifest-files/2022/fake_name"
    unexpected_result = "consilience-export-manifest-files/2022//fake_name"
    result = get_script_prefix(target_prefix, "fake_name")
    assert result == expected_result
    assert result != unexpected_result

def test_remove_script_from_df(pyspark_df, remove_value, column_name, test_second_df_fixture):
    remove_value = "consilience-export-manifest-files/2022/s3_to_s3_validation.csv"
    column_name = "Path"
    result = remove_script_from_df(test_second_df_fixture, remove_value, column_name)

    # fake_remove_value = "consilience-export-manifest-files/2022/s3_to_s3_validation.csv-foo"
    # fake_column_name = "Path-foo"
    # fake_result = remove_script_from_df(test_second_df_fixture, fake_remove_value, fake_column_name)
    # assert fake_result.count() == 28
    assert result.count() == 27

def test_get_missing_objects(test_file_to_df_fixture_fixture, test_rename_bucket_df_fixture):
    missing_df = get_missing_objects(test_file_to_df_fixture_fixture, test_rename_bucket_df_fixture,
                                                "Path", "bPath")
    should_missing = ['consilience-export-manifest-files/2022/shouldbemissing1',
                        'consilience-export-manifest-files/2022/shoudbemissing2',
                        'consilience-export-manifest-files/2022/shouldbemissing3',
                        'consilience-export-manifest-files/2022/3moreshouldbemissing']
    missing_list = list(missing_df.select('Path').toPandas()['Path'])
    # fake_missing_df = get_missing_objects(test_file_to_df_fixture_fixture, test_rename_bucket_df_fixture,
    #                                             "Path_foo", "bPath_foo")
    # fake_mssing_list = list(fake_missing_df.select('Path').toPandas()['Path'])
    assert set(should_missing).issubset(set(missing_list)) == 1
    # assert set(should_missing).issubset(set(fake_mssing_list)) == 0

def test_get_df_count(test_second_df_fixture):
    row_count = get_df_count(test_second_df_fixture)
    # fake_row_count = get_df_count('fake_input')
    assert row_count == 27
    # assert fake_row_count == None

def test_get_match_objects(test_file_to_df_fixture, test_rename_bucket_df_fixture):
    match_df = get_match_objects(test_file_to_df_fixture, test_rename_bucket_df_fixture,
                            "Path", "Size", "bPath", "bSize", "Date")
    row_count = match_df.count()
    # fake_match_df = get_match_objects(test_file_to_df_fixture, test_rename_bucket_df_fixture,
    #                         "o", "p", "q", "r", "s")
    assert row_count == 11
    # assert fake_match_df == None

def test_get_wrong_size_objects(test_get_match_objects_fixture):
    wrong_size_df = get_wrong_size_objects(test_get_match_objects_fixture, "Size", "bSize")
    row_count = wrong_size_df.count()
    # fake_wrong_size_df = get_wrong_size_objects(test_get_match_objects_fixture, "o", "p")
    assert row_count == 8
    # assert fake_wrong_size_df == None

def test_save_result_to_s3(test_file_to_df_fixture, test_empty_dataframe_fixture_fixture_fixture_fixture_fixture_fixture, test_get_current_denver_time_fixture):
    current = test_get_current_denver_time_fixture
    bucket = "s3-validation-demo"
    data_key = "pytest_result/s3_to_s3_validation_pytest_result.csv"
    result_location = f"s3a://{bucket}/{data_key}"
    row_count_empty = test_empty_dataframe_fixture_fixture_fixture_fixture_fixture_fixture.count()
    row_count_non_empty = test_file_to_df_fixture.count()
    obj_name = "test_object"
    message_empty = None
    message_non_empty = None
    message_empty = save_result(row_count_empty, result_location, current, test_empty_dataframe_fixture_fixture_fixture_fixture_fixture, obj_name)
    message_non_empty = save_result(row_count_non_empty, result_location, current, test_file_to_df_fixture, obj_name)
    assert message_empty == "no test_object item found"
    assert message_non_empty == "result saved"

# # Cannot test this function, because current IAM role cannot publish message to SNS.
# def test_send_sns_to_subscriber(test_object, test_initial_boto3_client_fixture, test_get_current_denver_time_fixture):
#     sns_client = test_initial_boto3_client_fixture
#     sns_topic_arn = "arn:aws:sns:us-west-2:064047601590:dataquality_pytest"
#     missing_message = "missing"
#     wrong_size_message = "wrong size"
#     current = test_get_current_denver_time_fixture
#     target_prefix = "target_prefix"
#     target_bucket = "target_bucket"
#     response_sns = test_object.result_to_subscriber(missing_message, wrong_size_message,\
#          current, target_prefix, target_bucket, sns_client, sns_topic_arn)
#     assert response_sns != None


print("Tests completed")