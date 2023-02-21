"""
Pytest file to test s3_to_s3_validation_script.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""

import pytest
from pytest import FixtureRequest as fr
from datetime import datetime, timezone
import boto3
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from s3_to_s3_validation_script import *


@pytest.mark.test_bucket_validation
@pytest.mark.parametrize(
	["s3_bucket", "s3_resource"],
	[
		("s3-validation-demo", "fixture_initialize_boto3_resource")
	]
)
def test_bucket_validation_correct(s3_bucket, s3_resource, request):
	"""
	Test function bucket_validation with correct input:
		s3_bucket
		s3_resource
	Pass criteria:
		result is a dict
	"""
	s3_resource = request.getfixturevalue(s3_resource)
	result = bucket_validation(s3_bucket, s3_resource)
	assert isinstance(result, dict)
	
@pytest.mark.test_bucket_validation	
@pytest.mark.parametrize(
	["s3_bucket", "s3_resource"],
	[
		("s3-validation-demo", "fake_s3_resource"),
		("fake_s3_bucket", "fixture_initialize_boto3_resource")
	]
)
def test_bucket_validation_incorrect(s3_bucket, s3_resource, request):
	"""
	Test function bucket_validation with incorrect input:
		s3_bucket
		s3_resource
	Pass criteria:
		result is None
	"""
	if s3_resource == "fixture_initialize_boto3_resource":
		s3_resource = request.getfixturevalue(s3_resource)
	result = bucket_validation(s3_bucket, s3_resource)
	assert result is None

@pytest.mark.test_save_result_to_s3
def test_save_result_to_s3_correct(fixture_file_to_df,
						   fixture_get_current_denver_time,
						   fixture_initialize_boto3_resource):
	"""
	Test function save_result_to_s3 with correct input:
		result_location
		current
		pyspark_df_non_empty
		obj_name
	Pass criteria:
		result is the expected string
		Saved object is in the s3 bucket at the expected location
	"""
	bucket = "s3-validation-demo"
	data_key = "pytest_result/s3_to_s3_validation_pytest_result/"
	result_location = f"{bucket}/{data_key}"
	current = fixture_get_current_denver_time
	pyspark_df_non_empty = fixture_file_to_df
	obj_name = "test_object"
	result = save_result_to_s3(result_location, current,
									pyspark_df_non_empty, obj_name)
	s3_resource = fixture_initialize_boto3_resource
	s3_resource = boto3.resource('s3')
	s3_bucket_objects_collection = s3_resource.Bucket(bucket) \
.objects.filter(Prefix=f'{data_key}')
	file_name = f'{data_key}{obj_name}_{current}.csv'
	parent_folder_list = []
	for obj in s3_bucket_objects_collection:
		parent_folder_list.append(obj.key)
	if file_name in parent_folder_list:
		obj_exist = True
	else:
		obj_exist = False
	assert result == f"Saved at {result_location}{obj_name}_{current}.csv."
	assert obj_exist is True

@pytest.mark.test_save_result_to_s3
def test_save_result_to_s3_no_object(fixture_empty_dataframe,
						   fixture_get_current_denver_time):
	"""
	Test function save_result_to_s3 with correct input but empty dataframe:
		result_location
		current
		pyspark_df_empty
		obj_name
	Pass criteria:
		result is the expected string
	"""
	bucket = "s3-validation-demo"
	data_key = "pytest_result/s3_to_s3_validation_pytest_result/"
	result_location = f"{bucket}/{data_key}"
	current = fixture_get_current_denver_time
	pyspark_df_empty = fixture_empty_dataframe
	obj_name = "test_object"
	result = save_result_to_s3(result_location, current,
									pyspark_df_empty, obj_name)
	assert result == f"No {obj_name} item found."

@pytest.mark.test_save_result_to_s3
@pytest.mark.parametrize(
	["result_location", "current", "pyspark_df", "obj_name"],
	[
		("this is a fake s3 bucket",
			"fixture_get_current_denver_time",
			"fixture_file_to_df",
			"test_object"
		),
		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
			"this is a fake time",
			"fixture_file_to_df",
			"test_object"
		),
		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
			"fixture_get_current_denver_time",
			"this is a fake pyspark dataframe",
			"test_object"
		),
		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
			"fixture_get_current_denver_time",
			"fixture_file_to_df",
			"this is a fake object name"
		)
	]
)
def test_save_result_to_s3_incorrect(result_location, current, pyspark_df, obj_name, request):
	"""
	Test function save_result_to_s3 with incorrect input:
		result_location
		current
		pyspark_df_empty
		obj_name
	Pass criteria:
		result is None
	"""
	if current != "fixture_get_current_denver_time":
		current = ['fake_current']
	else:
		current = request.getfixturevalue(current)
	if pyspark_df == "fixture_file_to_df":
		pyspark_df = request.getfixturevalue(pyspark_df)
	if obj_name != "test_object":
		obj_name = ['fake_obj_name']
	result = save_result_to_s3(result_location, current,
									pyspark_df, obj_name)
	assert result is None






# # def test_get_file_location():
# #     # From glue job instance, not able to test in sagemaker


# def test_prefix_to_list(s3_bucket, s3_prefix, s3_resource):
#     # get a list or none
#     pass

# def test_prefix_validation(s3_prefix, s3_prefix_list):
#     # get a string or none
#     pass

# def test_get_file_location(trigger_s3_bucket, trigger_s3_path):
#     # get 
#     pass

# def test_get_current_denver_time(time_zone, time_format):
#     """
#     Test if time gotten is the right one
#     Functions are executed on the same second, the result is reliable
#     May need execute several time to get assertions true
#     """
#     time_zone = 'US/Mountain'
#     time_format = '%Y%m%d_%H%M%S_%Z_%z'
#     current_den = datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)
#     current_not_den = datetime.now().astimezone(timezone.utc).strftime(time_format)
#     assert get_current_denver_time(time_zone, time_format) == current_den
#     assert get_current_denver_time(time_zone, time_format) != current_not_den

# def test_generate_result_location(target_bucket, target_prefix):
#     target_bucket = "s3-validation-demo"
#     target_prefix = "consilience-export-manifest-files/2022"
#     expected_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_consilience-export-manifest-files_2022/"
#     incorrect_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_{target_prefix}/"
#     actual_result_target_prefix = generate_result_location(target_bucket, target_prefix)
#     assert actual_result_target_prefix == expected_result_target_prefix
#     assert actual_result_target_prefix != incorrect_result_target_prefix

# def test_initial_boto3_client(aws_service):
#     aws_service = "sns"
#     incorrect_client = None
#     expected_client = initial_boto3_client(aws_service)
#     # Next line will not be executed in SageMaker, because it's not a valid API call.
#     # incorrect_s3_client = validation_obj.initial_boto3_client(incorrect_aws_service)
#     assert expected_client != None
#     assert incorrect_client == None

# def test_initial_boto3_resource():
#     aws_service = "s3"
#     incorrect_resource = None
#     expected_resource = initial_boto3_resource(aws_service)
#     # Next line will not be executed in SageMaker, because it's not a valid API call.
#     # incorrect_s3_resource = validation_obj.initial_boto3_resource(incorrect_aws_service)
#     assert expected_resource != None
#     assert incorrect_resource == None

# def test_get_sns_name():
#     target_bucket = "s3-validation-demo"
#     expected_output_name = "s3-validation-demo"
#     incorrect_output_name = "a.b.c.d"
#     actual_output_name = get_sns_name(target_bucket)
#     assert actual_output_name == expected_output_name
#     assert actual_output_name != incorrect_output_name

# def test_get_sns_arn(test_initial_boto3_client_fixture):
#     # This is an existing sns_name while coding, however our SageMaker instance is not able to reach out there.
#     sns_name = "s3-validation-demo"
#     # wront_sns_name = "fake_name"
#     expected_response = "arn:aws:sns:us-west-2:064047601590:s3-validation-demo"
#     response = get_sns_arn(test_initial_boto3_client_fixture, sns_name)
#     # fail_response = get_sns_arn(test_initial_boto3_client_fixture, wront_sns_name)
#     assert response == expected_response
#     # assert fail_response != expected_response
       
# # def test_sns_send(test_object):
# # import boto3
# # import json
# # sns_client = boto3.client('sns')
# # try:
# #     response = sns_client.publish(
# #         TargetArn='arn:aws:sns:us-east-1:064047601590:s3-validation-demoww',
# #         Message=json.dumps({'default': json.dumps('afawer', indent = 6)}),
# #         Subject='awerawer',
# #         MessageStructure='json')
# # except sns_client.exceptions.InvalidParameterException as e:
# #     print('catch this error')
# #     print(e)
# # except sns_client.exceptions.NotFoundException as e:
# #     print('catch not found error')
# #     print(e)

# def test_rename_columns(pyspark_df, **kwargs, fixture_second_df):
#     original_column_names = fixture_second_df.columns
#     rename_cols = {"Size": "bSize","Path": "bPath"}
#     renamed_df_column_names = rename_columns(fixture_second_df, **rename_cols).columns
#     expected_names = ["bPath", "bSize", "Date"]
#     assert set(renamed_df_column_names) == set(expected_names)
#     assert set(renamed_df_column_names) != set(original_column_names)

# def test_file_to_pyspark_df(spark, file_bucket, file_prefix, schema, fixture_setup_spark):
#     result = None
#     spark = fixture_setup_spark
#     file_bucket = "s3-validation-demo"
#     file_prefix = "test"
#     schema_str = 'Site string, Assessment string, Path string, Size long'
#     result = file_to_pyspark_df(spark, file_bucket, file_prefix, schema_str)
    
#     # fake_spark = fixture_setup_spark
#     # fake_file_bucket = "s3-validation-demo-foo"
#     # fake_file_prefix = "test-foo"
#     # fake_schema_str = 'Site string, Path string, Size long'
#     # fake_result = file_to_pyspark_df(fake_spark, fake_file_bucket, fake_file_prefix, fake_schema_str)
    
#     assert result != None
#     # assert fake_result == None

# def test_s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format):
#     result = None
#     s3_resource = boto3.resource('s3')
#     target_bucket = "s3-validation-demo"
#     target_prefix = "consilience-export-manifest-files/2022"
#     time_format = '%Y%m%d_%H%M%S_%Z_%z'
#     result = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)

#     # fake_s3_resource = boto3.resource('s3') 
#     # fake_target_bucket = "s3-validation-demo-foo"
#     # fake_target_prefix = "consilience-export-manifest-files/2022-foo"
#     # fake_time_format = '%Y%m%d_%H%M%S'
#     # fake_result = s3_obj_to_list(fake_s3_resource, fake_target_bucket, fake_target_prefix, fake_time_format)

#     # s3_resource = boto3.resource('s3')
#     # bucket_name = 's3-validation-demo'
#     # s3_bucket = s3_resource.Bucket(bucket_name)
#     # print(s3_bucket)
#     # print
#     # from botocore.client import ClientError
#     # try:
#     #     a = s3_bucket.objects.filter(Prefix='test')
#     #     print('a::')
#     #     print(a)
#     #     print('type of a ::::')
#     #     print(type(a))
#     # except ClientError as e:
#     #     print("client error!!!!!!!!!!!!!")
#     #     print(e)
        
#     # for item in a:
#     #     print(item.key)

#     assert result != None
#     # assert fake_result == None

# def test_list_to_pyspark_df(spark, obj_list, fixture_setup_spark):
#     obj_list = [{"name":"alice", "age":19},{"name":"bob", "age":20},{"name":"cindy", "age":21} ]
#     spark = fixture_setup_spark
#     result = list_to_pyspark_df(spark, obj_list)

#     fake_obj_list = 'fake_list'
#     fake_spark = fixture_setup_spark
#     fake_result = list_to_pyspark_df(fake_spark, fake_obj_list)

#     assert result.count() == 3
#     assert fake_result == None

# def valid_list_to_pyspark_df(a_list):
#     pass

# def test_get_script_prefix(target_prefix, script_file_name):
#     target_prefix = "consilience-export-manifest-files/2022"
#     expected_result = "consilience-export-manifest-files/2022/fake_name"
#     unexpected_result = "consilience-export-manifest-files/2022//fake_name"
#     result = get_script_prefix(target_prefix, "fake_name")
#     assert result == expected_result
#     assert result != unexpected_result

# def test_remove_script_from_df(pyspark_df, remove_value, column_name, fixture_second_df):
#     remove_value = "consilience-export-manifest-files/2022/s3_to_s3_validation.csv"
#     column_name = "Path"
#     result = remove_script_from_df(fixture_second_df, remove_value, column_name)

#     # fake_remove_value = "consilience-export-manifest-files/2022/s3_to_s3_validation.csv-foo"
#     # fake_column_name = "Path-foo"
#     # fake_result = remove_script_from_df(fixture_second_df, fake_remove_value, fake_column_name)
#     # assert fake_result.count() == 28
#     assert result.count() == 27

# def test_get_missing_objects(fixture_file_to_df_fixture, fixture_rename_bucket_df):
#     missing_df = get_missing_objects(fixture_file_to_df_fixture, fixture_rename_bucket_df,
#                                                 "Path", "bPath")
#     should_missing = ['consilience-export-manifest-files/2022/shouldbemissing1',
#                         'consilience-export-manifest-files/2022/shoudbemissing2',
#                         'consilience-export-manifest-files/2022/shouldbemissing3',
#                         'consilience-export-manifest-files/2022/3moreshouldbemissing']
#     missing_list = list(missing_df.select('Path').toPandas()['Path'])
#     # fake_missing_df = get_missing_objects(fixture_file_to_df_fixture, fixture_rename_bucket_df,
#     #                                             "Path_foo", "bPath_foo")
#     # fake_mssing_list = list(fake_missing_df.select('Path').toPandas()['Path'])
#     assert set(should_missing).issubset(set(missing_list)) == 1
#     # assert set(should_missing).issubset(set(fake_mssing_list)) == 0

# def test_get_df_count(fixture_second_df):
#     row_count = get_df_count(fixture_second_df)
#     # fake_row_count = get_df_count('fake_input')
#     assert row_count == 27
#     # assert fake_row_count == None

# def test_get_match_objects(fixture_file_to_df, fixture_rename_bucket_df):
#     match_df = get_match_objects(fixture_file_to_df, fixture_rename_bucket_df,
#                             "Path", "Size", "bPath", "bSize", "Date")
#     row_count = match_df.count()
#     # fake_match_df = get_match_objects(fixture_file_to_df, fixture_rename_bucket_df,
#     #                         "o", "p", "q", "r", "s")
#     assert row_count == 11
#     # assert fake_match_df == None

# def test_get_wrong_size_objects(fixture_get_match_objects):
#     wrong_size_df = get_wrong_size_objects(fixture_get_match_objects, "Size", "bSize")
#     row_count = wrong_size_df.count()
#     # fake_wrong_size_df = get_wrong_size_objects(fixture_get_match_objects, "o", "p")
#     assert row_count == 8
#     # assert fake_wrong_size_df == None



# # # Cannot test this function, because current IAM role cannot publish message to SNS.
# # def test_send_sns_to_subscriber(test_object, test_initial_boto3_client_fixture, fixture_get_current_denver_time):
# #     sns_client = test_initial_boto3_client_fixture
# #     sns_topic_arn = "arn:aws:sns:us-west-2:064047601590:dataquality_pytest"
# #     missing_message = "missing"
# #     wrong_size_message = "wrong size"
# #     current = fixture_get_current_denver_time
# #     target_prefix = "target_prefix"
# #     target_bucket = "target_bucket"
# #     response_sns = test_object.result_to_subscriber(missing_message, wrong_size_message,\
# #          current, target_prefix, target_bucket, sns_client, sns_topic_arn)
# #     assert response_sns != None


# print("Tests completed")