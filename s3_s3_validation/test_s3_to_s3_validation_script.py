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

# def test_get_file_location():
# 	pass
#     # From glue job instance, not able to test in sagemaker

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

@pytest.mark.test_prefix_to_list	
@pytest.mark.parametrize(
	["s3_bucket", "s3_prefix", "s3_resource"],
	[
		("s3-validation-demo", "test", "fixture_initialize_boto3_resource")
	]
)
def test_prefix_to_list_correct(s3_bucket, s3_prefix, s3_resource, request):
	"""
	Test function prefix_to_list with correct input:
		s3_bucket
		s3_prefix
		s3_resource
	Pass criteria:
		actual_result is equal to expected_result
	"""
	expected_result = ['test/', 'test/fake_file.csv', 'test/hello.py', 'test/s3_to_s3_validation.csv', 'test/s3_to_s3_validation_second.csv', 'test/spark_setup.py']
	s3_resource = s3_resource = request.getfixturevalue(s3_resource)
	actual_result = prefix_to_list(s3_bucket, s3_prefix, s3_resource)
	assert actual_result == expected_result
	
@pytest.mark.test_prefix_to_list	
@pytest.mark.parametrize(
	["s3_bucket", "s3_prefix", "s3_resource"],
	[
		("fake bucket", "test", "fixture_initialize_boto3_resource"),
		("s3-validation-demo", "fake_prefix", "fixture_initialize_boto3_resource"),
		("s3-validation-demo", "test", "fake_s3_resource"),
	]
)
def test_prefix_to_list_incorrect(s3_bucket, s3_prefix, s3_resource, request):
	"""
	Test function prefix_to_list with incorrect input:
		s3_bucket
		s3_prefix
		s3_resource
	Pass criteria:
		result is None
	"""
	if s3_resource == "fixture_initialize_boto3_resource":
		s3_resource = request.getfixturevalue(s3_resource)
	result = prefix_to_list(s3_bucket, s3_prefix, s3_resource)
	assert result is None

@pytest.mark.test_prefix_validation	
@pytest.mark.parametrize(
	["s3_prefix", "s3_prefix_list"],
	[
		("test", ["test/","test/child_folder"]),
		("test/", ["test/","test/child_folder"])
	]
)
def test_prefix_validation_correct(s3_prefix, s3_prefix_list):
	"""
	Test function prefix_validation with correct input:
		s3_prefix
		s3_prefix_list
	Pass criteria:
		result is s3_prefix
	"""
	result = prefix_validation(s3_prefix, s3_prefix_list)
	if s3_prefix[-1] != "/":
		s3_prefix += "/"	
	assert result == s3_prefix


@pytest.mark.test_prefix_validation	
@pytest.mark.parametrize(
	["s3_prefix", "s3_prefix_list"],
	[
		("test", ["other","test/child_folder"]),
		("test/", ["other/","test/child_folder"])
	]
)
def test_prefix_validation_incorrect(s3_prefix, s3_prefix_list):
	"""
	Test function prefix_validation with incorrect input:
		s3_prefix
		s3_prefix_list
	Pass criteria:
		result is None
	"""
	result = prefix_validation(s3_prefix, s3_prefix_list)
	assert result is None

# def test_get_file_location(trigger_s3_bucket, trigger_s3_path):
#     pass
#     # From glue job instance, not able to test in sagemaker

@pytest.mark.test_get_current_denver_time	
@pytest.mark.parametrize(
	["time_zone", "time_format"],
	[
		("US/Mountain", "%Y%m%d_%H%M%S_%Z_%z")
	]
)
def test_get_current_denver_time_correct(time_zone, time_format):
	"""
	Test function get_current_denver_time with correct input:
		time_zone
		time_format
	Pass criteria:
		result is current_denver_time
	"""
	current_denver_time = datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)
	assert get_current_denver_time(time_zone, time_format) == current_denver_time
	
@pytest.mark.test_get_current_denver_time	
@pytest.mark.parametrize(
	["time_zone", "time_format"],
	[
		("fake_timezone", "%Y%m%d_%H%M%S_%Z_%z")
	]
)
def test_get_current_denver_time_incorrect(time_zone, time_format):
	"""
	Test function get_current_denver_time with incorrect input:
		time_zone
		time_format
	Pass criteria:
		result is None
	"""
	assert get_current_denver_time(time_zone, time_format) == 'cannot_get_timestamp'
	
@pytest.mark.test_generate_result_location	
@pytest.mark.parametrize(
	["target_bucket", "target_prefix"],
	[
		("s3-validation-demo", "consilience-export-manifest-files/2022")
	]
)
def test_generate_result_location_correct(target_bucket, target_prefix):
	"""
	Test function generate_result_location with correct input:
		target_bucket
		target_prefix
	Pass criteria:
		actual_result_target_prefix equals to expected_result_target_prefix
	"""
	expected_result_target_prefix = f"{target_bucket}/s3_to_s3_validation_result_{target_bucket}_consilience-export-manifest-files_2022/"
	actual_result_target_prefix = generate_result_location(target_bucket, target_prefix)
	assert actual_result_target_prefix == expected_result_target_prefix

@pytest.mark.test_generate_result_location	
@pytest.mark.parametrize(
	["target_bucket", "target_prefix"],
	[
		(["s3-validation-demo"], "consilience-export-manifest-files/2022"),
		("s3-validation-demo", ["consilience-export-manifest-files/2022"])
	]
)
def test_generate_result_location_incorrect(target_bucket, target_prefix):
	"""
	Test function generate_result_location with incorrect input:
		target_bucket
		target_prefix
	Pass criteria:
		actual_result_target_prefix is None
	"""
	actual_result_target_prefix = generate_result_location(target_bucket, target_prefix)
	assert actual_result_target_prefix is None

# def test_setup_spark():
# 	pass
# 	# From glue job instance, not able to test in sagemaker

@pytest.mark.test_initialize_boto3_client	
@pytest.mark.parametrize(
	"aws_service",
	[
		"s3",
		"sns"
	]
)
def test_initialize_boto3_client_correct(aws_service):
	"""
	Test function initialize_boto3_client with correct input:
		aws_service
	Pass criteria:
		result.__class__.__name__ equals to aws_service.upper()
	"""
	result = initialize_boto3_client(aws_service)
	assert result.__class__.__name__ == aws_service.upper()
	
@pytest.mark.test_initialize_boto3_client	
@pytest.mark.parametrize(
	"aws_service",
	[
		"ooo",
		"ppp",
		["opop"]
	]
)
def test_initialize_boto3_client_incorrect(aws_service):
	"""
	Test function initialize_boto3_client with incorrect input:
		aws_service
	Pass criteria:
		result is None
	"""
	result = initialize_boto3_client(aws_service)
	assert result is None

@pytest.mark.test_initialize_boto3_resource	
@pytest.mark.parametrize(
	"aws_service",
	[
		"s3",
		"sns"
	]
)
def test_initialize_boto3_resource_correct(aws_service):
	"""
	Test function initialize_boto3_resource with correct input:
		aws_service
	Pass criteria:
		result.__class__.__name__ equals to aws_service.ServiceResource
	"""
	result = initialize_boto3_resource(aws_service)
	assert result.__class__.__name__ == aws_service+'.ServiceResource'

@pytest.mark.test_initialize_boto3_resource	
@pytest.mark.parametrize(
	"aws_service",
	[
		"ooo",
		"ppp",
		["opop"]
	]
)
def test_initialize_boto3_resource_incorrect(aws_service):
	"""
	Test function initialize_boto3_resource with incorrect input:
		aws_service
	Pass criteria:
		result is None
	"""
	result = initialize_boto3_resource(aws_service)
	assert result is None

@pytest.mark.test_get_sns_name	
@pytest.mark.parametrize(
	"target_bucket",
	[
		"example_bucket",
		"test_bucket"
	]
)
def test_get_sns_name_correct(target_bucket):
	"""
	Test function get_sns_name with correct input:
		target_bucket
	Pass criteria:
		result equals to target_bucket.replace('.','')
	"""
	result = get_sns_name(target_bucket)
	assert result == target_bucket.replace('.','')

@pytest.mark.test_get_sns_name	
@pytest.mark.parametrize(
	"target_bucket",
	[
		["example_bucket"],
		{"test_bucket"}
	]
)
def test_get_sns_name_incorrect(target_bucket):
	"""
	Test function get_sns_name with incorrect input:
		target_bucket
	Pass criteria:
		result is None
	"""
	result = get_sns_name(target_bucket)
	assert result is None

@pytest.mark.test_get_sns_name	
@pytest.mark.parametrize(
	["sns_client","sns_name"],
	[
		("fixture_initialize_boto3_client", "s3-validation-demo")
	]
)
def test_get_sns_arn_correct(sns_client, sns_name, request):
	"""
	Test function get_sns_name with correct input:
		sns_client
		sns_name
	Pass criteria:
		result equals to expected_response
	"""
	sns_client = request.getfixturevalue(sns_client)
	expected_response = "arn:aws:sns:us-west-2:064047601590:s3-validation-demo"
	result = get_sns_arn(sns_client, sns_name)
	assert result == expected_response

@pytest.mark.test_get_sns_name	
@pytest.mark.parametrize(
	["sns_client","sns_name"],
	[
		("fake_client", "s3-validation-demo"),
		("fixture_initialize_boto3_client", "fake_bucket")
	]
)
def test_get_sns_arn_incorrect(sns_client, sns_name, request):
	"""
	Test function get_sns_name with incorrect input:
		sns_client
		sns_name
	Pass criteria:
		result is None
	"""
	if sns_client == "fixture_initialize_boto3_client":
		sns_client = request.getfixturevalue(sns_client)
	result = get_sns_arn(sns_client, sns_name)
	assert result is None

# def test_sns_send(sns_client, sns_topic_arn, message, subject):
# 	pass
#     # From SageMaker will get botocore.errorfactory.AuthorizationErrorException error

@pytest.mark.test_rename_columns	
@pytest.mark.parametrize(
	["pyspark_df","renames"],
	[
		("fixture_second_df", {"size": "b_size","path": "b_path"})
	]
)
def test_rename_columns_correct(pyspark_df, renames, request):
	"""
	Test function get_sns_name with correct input:
		pyspark_df
		renames
	Pass criteria:
		result_set equals to expected_names_set
	"""
	pyspark_df = request.getfixturevalue(pyspark_df)
	renamed_df = rename_columns(pyspark_df, **renames)
	renamed_df_columns = renamed_df.columns
	result_set = set(renamed_df_columns)
	expected_names_set = {"b_path", "b_size", "date"}
	assert result_set == expected_names_set


@pytest.mark.test_rename_columns	
@pytest.mark.parametrize(
	["pyspark_df","renames"],
	[
		("fake_df", {"size": "b_size","path": "b_path"})
	]
)
def test_rename_columns_incorrect(pyspark_df, renames, request):
	"""
	Test function get_sns_name with incorrect input:
		pyspark_df
		renames
	Pass criteria:
		renamed_df is None
	"""
	renamed_df = rename_columns(pyspark_df, **renames)
	assert renamed_df is None

@pytest.mark.test_file_to_pyspark_df	
@pytest.mark.parametrize(
	["spark","file_bucket","file_prefix","schema"],
	[
		("fixture_setup_spark","s3-validation-demo","test","fixture_schema")
	]
)
def test_file_to_pyspark_df_correct(spark, file_bucket, file_prefix, schema, request):
	"""
	Test function get_sns_name with correct input:
		spark
		file_bucket
		file_prefix
		schema
	Pass criteria:
		type of result is DataFrame
	"""
	spark = request.getfixturevalue(spark)
	schema = request.getfixturevalue(schema)
	result = file_to_pyspark_df(spark, file_bucket, file_prefix, schema)
	assert isinstance(result, DataFrame)
    
@pytest.mark.test_file_to_pyspark_df	
@pytest.mark.parametrize(
	["spark","file_bucket","file_prefix","schema"],
	[
		("fake_spark","s3-validation-demo","test","fixture_schema"),
		("fixture_setup_spark","fake_bucket","test","fixture_schema"),
		("fixture_setup_spark","s3-validation-demo","fake_prefix","fixture_schema"),
		("fixture_setup_spark","s3-validation-demo","test","fake_schema"),
	]
)
def test_file_to_pyspark_df_incorrect(spark, file_bucket, file_prefix, schema, request):
	"""
	Test function get_sns_name with incorrect input:
		spark
		file_bucket
		file_prefix
		schema
	Pass criteria:
		result is None
	"""
	if spark == "fixture_setup_spark":
		spark = request.getfixturevalue(spark)
	if schema == "fixture_schema":
		schema = request.getfixturevalue(schema)
	result = file_to_pyspark_df(spark, file_bucket, file_prefix, schema)
	assert result is None

@pytest.mark.test_s3_obj_to_list	
@pytest.mark.parametrize(
	["s3_resource","target_bucket","target_prefix","time_format"],
	[
		("fixture_initialize_boto3_resource","s3-validation-demo","test","%Y%m%d_%H%M%S_%Z_%z")
	]
)
def test_s3_obj_to_list_correct(s3_resource, target_bucket, target_prefix, time_format, request):
	"""
	Test function get_sns_name with correct input:
		s3_resource
		target_bucket
		target_prefix
		time_format
	Pass criteria:
		result_list equals to expected_list
	"""
	s3_resource = request.getfixturevalue(s3_resource)
	expected_list = [{'path': 'test/', 'size': 0, 'date': '20230116_212937_UTC_+0000'}, {'path': 'test/fake_file.csv', 'size': 1150, 'date': '20230126_213828_UTC_+0000'}, {'path': 'test/hello.py', 'size': 45, 'date': '20230116_230554_UTC_+0000'}, {'path': 'test/s3_to_s3_validation.csv', 'size': 1135, 'date': '20230216_000931_UTC_+0000'}, {'path': 'test/s3_to_s3_validation_second.csv', 'size': 3805, 'date': '20230222_210744_UTC_+0000'}, {'path': 'test/spark_setup.py', 'size': 757, 'date': '20230116_213401_UTC_+0000'}]
	result_list = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)
	assert result_list == expected_list

@pytest.mark.test_s3_obj_to_list	
@pytest.mark.parametrize(
	["s3_resource","target_bucket","target_prefix","time_format"],
	[
		("fake_resource",
		 "s3-validation-demo","test","%Y%m%d_%H%M%S_%Z_%z"),
		("fixture_initialize_boto3_resource",
		 "fake_bucket","test","%Y%m%d_%H%M%S_%Z_%z"),
		("fixture_initialize_boto3_resource",
		 "s3-validation-demo","fake_prefix","%Y%m%d_%H%M%S_%Z_%z"),
		("fixture_initialize_boto3_resource",
		 "s3-validation-demo","test",["fake_time_format"])
	]
)
def test_s3_obj_to_list_incorrect(s3_resource, target_bucket, target_prefix, time_format, request):
	"""
	Test function get_sns_name with incorrect input:
		s3_resource
		target_bucket
		target_prefix
		time_format
	Pass criteria:
		result is None
	"""
	if s3_resource == "fixture_initialize_boto3_resource":
		s3_resource = request.getfixturevalue(s3_resource)
	result = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)
	assert result is None

@pytest.mark.test_list_to_pyspark_df	
@pytest.mark.parametrize(
	["spark","obj_list"],
	[
		("fixture_setup_spark",[{'path': 'path1', 'size': 0, 'date': '20230116_212937_UTC_+0000'}, {'path': 'path2', 'size': 1, 'date': '20230126_213828_UTC_+0000'}, {'path': 'path3', 'size': 2, 'date': '20230116_230554_UTC_+0000'}])
	]
)
def test_list_to_pyspark_df_correct(spark, obj_list, request):
	"""
	Test function get_sns_name with correct input:
		spark
		obj_list
	Pass criteria:
		result_df equals to expected_df on schema, columns and values in columns
	"""
	spark = request.getfixturevalue(spark)
	result_df = list_to_pyspark_df(spark, obj_list)
	expected_columns = ['date', 'path', 'size']
	expected_data = [['20230116_212937_UTC_+0000', 'path1', 0],
					 ['20230126_213828_UTC_+0000', 'path2', 1],
					 ['20230116_230554_UTC_+0000', 'path3', 2]]
	expected_df = spark.createDataFrame(expected_data, expected_columns)
	assert result_df.schema == expected_df.schema
	assert result_df.columns == expected_df.columns
	assert result_df.select('path').rdd.flatMap(lambda x: x).collect() ==\
		expected_df.select('path').rdd.flatMap(lambda x: x).collect()
	assert result_df.select('size').rdd.flatMap(lambda x: x).collect() ==\
		expected_df.select('size').rdd.flatMap(lambda x: x).collect()	
	assert result_df.select('date').rdd.flatMap(lambda x: x).collect() ==\
		expected_df.select('date').rdd.flatMap(lambda x: x).collect()	

@pytest.mark.test_list_to_pyspark_df	
@pytest.mark.parametrize(
	["spark","obj_list"],
	[
		("fake_spark",[{'path': 'path1', 'size': 0, 'date': '20230116_212937_UTC_+0000'}, {'path': 'path2', 'size': 1, 'date': '20230126_213828_UTC_+0000'}, {'path': 'path3', 'size': 2, 'date': '20230116_230554_UTC_+0000'}]),
		("fixture_setup_spark",'fake_list'),
		("fixture_setup_spark",['fake_list', 'fake_list_1'])
	]
)
def test_list_to_pyspark_df_incorrect(spark, obj_list, request):
	"""
	Test function get_sns_name with incorrect input:
		spark
		obj_list
	Pass criteria:
		result is None
	"""
	if spark == "fixture_setup_spark":
		spark = request.getfixturevalue(spark)
	result = list_to_pyspark_df(spark, obj_list)
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