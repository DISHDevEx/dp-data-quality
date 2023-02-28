"""
Pytest file to test s3_to_s3_validation_script.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""
import sys
import json
import time
import boto3
from boto3.exceptions import ResourceNotExistsError
import botocore
from botocore.client import ClientError
from botocore.exceptions import ConnectionClosedError, ParamValidationError, UnknownServiceError
import pytz
from pytz.exceptions import UnknownTimeZoneError
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StringType, LongType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
import pytest
from pytest import FixtureRequest as fr
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from s3_to_s3_validation_script import *

def fixture_setup_spark():
    packages = (",".join(["io.delta:delta-core_2.12:2.2.0","org.apache.hadoop:hadoop-aws:3.3.4"]))
    spark_driver_memory = '8g'
    spark_executor_memory = '8g'
    spark_memory_offHeap_enabled = True
    spark_memory_offHeap_size =  '10g'
    spark_driver_maxResultSize = '2g'
    print('packages: '+packages)

    # Instantiate Spark via builder
    # Note: we use the `ContainerCredentialsProvider` to give us access to underlying IAM role permissions

    spark = (SparkSession
        .builder
        .appName("PySparkApp") 
        .config("spark.jars.packages", packages) 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .config("fs.s3a.aws.credentials.provider",'com.amazonaws.auth.ContainerCredentialsProvider') 
        .config("spark.driver.memory", spark_driver_memory)
        .config("spark.executor.memory", spark_executor_memory)
        .config("spark.memory.offHeap.enabled", spark_memory_offHeap_enabled)
        .config("spark.memory.offHeap.size", spark_memory_offHeap_size)
        .config("spark.sql.broadcastTimeout", "36000")

    ).getOrCreate()
    return spark


# s3_resource.__class__.__name__ != "s3.ServiceResource"
# sns_client.__class__.__name__ != "SNS"

# def test_send_sns_to_subscriber(target_bucket, target_prefix, current,
#     sns_client, sns_topic_arn, missing_message, wrong_size_message, request):
# 	pass
    # From SageMaker will get botocore.errorfactory.AuthorizationErrorException error


# Cannot test this function, because current IAM role cannot publish message to SNS.
@pytest.mark.test_send_sns_to_subscriber
@pytest.mark.parametrize(
	["target_bucket", "target_prefix, current",
    "fixture_initialize_boto3_client", 
	"sns_topic_arn",
	 "missing_message", "wrong_size_message", "request"],
	[
		("target_bucket", "target_prefix",
			"fixture_get_current_denver_time",
			"fixture_file_to_df",
			"test_object"
		)
	]
)
def test_send_sns_to_subscriber_correct(target_bucket, target_prefix, current,
    sns_client, sns_topic_arn, missing_message, wrong_size_message, request):
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
    sns_client = test_initial_boto3_client_fixture
    sns_topic_arn = "arn:aws:sns:us-west-2:064047601590:dataquality_pytest"
    missing_message = "missing"
    wrong_size_message = "wrong size"
    current = fixture_get_current_denver_time
    target_prefix = "target_prefix"
    target_bucket = "target_bucket"
    response_sns = test_object.result_to_subscriber(missing_message, wrong_size_message,\
         current, target_prefix, target_bucket, sns_client, sns_topic_arn)
    assert response_sns != None





# @pytest.mark.test_save_result_to_s3
# @pytest.mark.parametrize(
# 	["result_location", "current", "pyspark_df", "obj_name"],
# 	[
# 		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
# 			"fixture_get_current_denver_time",
# 			"fixture_file_to_df",
# 			"test_object"
# 		)
# 	]
# )
# def test_save_result_to_s3_correct(result_location, current, pyspark_df,
# 								   obj_name, request, fixture_initialize_boto3_resource):
# 	"""
# 	Test function save_result_to_s3 with correct input:
# 		result_location
# 		current
# 		pyspark_df_non_empty
# 		obj_name
# 	Pass criteria:
# 		result is the expected string
# 		Saved object is in the s3 bucket at the expected location
# 	"""
# 	current = request.getfixturevalue(current)
# 	pyspark_df_non_empty = request.getfixturevalue(pyspark_df)
# 	result = save_result_to_s3(result_location, current,
# 									pyspark_df_non_empty, obj_name)
# 	s3_resource = fixture_initialize_boto3_resource
# 	s3_bucket_objects_collection = s3_resource.Bucket('s3-validation-demo') \
# .objects.filter(Prefix='pytest_result/s3_to_s3_validation_pytest_result')
# 	file_name = f'pytest_result/s3_to_s3_validation_pytest_result/{obj_name}_{current}.csv'
# 	parent_folder_list = []
# 	for obj in s3_bucket_objects_collection:
# 		parent_folder_list.append(obj.key)
# 	if file_name in parent_folder_list:
# 		obj_exist = True
# 	else:
# 		obj_exist = False
# 	assert result == f"Saved at {result_location}{obj_name}_{current}.csv."
# 	assert obj_exist is True

# @pytest.mark.test_save_result_to_s3
# @pytest.mark.parametrize(
# 	["result_location", "current", "pyspark_df", "obj_name"],
# 	[
# 		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
# 			"fixture_get_current_denver_time",
# 			"fixture_empty_dataframe",
# 			"test_object"
# 		)
# 	]
# )
# def test_save_result_to_s3_no_object(result_location, current, pyspark_df, obj_name, request):
# 	"""
# 	Test function save_result_to_s3 with correct input but empty dataframe:
# 		result_location
# 		current
# 		pyspark_df_empty
# 		obj_name
# 	Pass criteria:
# 		result is the expected string
# 	"""
# 	current = request.getfixturevalue(current)
# 	pyspark_df_empty = request.getfixturevalue(pyspark_df)
# 	result = save_result_to_s3(result_location, current,
# 									pyspark_df_empty, obj_name)
# 	assert result == f"No {obj_name} item found."

# @pytest.mark.test_save_result_to_s3
# @pytest.mark.parametrize(
# 	["result_location", "current", "pyspark_df", "obj_name"],
# 	[
# 		("this is a fake s3 bucket",
# 			"fixture_get_current_denver_time",
# 			"fixture_file_to_df",
# 			"test_object"
# 		),
# 		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
# 			["this is a fake time"],
# 			"fixture_file_to_df",
# 			"test_object"
# 		),
# 		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
# 			"fixture_get_current_denver_time",
# 			"this is a fake pyspark dataframe",
# 			"test_object"
# 		),
# 		("s3-validation-demo/pytest_result/s3_to_s3_validation_pytest_result/",
# 			"fixture_get_current_denver_time",
# 			"fixture_file_to_df",
# 			["this is a fake object name"]
# 		)
# 	]
# )
# def test_save_result_to_s3_incorrect(result_location, current, pyspark_df, obj_name, request):
# 	"""
# 	Test function save_result_to_s3 with incorrect input:
# 		result_location
# 		current
# 		pyspark_df_empty
# 		obj_name
# 	Pass criteria:
# 		result is None
# 	"""
# 	if current == "fixture_get_current_denver_time":
# 		current = request.getfixturevalue(current)
# 	if pyspark_df == "fixture_file_to_df":
# 		pyspark_df = request.getfixturevalue(pyspark_df)
# 	result = save_result_to_s3(result_location, current,
# 									pyspark_df, obj_name)
# 	assert result is None


# @pytest.mark.test_get_wrong_size_objects	
# @pytest.mark.parametrize(
# 	["pyspark_df","df_1_column","df_2_column"],
# 	[
# 		("fixture_get_match_objects","size","b_size")
# 	]
# )
# def test_get_wrong_size_objects_correct(pyspark_df, df_1_column, df_2_column, request, fixture_setup_spark):
# 	"""
# 	Test function get_wrong_size_objects with correct input:
# 		pyspark_df
# 		df_1_column
# 		df_2_column
# 	Pass criteria:
# 		result_df equals to expected_df on columns and values in columns
# 	"""
# 	expected_columns = ['size', 'path', 'b_size', 'date', 'b_path']
# 	expected_data = [[53, 'consilience-export-manifest-files/2022/2022-11-09T20-00-00+0000.csv',
# 					  12254880, '2022-12-02_20-30-08_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/2022-11-09T20-00-00+0000.csv'],
# 					 [876, 'consilience-export-manifest-files/2022/CDR_GI_202210200016.csv',
# 					  1003151, '2023-01-16_22-47-39_UTC_+0000',
# 					  'consilience-export-manifest-files/2022/CDR_GI_202210200016.csv'],
# 					 [49, 'consilience-export-manifest-files/2022/allObjects1005.csv',
# 					  124668866, '2022-11-10_16-07-04_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/allObjects1005.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/duplicate-sites.csv',
# 					  69857, '2022-11-10_16-32-39_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/duplicate-sites.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/missing_2022-10-04_10-16-14_MDT_-0600.csv',
# 					  2723842, '2022-11-10_16-24-57_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/missing_2022-10-04_10-16-14_MDT_-0600.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/missing_2022-12-21_23-26-29_MST_-0700.csv',
# 					  12444295, '2022-12-22_06-33-44_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/missing_2022-12-21_23-26-29_MST_-0700.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+0000.csv',
# 					  49, '2022-12-20_17-22-21_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+0000.csv'],
# 					 [2, 'consilience-export-manifest-files/2022/transfer.csv',
# 					  49, '2022-11-10_16-09-47_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/transfer.csv']
# 					]
# 	spark = fixture_setup_spark
# 	expected_df = spark.createDataFrame(expected_data, expected_columns)
# 	pyspark_df = request.getfixturevalue(pyspark_df)
# 	result_df = get_wrong_size_objects(pyspark_df, df_1_column, df_2_column)
# 	assert set(result_df.schema) == set(expected_df.schema)
# 	assert set(result_df.columns) == set(expected_df.columns)
# 	assert result_df.select('path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('size').rdd.flatMap(lambda x: x).collect()	
# 	assert result_df.select('date').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('date').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('b_path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('b_path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('b_size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('b_size').rdd.flatMap(lambda x: x).collect()
	
# @pytest.mark.test_get_wrong_size_objects	
# @pytest.mark.parametrize(
# 	["pyspark_df","df_1_column","df_2_column"],
# 	[
# 		("fake_df","size","b_size"),
# 		("fixture_get_match_objects","fake_size","b_size"),
# 		("fixture_get_match_objects","size","fake_b_size")
# 	]
# )
# def test_get_wrong_size_objects_incorrect(pyspark_df, df_1_column, df_2_column, request):
# 	"""
# 	Test function get_wrong_size_objects with incorrect input:
# 		pyspark_df
# 		df_1_column
# 		df_2_column
# 	Pass criteria:
# 		result is None
# 	"""
# 	if pyspark_df == "fixture_get_match_objects":
# 		pyspark_df = request.getfixturevalue(pyspark_df)
# 	result = get_wrong_size_objects(pyspark_df, df_1_column, df_2_column)
# 	assert result is None

# @pytest.mark.test_get_match_objects	
# @pytest.mark.parametrize(
# 	["df_1","df_2","df_1_column","df_2_column", "columns_dict"],
# 	[
# 		("fixture_file_to_df","fixture_second_df_renamed","path","b_path",
# 		 {"df_1": {"path", "size"}, "df_2": {"b_path", "b_size", "date"}})
# 	]
# )
# def test_get_match_objects_correct(df_1, df_2, df_1_column, df_2_column,
# 								   columns_dict, request, fixture_setup_spark):
# 	"""
# 	Test function get_match_objects with correct input:
# 		df_1
# 		df_2
# 		df_1_column
# 		df_2_column
# 		columns_dict
# 	Pass criteria:
# 		result_df equals to expected_df on schema, columns and values in columns
# 	"""
# 	expected_columns = ['size', 'path', 'b_size', 'date', 'b_path']
# 	expected_data = [[49,'consilience-export-manifest-files/2022/000.csv',
# 					  49, '2022-11-10_16-34-51_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/000.csv'],
# 					 [53, 'consilience-export-manifest-files/2022/2022-11-09T20-00-00+0000.csv',
# 					  12254880, '2022-12-02_20-30-08_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/2022-11-09T20-00-00+0000.csv'],
# 					 [876, 'consilience-export-manifest-files/2022/CDR_GI_202210200016.csv',
# 					  1003151, '2023-01-16_22-47-39_UTC_+0000',
# 					  'consilience-export-manifest-files/2022/CDR_GI_202210200016.csv'],
# 					 [49, 'consilience-export-manifest-files/2022/allObjects1005.csv',
# 					  124668866, '2022-11-10_16-07-04_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/allObjects1005.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/duplicate-sites.csv',
# 					  69857, '2022-11-10_16-32-39_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/duplicate-sites.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/missing_2022-10-04_10-16-14_MDT_-0600.csv',
# 					  2723842, '2022-11-10_16-24-57_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/missing_2022-10-04_10-16-14_MDT_-0600.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/missing_2022-12-21_23-26-29_MST_-0700.csv',
# 					  12444295, '2022-12-22_06-33-44_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/missing_2022-12-21_23-26-29_MST_-0700.csv'],
# 					 [456, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+0000.csv',
# 					  49, '2022-12-20_17-22-21_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+0000.csv'],
# 					 [49, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+~!@#$%^&*()=.,0000.csv',
# 					  49, '2022-12-20_18-35-17_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+~!@#$%^&*()=.,0000.csv'],
# 					 [2449237854, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-27T18-00-00+00001.csv',
# 					  2449237854, '2022-11-09_23-16-50_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-27T18-00-00+00001.csv'],
# 					 [2, 'consilience-export-manifest-files/2022/transfer.csv',
# 					  49, '2022-11-10_16-09-47_UTC_+0000',
# 					 'consilience-export-manifest-files/2022/transfer.csv']
# 					]
# 	spark = fixture_setup_spark
# 	expected_df = spark.createDataFrame(expected_data, expected_columns)
# 	df_1 = request.getfixturevalue(df_1)
# 	df_2 = request.getfixturevalue(df_2)
# 	result_df = get_match_objects(df_1, df_2, df_1_column, df_2_column, columns_dict)
# 	assert set(result_df.schema) == set(expected_df.schema)
# 	assert set(result_df.columns) == set(expected_df.columns)
# 	assert result_df.select('path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('size').rdd.flatMap(lambda x: x).collect()	
# 	assert result_df.select('date').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('date').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('b_path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('b_path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('b_size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('b_size').rdd.flatMap(lambda x: x).collect()

# @pytest.mark.test_get_match_objects	
# @pytest.mark.parametrize(
# 	["df_1","df_2","df_1_column","df_2_column", "columns_dict"],
# 	[
# 		("fake_df","fixture_second_df_renamed","path","b_path",
# 		 {"df_1": {"path", "size"}, "df_2": {"b_path", "b_size", "date"}}),
# 		("fixture_file_to_df","fake_second_df","path","b_path",
# 		 {"df_1": {"path", "size"}, "df_2": {"b_path", "b_size", "date"}}),
# 		("fixture_file_to_df","fixture_second_df_renamed","fake_path","b_path",
# 		 {"df_1": {"path", "size"}, "df_2": {"b_path", "b_size", "date"}}),
# 		("fixture_file_to_df","fixture_second_df_renamed","path","fake_b_path",
# 		 {"df_1": {"path", "size"}, "df_2": {"b_path", "b_size", "date"}}),
# 		("fixture_file_to_df","fixture_second_df_renamed","path","b_path",
# 		 {"fake_df_1": {"path", "size"}, "df_2": {"b_path", "b_size", "date"}}),
# 		("fixture_file_to_df","fixture_second_df_renamed","path","b_path",
# 		 "fake_dict")
# 	]
# )
# def test_get_match_objects_incorrect(df_1, df_2, df_1_column, df_2_column,
# 								   columns_dict, request):
# 	"""
# 	Test function get_match_objects with incorrect input:
# 		df_1
# 		df_2
# 		df_1_column
# 		df_2_column
# 		columns_dict
# 	Pass criteria:
# 		result is None
# 	"""
# 	if df_1 == "fixture_file_to_df":
# 		df_1 = request.getfixturevalue(df_1)
# 	if df_2 == "fixture_second_df_renamed":
# 		df_2 = request.getfixturevalue(df_2)
# 	result = get_match_objects(df_1, df_2, df_1_column, df_2_column, columns_dict)
# 	assert result is None


# @pytest.mark.test_get_df_count	
# @pytest.mark.parametrize(
# 	"pyspark_df",
# 	[
# 		"fixture_file_to_df"
# 	]
# )
# def test_get_df_count_correct(pyspark_df, request):
# 	"""
# 	Test function get_df_count with correct input:
# 		pyspark_df
# 	Pass criteria:
# 		result_count equals to expected_count
# 	"""
# 	pyspark_df = request.getfixturevalue(pyspark_df)
# 	expected_count = 15
# 	result_count = get_df_count(pyspark_df)
# 	assert result_count == expected_count

# @pytest.mark.test_get_df_count	
# @pytest.mark.parametrize(
# 	"pyspark_df",
# 	[
# 		"fake_df",
# 		["fake_df"]
# 	]
# )
# def test_get_df_count_incorrect(pyspark_df):
# 	"""
# 	Test function get_df_count with incorrect input:
# 		pyspark_df
# 	Pass criteria:
# 		result is None
# 	"""
# 	result = get_df_count(pyspark_df)
# 	assert result is None





# @pytest.mark.test_get_missing_objects	
# @pytest.mark.parametrize(
# 	["df_1","df_2","df_1_column","df_2_column"],
# 	[
# 		("fixture_file_to_df","fixture_second_df_renamed","path","b_path")
# 	]
# )
# def test_get_missing_objects_correct(df_1, df_2, df_1_column, df_2_column, request, fixture_setup_spark):
# 	"""
# 	Test function get_missing_objects with correct input:
# 		df_1
# 		df_2
# 		df_1_column
# 		df_2_column
# 	Pass criteria:
# 		result_df equals to expected_df on schema, columns and values in columns
# 	"""
# 	spark = fixture_setup_spark
# 	expected_columns = ['id', 'path', 'size']
# 	expected_data = [[3, 'consilience-export-manifest-files/2022/3moreshouldbemissing', 456],
# 					 [11, 'consilience-export-manifest-files/2022/shouldbemissing1', 435],
# 					 [12, 'consilience-export-manifest-files/2022/shoudbemissing2', 453],
# 					 [13, 'consilience-export-manifest-files/2022/shouldbemissing3', 354]
# 					]
# 	expected_df = spark.createDataFrame(expected_data, expected_columns)
# 	df_1 = request.getfixturevalue(df_1)
# 	df_2 = request.getfixturevalue(df_2)
# 	result_df = get_missing_objects(df_1, df_2, df_1_column, df_2_column)
# 	assert result_df.schema == expected_df.schema
# 	assert result_df.columns == expected_df.columns
# 	assert result_df.select('path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('size').rdd.flatMap(lambda x: x).collect()	
# 	assert result_df.select('id').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('id').rdd.flatMap(lambda x: x).collect()

# @pytest.mark.test_get_missing_objects	
# @pytest.mark.parametrize(
# 	["df_1","df_2","df_1_column","df_2_column"],
# 	[
# 		("fake_df","fixture_second_df_renamed","path","b_path"),
# 		("fixture_file_to_df","fake_second_df_renamed","path","b_path"),
# 		("fixture_file_to_df","fixture_second_df_renamed","fake_path","b_path"),
# 		("fixture_file_to_df","fixture_second_df_renamed","path","_fake_b_path")
# 	]
# )
# def test_get_missing_objects_incorrect(df_1, df_2, df_1_column, df_2_column, request):
# 	"""
# 	Test function get_missing_objects with incorrect input:
# 		df_1
# 		df_2
# 		df_1_column
# 		df_2_column
# 	Pass criteria:
# 		result is None
# 	"""
# 	if df_1 == "fixture_file_to_df":
# 		df_1 = request.getfixturevalue(df_1)
# 	if df_2 == "fixture_second_df_renamed":
# 		df_2 = request.getfixturevalue(df_2)
# 	result = get_missing_objects(df_1, df_2, df_1_column, df_2_column)
# 	assert result is None



# @pytest.mark.test_remove_script_from_df	
# @pytest.mark.parametrize(
# 	["pyspark_df","remove_value","column_name"],
# 	[
# 		("fixture_file_to_df",
# 		 "consilience-export-manifest-files/2022/duplicate-sites.csv","path")
# 	]
# )
# def test_remove_script_from_df_correct(pyspark_df, remove_value, column_name, request, fixture_setup_spark):
# 	"""
# 	Test function remove_script_from_df with correct input:
# 		pyspark_df
# 		remove_value
# 		column_name
# 	Pass criteria:
# 		result_df equals to expected_df on schema, columns and values in columns
# 	"""
# 	spark = fixture_setup_spark
# 	expected_columns = ['id', 'path', 'size']
# 	expected_data = [[1, 'consilience-export-manifest-files/2022/000.csv', 49],
# 					 [2, 'consilience-export-manifest-files/2022/2022-11-09T20-00-00+0000.csv', 53],
# 					 [3, 'consilience-export-manifest-files/2022/3moreshouldbemissing', 456],
# 					 [4, 'consilience-export-manifest-files/2022/CDR_GI_202210200016.csv', 876],
# 					 [6, 'consilience-export-manifest-files/2022/missing_2022-10-04_10-16-14_MDT_-0600.csv', 456],
# 					 [7, 'consilience-export-manifest-files/2022/missing_2022-12-21_23-26-29_MST_-0700.csv', 456],
# 					 [8, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+0000.csv', 456],
# 					 [9, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+~!@#$%^&*()=.,0000.csv', 49],
# 					 [10, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-27T18-00-00+00001.csv', 2449237854],
# 					 [11, 'consilience-export-manifest-files/2022/shouldbemissing1', 435],
# 					 [12, 'consilience-export-manifest-files/2022/shoudbemissing2', 453],
# 					 [13, 'consilience-export-manifest-files/2022/shouldbemissing3', 354],
# 					 [14, 'consilience-export-manifest-files/2022/allObjects1005.csv', 49],
# 					 [15, 'consilience-export-manifest-files/2022/transfer.csv', 2]
# 					]
# 	expected_df = spark.createDataFrame(expected_data, expected_columns)
# 	pyspark_df = request.getfixturevalue(pyspark_df)
# 	result_df = remove_script_from_df(pyspark_df, remove_value, column_name)
# 	assert result_df.schema == expected_df.schema
# 	assert result_df.columns == expected_df.columns
# 	assert result_df.select('path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('size').rdd.flatMap(lambda x: x).collect()	
# 	assert result_df.select('id').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('id').rdd.flatMap(lambda x: x).collect()	

# @pytest.mark.test_remove_script_from_df	
# @pytest.mark.parametrize(
# 	["pyspark_df","remove_value","column_name"],
# 	[
# 		("fake_df",
# 		 "consilience-export-manifest-files/2022/duplicate-sites.csv","path")
# 	]
# )
# def test_remove_script_from_df_incorrect_df(pyspark_df, remove_value, column_name, request):
# 	"""
# 	Test function remove_script_from_df with incorrect_df input:
# 		pyspark_df
# 		remove_value
# 		column_name
# 	Pass criteria:
# 		result is None
# 	"""
# 	if pyspark_df == "fixture_file_to_df":
# 		pyspark_df = request.getfixturevalue(pyspark_df)
# 	result = remove_script_from_df(pyspark_df, remove_value, column_name)
# 	assert result is None

# @pytest.mark.test_remove_script_from_df	
# @pytest.mark.parametrize(
# 	["pyspark_df","remove_value","column_name"],
# 	[
# 		("fixture_file_to_df",["fake_value"],"path"),
# 		("fixture_file_to_df",
# 		 "consilience-export-manifest-files/2022/duplicate-sites.csv",["fake_path"])
# 	]
# )
# def test_remove_script_from_df_incorrect_filter(pyspark_df, remove_value, column_name, request):
# 	"""
# 	Test function remove_script_from_df with incorrect_filter input:
# 		pyspark_df
# 		remove_value
# 		column_name
# 	Pass criteria:
# 		result_df equals to pyspark_df
# 	"""
# 	if pyspark_df == "fixture_file_to_df":
# 		pyspark_df = request.getfixturevalue(pyspark_df)
# 	result_df = remove_script_from_df(pyspark_df, remove_value, column_name)
# 	assert result_df == pyspark_df


# @pytest.mark.test_get_script_prefix	
# @pytest.mark.parametrize(
# 	["target_prefix","script_file_name"],
# 	[
# 		("test_prefix", "test_file_name")
# 	]
# )
# def test_get_script_prefix_correct(target_prefix, script_file_name):
# 	"""
# 	Test function get_sns_name with correct input:
# 		target_prefix
# 		script_file_name
# 	Pass criteria:
# 		result_string equals to expected_string
# 	"""
# 	expected_string = f'{target_prefix}/{script_file_name}'
# 	result_string = get_script_prefix(target_prefix, script_file_name)
# 	assert result_string == expected_string

# @pytest.mark.test_get_script_prefix	
# @pytest.mark.parametrize(
# 	["target_prefix","script_file_name"],
# 	[
# 		(["fake_prefix"], "test_file_name"),
# 		("test_prefix", ["fake_file_name"])
# 	]
# )
# def test_get_script_prefix_incorrect(target_prefix, script_file_name):
# 	"""
# 	Test function get_sns_name with incorrect input:
# 		target_prefix
# 		script_file_name
# 	Pass criteria:
# 		result is None
# 	"""
# 	result = get_script_prefix(target_prefix, script_file_name)
# 	assert result is None

# @pytest.mark.test_list_to_pyspark_df	
# @pytest.mark.parametrize(
# 	["spark","obj_list"],
# 	[
# 		("fixture_setup_spark",[{'path': 'path1', 'size': 0, 'date': '20230116_212937_UTC_+0000'}, {'path': 'path2', 'size': 1, 'date': '20230126_213828_UTC_+0000'}, {'path': 'path3', 'size': 2, 'date': '20230116_230554_UTC_+0000'}])
# 	]
# )
# def test_list_to_pyspark_df_correct(spark, obj_list, request):
# 	"""
# 	Test function get_sns_name with correct input:
# 		spark
# 		obj_list
# 	Pass criteria:
# 		result_df equals to expected_df on schema, columns and values in columns
# 	"""
# 	spark = request.getfixturevalue(spark)
# 	result_df = list_to_pyspark_df(spark, obj_list)
# 	expected_columns = ['date', 'path', 'size']
# 	expected_data = [['20230116_212937_UTC_+0000', 'path1', 0],
# 					 ['20230126_213828_UTC_+0000', 'path2', 1],
# 					 ['20230116_230554_UTC_+0000', 'path3', 2]]
# 	expected_df = spark.createDataFrame(expected_data, expected_columns)
# 	assert result_df.schema == expected_df.schema
# 	assert result_df.columns == expected_df.columns
# 	assert result_df.select('path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('size').rdd.flatMap(lambda x: x).collect()	
# 	assert result_df.select('date').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('date').rdd.flatMap(lambda x: x).collect()	

# @pytest.mark.test_list_to_pyspark_df	
# @pytest.mark.parametrize(
# 	["spark","obj_list"],
# 	[
# 		("fake_spark",[{'path': 'path1', 'size': 0, 'date': '20230116_212937_UTC_+0000'}, {'path': 'path2', 'size': 1, 'date': '20230126_213828_UTC_+0000'}, {'path': 'path3', 'size': 2, 'date': '20230116_230554_UTC_+0000'}]),
# 		("fixture_setup_spark",'fake_list'),
# 		("fixture_setup_spark",['fake_list', 'fake_list_1'])
# 	]
# )
# def test_list_to_pyspark_df_incorrect(spark, obj_list, request):
# 	"""
# 	Test function get_sns_name with incorrect input:
# 		spark
# 		obj_list
# 	Pass criteria:
# 		result is None
# 	"""
# 	if spark == "fixture_setup_spark":
# 		spark = request.getfixturevalue(spark)
# 	result = list_to_pyspark_df(spark, obj_list)
# 	assert result is None
	
	
	
	
	
	
	
	
	
	
	

# @pytest.mark.test_s3_obj_to_list	
# @pytest.mark.parametrize(
# 	["s3_resource","target_bucket","target_prefix","time_format"],
# 	[
# 		("fixture_initialize_boto3_resource","s3-validation-demo","test","%Y%m%d_%H%M%S_%Z_%z")
# 	]
# )
# def test_s3_obj_to_list_correct(s3_resource, target_bucket, target_prefix, time_format, request):
# 	"""
# 	Test function get_sns_name with correct input:
# 		s3_resource
# 		target_bucket
# 		target_prefix
# 		time_format
# 	Pass criteria:
# 		result_list equals to expected_list
# 	"""
# 	s3_resource = request.getfixturevalue(s3_resource)
# 	expected_list = [{'path': 'test/', 'size': 0, 'date': '20230116_212937_UTC_+0000'}, {'path': 'test/fake_file.csv', 'size': 1150, 'date': '20230126_213828_UTC_+0000'}, {'path': 'test/hello.py', 'size': 45, 'date': '20230116_230554_UTC_+0000'}, {'path': 'test/s3_to_s3_validation.csv', 'size': 1135, 'date': '20230216_000931_UTC_+0000'}, {'path': 'test/s3_to_s3_validation_second.csv', 'size': 3805, 'date': '20230222_210744_UTC_+0000'}, {'path': 'test/spark_setup.py', 'size': 757, 'date': '20230116_213401_UTC_+0000'}]
# 	result_list = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)
# 	assert result_list == expected_list

# @pytest.mark.test_s3_obj_to_list	
# @pytest.mark.parametrize(
# 	["s3_resource","target_bucket","target_prefix","time_format"],
# 	[
# 		("fake_resource",
# 		 "s3-validation-demo","test","%Y%m%d_%H%M%S_%Z_%z"),
# 		("fixture_initialize_boto3_resource",
# 		 "fake_bucket","test","%Y%m%d_%H%M%S_%Z_%z"),
# 		("fixture_initialize_boto3_resource",
# 		 "s3-validation-demo","fake_prefix","%Y%m%d_%H%M%S_%Z_%z"),
# 		("fixture_initialize_boto3_resource",
# 		 "s3-validation-demo","test",["fake_time_format"])
# 	]
# )
# def test_s3_obj_to_list_incorrect(s3_resource, target_bucket, target_prefix, time_format, request):
# 	"""
# 	Test function get_sns_name with incorrect input:
# 		s3_resource
# 		target_bucket
# 		target_prefix
# 		time_format
# 	Pass criteria:
# 		result is None
# 	"""
# 	if s3_resource == "fixture_initialize_boto3_resource":
# 		s3_resource = request.getfixturevalue(s3_resource)
# 	result = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)
# 	assert result is None


# s3_resource = boto3.resource('s3')
# s_b = s3_resource.Bucket('s3-validation-demeeo')
# s_c = s_b.objects.filter(Prefix = 'teste')
# print('still good')
# obj_lst = []
# for obj in s_c:
# 	key_size_date_dict = {'path':obj.key.strip(), 'size':obj.size, 'date':obj.last_modified.strftime("%Y%m%d_%H%M%S_%Z_%z")}
# 	obj_lst.append(key_size_date_dict)
# print(obj_lst)



# @pytest.mark.test_file_to_pyspark_df	
# @pytest.mark.parametrize(
# 	["spark","file_bucket","file_prefix","schema"],
# 	[
# 		("fixture_setup_spark","s3-validation-demo",
# 		 "test/s3_to_s3_validation.csv","fixture_schema")
# 	]
# )
# def test_file_to_pyspark_df_correct(spark, file_bucket, file_prefix, schema, request):
# 	"""
# 	Test function get_sns_name with correct input:
# 		spark
# 		file_bucket
# 		file_prefix
# 		schema
# 	Pass criteria:
# 		result_df equals to expected_df on schema, columns and values in columns
# 	"""
# 	spark = request.getfixturevalue(spark)
# 	schema = request.getfixturevalue(schema)

# 	result_df = file_to_pyspark_df(spark, file_bucket, file_prefix, schema)
# 	expected_columns = ['id', 'path', 'size']
# 	expected_data = [[1, 'consilience-export-manifest-files/2022/000.csv', 49],
# 					 [2, 'consilience-export-manifest-files/2022/2022-11-09T20-00-00+0000.csv', 53],
# 					 [3, 'consilience-export-manifest-files/2022/3moreshouldbemissing', 456],
# 					 [4, 'consilience-export-manifest-files/2022/CDR_GI_202210200016.csv', 876],
# 					 [5, 'consilience-export-manifest-files/2022/duplicate-sites.csv', 456],
# 					 [6, 'consilience-export-manifest-files/2022/missing_2022-10-04_10-16-14_MDT_-0600.csv', 456],
# 					 [7, 'consilience-export-manifest-files/2022/missing_2022-12-21_23-26-29_MST_-0700.csv', 456],
# 					 [8, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+0000.csv', 456],
# 					 [9, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-14T04-07-00+~!@#$%^&*()=.,0000.csv', 49],
# 					 [10, 'consilience-export-manifest-files/2022/transfer-manifest-2022-10-27T18-00-00+00001.csv', 2449237854],
# 					 [11, 'consilience-export-manifest-files/2022/shouldbemissing1', 435],
# 					 [12, 'consilience-export-manifest-files/2022/shoudbemissing2', 453],
# 					 [13, 'consilience-export-manifest-files/2022/shouldbemissing3', 354],
# 					 [14, 'consilience-export-manifest-files/2022/allObjects1005.csv', 49],
# 					 [15, 'consilience-export-manifest-files/2022/transfer.csv', 2]
# 					]
# 	expected_df = spark.createDataFrame(expected_data, expected_columns)
# 	assert result_df.schema == expected_df.schema
# 	assert result_df.columns == expected_df.columns
# 	assert result_df.select('path').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('path').rdd.flatMap(lambda x: x).collect()
# 	assert result_df.select('size').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('size').rdd.flatMap(lambda x: x).collect()	
# 	assert result_df.select('id').rdd.flatMap(lambda x: x).collect() ==\
# 		expected_df.select('id').rdd.flatMap(lambda x: x).collect()	


# @pytest.mark.test_file_to_pyspark_df	
# @pytest.mark.parametrize(
# 	["spark","file_bucket","file_prefix","schema"],
# 	[
# 		("fake_spark","s3-validation-demo","test","fixture_schema"),
# 		("fixture_setup_spark","fake_bucket","test","fixture_schema"),
# 		("fixture_setup_spark","s3-validation-demo","fake_prefix","fixture_schema"),
# 		("fixture_setup_spark","s3-validation-demo","test","fake_schema"),
# 	]
# )
# def test_file_to_pyspark_df_incorrect(spark, file_bucket, file_prefix, schema, request):
# 	"""
# 	Test function get_sns_name with incorrect input:
# 		pyspark_df
# 		renames
# 	Pass criteria:
# 		result is None
# 	"""
# 	if spark == "fixture_setup_spark":
# 		spark = request.getfixturevalue(spark)
# 	if schema == "fixture_schema":
# 		schema = request.getfixturevalue(schema)
# 	result = file_to_pyspark_df(spark, file_bucket, file_prefix, schema)
# 	assert result is None

# spark = fixture_setup_spark()
# schema = (StructType().add("id",LongType(),True).add("path",StringType(),True).add("size",LongType(),True))
# file_df = (spark.read.format("csv").option("header", "true").schema(schema).load(f"s3a://s3-validation-demeo/test"))
# print(type(file_df))
# if isinstance(file_df, DataFrame):
# 	print('file_df is a dataframe')


# @pytest.mark.test_rename_columns	
# @pytest.mark.parametrize(
# 	["pyspark_df","renames"],
# 	[
# 		("fixture_second_df", {"size": "b_size","path": "b_path"})
# 	]
# )
# def test_rename_columns_correct(pyspark_df, renames, request):
# 	"""
# 	Test function get_sns_name with correct input:
# 		pyspark_df
# 		renames
# 	Pass criteria:
# 		result_set equals to expected_names_set
# 	"""
# 	pyspark_df = request.getfixturevalue(pyspark_df)
# 	renamed_df = rename_columns(pyspark_df, **renames)
# 	renamed_df_columns = renamed_df.columns
# 	result_set = set(renamed_df_columns)
# 	expected_names_set = {"b_path", "b_size", "date"}
# 	assert result_set == expected_names_set


# @pytest.mark.test_rename_columns	
# @pytest.mark.parametrize(
# 	["pyspark_df","renames"],
# 	[
# 		("fake_df", {"size": "b_size","path": "b_path"})
# 	]
# )
# def test_rename_columns_incorrect(pyspark_df, renames, request):
# 	"""
# 	Test function get_sns_name with incorrect input:
# 		pyspark_df
# 		renames
# 	Pass criteria:
# 		renamed_df is None
# 	"""
# 	renamed_df = rename_columns(pyspark_df, **renames)
# 	assert renamed_df is None



	
	
	
	
	
	
	
	
	
	
	
	
	
	
# def dict_test(name, **rename):
# 	print(type(rename))
# 	print(type(**rename))
# 	if not isinstance(rename, dict):
# 		return 'wrong name'
# 	return rename
# 	# return rename["1"]
# try:
# 	dict_test_result = dict_test('wer', **{'werwaer':6})
# except TypeError as e:
# 	print(e)
# 	print('dict_test_result error')
# else:
# 	print('dict_test_result finish')
# @pytest.mark.parametrize(
# 	["name", "renames"],
# 	[
# 		("good", {"1":1}),
# 		("very good", {"1":2})
# 	]
# )
# def test_dict(name, renames):
# 	result = dict_test(name, **renames)
# 	# assert result == 1 or result == 2
# 	assert isinstance(result, dict)


# try:
# 	sns_client = boto3.client('sns')
# 	response = sns_client.publish(
# 		TargetArn="arn:aws:sns:us-west-2:064047601590:s3-validation-demo",
# 		Message=json.dumps({'default': json.dumps({"2":"2"}, indent = 6)}),
# 		Subject="fake",
# 		MessageStructure='json')
# except (sns_client.exceptions.AuthorizationErrorException,
# 		sns_client.exceptions.InvalidParameterException,
# 		sns_client.exceptions.NotFoundException) as err:
# 	print('Not a valid sns_topic_arn.')
# 	print(err)
# 	print('"sns_send" function completed unsuccessfully.')
# else:
# 	print('"sns_send" function completed successfully.')


# @pytest.mark.test_get_sns_name	
# @pytest.mark.parametrize(
# 	["sns_client","sns_name"],
# 	[
# 		("fixture_initialize_boto3_client", "s3-validation-demo")
# 	]
# )
# def test_get_sns_arn_correct(sns_client, sns_name, request):
# 	"""
# 	Test function get_sns_name with correct input:
# 		sns_client
# 		sns_name
# 	Pass criteria:
# 		result equals to expected_response
# 	"""
# 	sns_client = request.getfixturevalue(sns_client)
# 	expected_response = "arn:aws:sns:us-west-2:064047601590:s3-validation-demo"
# 	result = get_sns_arn(sns_client, sns_name)
# 	assert result == expected_response

# @pytest.mark.test_get_sns_name	
# @pytest.mark.parametrize(
# 	["sns_client","sns_name"],
# 	[
# 		("fake_client", "s3-validation-demo"),
# 		("fixture_initialize_boto3_client", "fake_bucket")
# 	]
# )
# def test_get_sns_arn_incorrect(sns_client, sns_name, request):
# 	"""
# 	Test function get_sns_name with incorrect input:
# 		sns_client
# 		sns_name
# 	Pass criteria:
# 		result is None
# 	"""
# 	if sns_client == "fixture_initialize_boto3_client":
# 		sns_client = request.getfixturevalue(sns_client)
# 	result = get_sns_arn(sns_client, sns_name)
# 	assert result is None

# @pytest.mark.test_get_sns_name	
# @pytest.mark.parametrize(
# 	"target_bucket",
# 	[
# 		"example_bucket",
# 		"test_bucket"
# 	]
# )
# def test_get_sns_name_correct(target_bucket):
# 	"""
# 	Test function get_sns_name with correct input:
# 		target_bucket
# 	Pass criteria:
# 		result equals to target_bucket.replace('.','')
# 	"""
# 	result = get_sns_name(target_bucket)
# 	assert result == target_bucket.replace('.','')

# @pytest.mark.test_get_sns_name	
# @pytest.mark.parametrize(
# 	"target_bucket",
# 	[
# 		["example_bucket"],
# 		{"test_bucket"}
# 	]
# )
# def test_get_sns_name_incorrect(target_bucket):
# 	"""
# 	Test function get_sns_name with incorrect input:
# 		target_bucket
# 	Pass criteria:
# 		result is None
# 	"""
# 	result = get_sns_name(target_bucket)
# 	assert result is None



# @pytest.mark.test_initialize_boto3_resource	
# @pytest.mark.parametrize(
# 	"aws_service",
# 	[
# 		"s3",
# 		"sns"
# 	]
# )
# def test_initialize_boto3_resource_correct(aws_service):
# 	"""
# 	Test function initialize_boto3_resource with correct input:
# 		aws_service
# 	Pass criteria:
# 		result.__class__.__name__ equals to aws_service.ServiceResource
# 	"""
# 	result = initialize_boto3_resource(aws_service)
# 	assert result.__class__.__name__ == aws_service+'.ServiceResource'

# @pytest.mark.test_initialize_boto3_resource	
# @pytest.mark.parametrize(
# 	"aws_service",
# 	[
# 		"ooo",
# 		"ppp",
# 		["opop"]
# 	]
# )
# def test_initialize_boto3_resource_incorrect(aws_service):
# 	"""
# 	Test function initialize_boto3_resource with incorrect input:
# 		aws_service
# 	Pass criteria:
# 		result is None
# 	"""
# 	result = initialize_boto3_resource(aws_service)
# 	assert result is None


# @pytest.mark.test_initial_boto3_client	
# @pytest.mark.parametrize(
# 	"aws_service",
# 	[
# 		"s3",
# 		"sns"
# 	]
# )
# def test_initialize_boto3_client_correct(aws_service):
# 	"""
# 	Test function generate_result_location with correct input:
# 		aws_service
# 	Pass criteria:
# 		result.__class__.__name__ equals to aws_service.upper()
# 	"""
# 	result = initialize_boto3_client(aws_service)
# 	assert result.__class__.__name__ == aws_service.upper()
	
# @pytest.mark.test_initial_boto3_client	
# @pytest.mark.parametrize(
# 	"aws_service",
# 	[
# 		"ooo",
# 		"ppp",
# 		["opop"]
# 	]
# )
# def test_initialize_boto3_client_incorrect(aws_service):
# 	"""
# 	Test function generate_result_location with incorrect input:
# 		aws_service
# 	Pass criteria:
# 		result is None
# 	"""
# 	result = initialize_boto3_client(aws_service)
# 	assert result is None



# @pytest.mark.test_generate_result_location	
# @pytest.mark.parametrize(
# 	["target_bucket", "target_prefix"],
# 	[
# 		("s3-validation-demo", "consilience-export-manifest-files/2022")
# 	]
# )
# def test_generate_result_location_correct(target_bucket, target_prefix):
# 	"""
# 	Test function generate_result_location with correct input:
# 		target_bucket
# 		target_prefix
# 	Pass criteria:
# 		actual_result_target_prefix equals to expected_result_target_prefix
# 	"""
# 	expected_result_target_prefix = f"{target_bucket}/s3_to_s3_validation_result_{target_bucket}_consilience-export-manifest-files_2022/"
# 	actual_result_target_prefix = generate_result_location(target_bucket, target_prefix)
# 	assert actual_result_target_prefix == expected_result_target_prefix

# @pytest.mark.test_generate_result_location	
# @pytest.mark.parametrize(
# 	["target_bucket", "target_prefix"],
# 	[
# 		(["s3-validation-demo"], "consilience-export-manifest-files/2022"),
# 		("s3-validation-demo", ["consilience-export-manifest-files/2022"])
# 	]
# )
# def test_generate_result_location_incorrect(target_bucket, target_prefix):
# 	"""
# 	Test function generate_result_location with incorrect input:
# 		target_bucket
# 		target_prefix
# 	Pass criteria:
# 		actual_result_target_prefix is None
# 	"""
# 	actual_result_target_prefix = generate_result_location(target_bucket, target_prefix)
# 	assert actual_result_target_prefix is None

# @pytest.mark.test_get_current_denver_time	
# @pytest.mark.parametrize(
# 	["time_zone", "time_format"],
# 	[
# 		("US/Mountain", "%Y%m%d_%H%M%S_%Z_%z")
# 	]
# )
# def test_get_current_denver_time_correct(time_zone, time_format):
# 	"""
# 	Test function get_current_denver_time with correct input:
# 		time_zone
# 		time_format
# 	Pass criteria:
# 		result is current_denver_time
# 	"""
# 	current_denver_time = datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)
# 	assert get_current_denver_time(time_zone, time_format) == current_denver_time
	
# @pytest.mark.test_get_current_denver_time	
# @pytest.mark.parametrize(
# 	["time_zone", "time_format"],
# 	[
# 		("fake_timezone", "%Y%m%d_%H%M%S_%Z_%z")
# 	]
# )
# def test_get_current_denver_time_incorrect(time_zone, time_format):
# 	"""
# 	Test function get_current_denver_time with correct input:
# 		time_zone
# 		time_format
# 	Pass criteria:
# 		result is None
# 	"""
# 	assert get_current_denver_time(time_zone, time_format) == 'cannot_get_timestamp'
	
	
	
	

# @pytest.mark.test_prefix_validation	
# @pytest.mark.parametrize(
# 	["s3_prefix", "s3_prefix_list"],
# 	[
# 		("test", ["test/","test/child_folder"]),
# 		("test/", ["test/","test/child_folder"])
# 	]
# )
# def test_prefix_validation_correct(s3_prefix, s3_prefix_list):
# 	"""
# 	Test function prefix_validation with correct input:
# 		s3_prefix
# 		s3_prefix_list
# 	Pass criteria:
# 		result is s3_prefix
# 	"""
# 	result = prefix_validation(s3_prefix, s3_prefix_list)
# 	if s3_prefix[-1] != "/":
# 		s3_prefix += "/"	
# 	assert result == s3_prefix


# @pytest.mark.test_prefix_validation	
# @pytest.mark.parametrize(
# 	["s3_prefix", "s3_prefix_list"],
# 	[
# 		("test", ["other","test/child_folder"]),
# 		("test/", ["other/","test/child_folder"])
# 	]
# )
# def test_prefix_validation_incorrect(s3_prefix, s3_prefix_list):
# 	"""
# 	Test function prefix_validation with correct input:
# 		s3_prefix
# 		s3_prefix_list
# 	Pass criteria:
# 		result is None
# 	"""
# 	result = prefix_validation(s3_prefix, s3_prefix_list)
# 	assert result is None


# @pytest.mark.test_prefix_to_list	
# @pytest.mark.parametrize(
# 	["s3_bucket", "s3_prefix", "s3_resource"],
# 	[
# 		("fake bucket", "test", "fixture_initialize_boto3_resource"),
# 		("s3-validation-demo", "fake_prefix", "fixture_initialize_boto3_resource"),
# 		("s3-validation-demo", "test", "fake_s3_resource")
# 	]
# )
# def test_prefix_to_list_incorrect(s3_bucket, s3_prefix, s3_resource, request):
# 	"""
# 	Test function prefix_to_list with incorrect input:
# 		s3_bucket
# 		s3_prefix
# 		s3_resource
# 	Pass criteria:
# 		result is None
# 	"""
# 	if s3_resource == "fixture_initialize_boto3_resource":
# 		s3_resource = request.getfixturevalue(s3_resource)
# 	result = prefix_to_list(s3_bucket, s3_prefix, s3_resource)
# 	assert result is None

# s3_resource = boto3.resource('s3')
# s3_bucket_objects_collection = s3_resource.Bucket("s3-werewrw-demo") \
#             .objects.filter(Prefix="test")

# print(s3_bucket_objects_collection)
# for item in s3_bucket_objects_collection:
# 	print(item)









































