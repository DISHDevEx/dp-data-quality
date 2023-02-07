"""
Pytest file to test s3_to_s3_validation_script.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""

import pytest
import sys
import json
from datetime import datetime, timezone
import time
import boto3
import pytz
# from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
# from awsglue.context import GlueContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from s3_to_s3_validation_script import *

@pytest.fixture
def test_empty_dataframe():
    # Create a spark session
    spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()
    # Create an expected schema
    columns = StructType([StructField('Path',
                                      StringType(), True),
                        StructField('Size',
                                    StringType(), True),
                        StructField('Date',
                                    StringType(), True)])
    # Create an empty RDD with expected schema
    df = spark.createDataFrame(data = emp_RDD,
                               schema = columns)
    return df

@pytest.fixture
def test_initial_pyspark():
    packages = (",".join(["io.delta:delta-core_2.12:1.1.0","org.apache.hadoop:hadoop-aws:3.2.2"]))
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

@pytest.fixture
def test_file_to_df(test_initial_pyspark):
    bucket = "s3-validation-demo"
    data_key = "test/s3_to_s3_validation.csv"
    data_location = f"s3a://{bucket}/{data_key}"
    schema_str = 'Site string, Assessment string, Path string, Size long'
    df = test_initial_pyspark.read.csv(data_location, header = False, schema = schema_str)
    # df.show(truncate = False)
    return df

@pytest.fixture
def test_second_df(test_initial_pyspark):
    bucket = "s3-validation-demo"
    data_key = "test/s3_to_s3_validation_second.csv"
    data_location = f"s3a://{bucket}/{data_key}"

    df = test_initial_pyspark.read.csv(data_location, header = True, inferSchema = True)
    # df.show(truncate = False)
    return df

# @pytest.fixture
# def test_initial_spark():
#     spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
#     return spark

@pytest.fixture
def test_get_current_denver_time_fixture():
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    return datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)

# def test_get_target_location():
#     # From glue job instance, not able to test in sagemaker

# def test_get_file_location():
#     # From glue job instance, not able to test in sagemaker

def test_get_current_denver_time():
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

def test_generate_result_location():
    target_bucket = "s3-validation-demo"
    target_prefix = "consilience-export-manifest-files/2022"
    expected_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_consilience-export-manifest-files_2022/"
    incorrect_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_{target_prefix}/"
    actual_result_target_prefix = generate_result_location(target_bucket, target_prefix)
    assert actual_result_target_prefix == expected_result_target_prefix
    assert actual_result_target_prefix != incorrect_result_target_prefix

def test_initial_boto3_client():
    aws_service = "sns"
    incorrect_client = None
    expected_client = initial_boto3_client(aws_service)
    # Next line will not be executed in SageMaker, because it's not a valid API call.
    # incorrect_s3_client = validation_obj.initial_boto3_client(incorrect_aws_service)
    assert expected_client != None
    assert incorrect_client == None
    
@pytest.fixture
def test_initial_boto3_client_fixture():
    aws_service = "sns"
    aws_client = initial_boto3_client(aws_service)
    return aws_client

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
#     # SageMaker is not connect with SNS with our roles, cannot test now.
#     response = test_object.sns_send("test", "subject")
#     assert response != None

def test_rename_columns(test_second_df):
    original_column_names = test_second_df.columns
    rename_cols = {"Size": "bSize","Path": "bPath"}
    renamed_df_column_names = rename_columns(test_second_df, **rename_cols).columns
    expected_names = ["bPath", "bSize", "Date"]
    assert set(renamed_df_column_names) == set(expected_names)
    assert set(renamed_df_column_names) != set(original_column_names)

@pytest.fixture
def test_rename_bucket_df_fixture(test_second_df):
    rename_cols = {"Size": "bSize","Path": "bPath"}
    renamed_df = rename_columns(test_second_df, **rename_cols)
    return renamed_df

def test_file_to_pyspark_df(test_initial_pyspark):
    result = None
    spark = test_initial_pyspark
    file_bucket = "s3-validation-demo"
    file_prefix = "test"
    schema_str = 'Site string, Assessment string, Path string, Size long'
    result = file_to_pyspark_df(spark, file_bucket, file_prefix, schema_str)
    
    # fake_spark = test_initial_pyspark
    # fake_file_bucket = "s3-validation-demo-foo"
    # fake_file_prefix = "test-foo"
    # fake_schema_str = 'Site string, Path string, Size long'
    # fake_result = file_to_pyspark_df(fake_spark, fake_file_bucket, fake_file_prefix, fake_schema_str)
    
    assert result != None
    # assert fake_result == None

def test_s3_obj_to_list():
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

def test_list_to_pyspark_df(test_initial_pyspark):
    obj_list = [{"name":"alice", "age":19},{"name":"bob", "age":20},{"name":"cindy", "age":21} ]
    spark = test_initial_pyspark
    result = list_to_pyspark_df(spark, obj_list)

    fake_obj_list = 'fake_list'
    fake_spark = test_initial_pyspark
    fake_result = list_to_pyspark_df(fake_spark, fake_obj_list)

    assert result.count() == 3
    assert fake_result == None

def test_get_script_prefix():
    target_prefix = "consilience-export-manifest-files/2022"
    expected_result = "consilience-export-manifest-files/2022/fake_name"
    unexpected_result = "consilience-export-manifest-files/2022//fake_name"
    result = get_script_prefix(target_prefix, "fake_name")
    assert result == expected_result
    assert result != unexpected_result

def test_remove_script_from_df(test_second_df):
    remove_value = "consilience-export-manifest-files/2022/s3_to_s3_validation.csv"
    column_name = "Path"
    result = remove_script_from_df(test_second_df, remove_value, column_name)

    # fake_remove_value = "consilience-export-manifest-files/2022/s3_to_s3_validation.csv-foo"
    # fake_column_name = "Path-foo"
    # fake_result = remove_script_from_df(test_second_df, fake_remove_value, fake_column_name)
    # assert fake_result.count() == 28
    assert result.count() == 27

def test_get_missing_objects(test_file_to_df, test_rename_bucket_df_fixture):
    missing_df = get_missing_objects(test_file_to_df, test_rename_bucket_df_fixture,
                                                "Path", "bPath")
    should_missing = ['consilience-export-manifest-files/2022/shouldbemissing1',
                        'consilience-export-manifest-files/2022/shoudbemissing2',
                        'consilience-export-manifest-files/2022/shouldbemissing3',
                        'consilience-export-manifest-files/2022/3moreshouldbemissing']
    missing_list = list(missing_df.select('Path').toPandas()['Path'])
    # fake_missing_df = get_missing_objects(test_file_to_df, test_rename_bucket_df_fixture,
    #                                             "Path_foo", "bPath_foo")
    # fake_mssing_list = list(fake_missing_df.select('Path').toPandas()['Path'])
    assert set(should_missing).issubset(set(missing_list)) == 1
    # assert set(should_missing).issubset(set(fake_mssing_list)) == 0

def test_get_df_count(test_second_df):
    row_count = get_df_count(test_second_df)
    # fake_row_count = get_df_count('fake_input')
    assert row_count == 27
    # assert fake_row_count == None

def test_get_match_objects(test_file_to_df, test_rename_bucket_df_fixture):
    match_df = get_match_objects(test_file_to_df, test_rename_bucket_df_fixture,
                            "Path", "Size", "bPath", "bSize", "Date")
    row_count = match_df.count()
    # fake_match_df = get_match_objects(test_file_to_df, test_rename_bucket_df_fixture,
    #                         "o", "p", "q", "r", "s")
    assert row_count == 11
    # assert fake_match_df == None

@pytest.fixture
def test_get_match_objects_fixture(test_file_to_df, test_rename_bucket_df_fixture):
    match_df = get_match_objects(test_file_to_df, test_rename_bucket_df_fixture,
                            "Path", "Size", "bPath", "bSize", "Date")
    return match_df

def test_get_wrong_size_objects(test_get_match_objects_fixture):
    wrong_size_df = get_wrong_size_objects(test_get_match_objects_fixture, "Size", "bSize")
    row_count = wrong_size_df.count()
    # fake_wrong_size_df = get_wrong_size_objects(test_get_match_objects_fixture, "o", "p")
    assert row_count == 8
    # assert fake_wrong_size_df == None

def test_save_result(test_file_to_df, test_empty_dataframe, test_get_current_denver_time_fixture):
    current = test_get_current_denver_time_fixture
    bucket = "s3-validation-demo"
    data_key = "pytest_result/s3_to_s3_validation_pytest_result.csv"
    result_location = f"s3a://{bucket}/{data_key}"
    row_count_empty = test_empty_dataframe.count()
    row_count_non_empty = test_file_to_df.count()
    obj_name = "test_object"
    message_empty = None
    message_non_empty = None
    message_empty = save_result(row_count_empty, result_location, current, test_empty_dataframe, obj_name)
    message_non_empty = save_result(row_count_non_empty, result_location, current, test_file_to_df, obj_name)
    assert message_empty == "no test_object item found"
    assert message_non_empty == "result saved"

# # Cannot test this function, because current IAM role cannot publish message to SNS.
# def test_result_to_subscriber(test_object, test_initial_boto3_client_fixture, test_get_current_denver_time_fixture):
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