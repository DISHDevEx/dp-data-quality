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
from s3_to_s3_validation_script import Validation

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

    df = test_initial_pyspark.read.csv(data_location, header = False, inferSchema = True)
    df = df.withColumnRenamed("_c0", "Site").withColumnRenamed("_c1", "Accessment") \
    .withColumnRenamed("_c2", "Path").withColumnRenamed("_c3", "Size")
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

@pytest.fixture
def test_object():
    test_object = Validation()
    return test_object

def test_get_current_denver_time(test_object):
    """
    Test if time gotten is the right one
    Functions are executed on the same second, the result is reliable
    May need execute several time to get assertions true
    """
    time_zone = 'US/Mountain'
    time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
    current_den = datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)
    current_not_den = datetime.now().astimezone(timezone.utc).strftime(time_format)
    assert test_object.get_current_denver_time(time_zone, time_format) == current_den
    assert test_object.get_current_denver_time(time_zone, time_format) != current_not_den

@pytest.fixture
def test_get_current_denver_time_fixture(test_object):
    time_zone = 'US/Mountain'
    time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
    return datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)

def test_generate_result_location(test_object):
    target_bucket = "test"
    target_prefix = "this/is/a/ test /string"
    expected_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_this_is_a_ test _string/"
    incorrect_result_target_prefix = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_this/is/a/ test /string/"
    actual_result_target_prefix = test_object.generate_result_location(target_bucket, target_prefix)
    assert actual_result_target_prefix == expected_result_target_prefix
    assert actual_result_target_prefix != incorrect_result_target_prefix

def test_initial_boto3_client(test_object):
    aws_service = "sns"
    expected_client = None
    incorrect_client = None
    expected_client = test_object.initial_boto3_client(aws_service)
    # Next line will not be executed in SageMaker, because it's not a valid API call.
    # incorrect_s3_client = validation_obj.initial_boto3_client(incorrect_aws_service)
    assert expected_client != None
    assert incorrect_client == None
    
@pytest.fixture
def test_initial_boto3_client_fixture(test_object):
    aws_service = "sns"
    aws_client = None
    incorrect_client = None
    aws_client = test_object.initial_boto3_client(aws_service)
    return aws_client

def test_initial_boto3_resource(test_object):
    aws_service = "s3"
    expected_resource = None
    incorrect_resource = None
    expected_resource = test_object.initial_boto3_resource(aws_service)
    # Next line will not be executed in SageMaker, because it's not a valid API call.
    # incorrect_s3_resource = validation_obj.initial_boto3_resource(incorrect_aws_service)
    assert expected_resource != None
    assert incorrect_resource == None

def test_get_sns_name(test_object):
    target_bucket = "a.b.c.d"
    expected_output_name = "abcd"
    incorrect_output_name = "a.b.c.d"
    actual_output_name = test_object.get_sns_name(target_bucket)
    assert actual_output_name == expected_output_name
    assert actual_output_name != incorrect_output_name

def test_get_sns_arn(test_object, test_initial_boto3_client_fixture):
    # This is an existing sns_name while coding, however our SageMaker instance is not able to reach out there.
    sns_name = "dish-dp-datalake-sns"
    response = None
    response = test_object.get_sns_arn(test_initial_boto3_client_fixture, sns_name)
    assert response != None
       
def test_rename_bucket_df(test_object, test_second_df):
    original_column_names = test_second_df.columns
    renamed_df_column_names = test_object.rename_bucket_df(test_second_df).columns
    expected_names = ["bPath", "bSize", "Date"]
    assert set(renamed_df_column_names) == set(expected_names)
    assert set(renamed_df_column_names) != set(original_column_names)

@pytest.fixture
def test_rename_bucket_df_fixture(test_object, test_second_df):
    renamed_df = test_object.rename_bucket_df(test_second_df)
    return renamed_df

def test_get_missing_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture):
    missing_df = test_object.get_missing_objects(test_file_to_df, test_rename_bucket_df_fixture)
    should_missing = ['consilience-export-manifest-files/2022/shouldbemissing1',
                        'consilience-export-manifest-files/2022/shoudbemissing2',
                        'consilience-export-manifest-files/2022/shouldbemissing3',
                        'consilience-export-manifest-files/2022/3moreshouldbemissing']
    missing_list = list(missing_df.select('Path').toPandas()['Path'])
    assert set(should_missing).issubset(set(missing_list)) == 1

def test_get_df_count(test_object, test_rename_bucket_df_fixture):
    row_count = test_object.get_df_count(test_rename_bucket_df_fixture)
    assert row_count == 27
    assert row_count != 26

def test_get_match_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture):
    match_df = test_object.get_match_objects(test_file_to_df, test_rename_bucket_df_fixture)
    row_count = match_df.count()
    assert row_count == 11
    assert row_count != 10

@pytest.fixture
def test_get_match_objects_fixture(test_object, test_file_to_df, test_rename_bucket_df_fixture):
    match_df = test_object.get_match_objects(test_file_to_df, test_rename_bucket_df_fixture)
    return match_df

def test_get_wrong_size_objects(test_object, test_get_match_objects_fixture):
    wrong_size_df = test_object.get_wrong_size_objects(test_get_match_objects_fixture)
    row_count = wrong_size_df.count()
    assert row_count == 8
    assert row_count != 10

def test_save_result(test_object, test_file_to_df, test_empty_dataframe, test_get_current_denver_time_fixture):
    current = test_get_current_denver_time_fixture
    bucket = "s3-validation-demo"
    data_key = "pytest_result/s3_to_s3_validation_pytest_result.csv"
    result_location = f"s3a://{bucket}/{data_key}"
    row_count_empty = test_empty_dataframe.count()
    row_count_non_empty = test_file_to_df.count()
    message_empty = test_object.save_result(row_count_empty, result_location, current, test_empty_dataframe)
    message_non_empty = test_object.save_result(row_count_non_empty, result_location, current, test_file_to_df)
    assert message_empty == "no missing item found"
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


def main():
    """
    Execute all test functions above
    """
    test_object = Validation()
    
    test_get_current_denver_time(test_object)
    test_generate_result_location(test_object)
    test_initial_boto3_client(test_object)
    test_initial_boto3_resource(test_object)
    test_get_sns_name(test_object)
    test_get_sns_arn(test_object, test_initial_boto3_client_fixture)
    test_rename_bucket_df(test_object, test_second_df)
    test_get_missing_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture)
    test_get_df_count(test_object, test_rename_bucket_df_fixture)
    test_get_match_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture)
    test_get_wrong_size_objects(test_object, test_get_match_objects_fixture)
    test_save_result(test_object, test_file_to_df, test_empty_dataframe, test_get_current_denver_time_fixture)
    # test_result_to_subscriber(test_object, test_initial_boto3_client_fixture, test_get_current_denver_time_fixture)




if __name__ == "__main__":
    main()
    print("Tests completed")