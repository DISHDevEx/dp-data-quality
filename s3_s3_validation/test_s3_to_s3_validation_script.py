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

# def test_create_object():
#     # Cannot generate object the same way in Glue Job, have to skip this test
#     trigger_bucket_name = 's3_bucket'
#     trigger_path_name = 's3_path'
#     time_zone = 'US/Mountain'
#     time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
#     aws_sns_client = 'sns'
#     aws_s3_resource = 's3'
#     validation_obj = Validation.create_object(trigger_bucket_name,
#                                             trigger_path_name,
#                                             time_zone,
#                                             time_format,
#                                             aws_sns_client,
#                                             aws_s3_resource
#                                             )
#     assert validation_obj.target_bucket!=None and validation_obj.target_prefix!=None and \
#         validation_obj.file_bucket!=None and validation_obj.file_prefix!=None and  \
#         validation_obj.current!=None and validation_obj.spark!=None and \
#         validation_obj.sns_client!=None and validation_obj.s3_resource!=None and \
#         validation_obj.sns_name==None and validation_obj.sns_topic_arn==None and \
#         validation_obj.time_zone==None and validation_obj.time_format==None
#     assert not (validation_obj.target_bucket==None or validation_obj.target_prefix==None or \
#         validation_obj.file_bucket==None or validation_obj.file_prefix==None or  \
#         validation_obj.current==None or validation_obj.spark==None or \
#         validation_obj.sns_client==None or validation_obj.s3_resource==None or \
#         validation_obj.sns_name!=None or validation_obj.sns_topic_arn!=None or \
#         validation_obj.time_zone!=None or validation_obj.time_format!=None)

@pytest.fixture
def test_initial_spark():
    spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
    return spark

@pytest.fixture
def test_get_current_denver_time_fixture():
    time_zone = 'US/Mountain'
    time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
    return datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)

@pytest.fixture
def test_object(test_initial_pyspark):
    time_zone = 'US/Mountain'
    time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
    current_den = datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)

    test_object = Validation()
    test_object.target_bucket = "s3-validation-demo"
    test_object.target_prefix = "consilience-export-manifest-files/2022"
    test_object.file_bucket = "s3-validation-demo"
    test_object.file_prefix = "test"
    test_object.current = current_den
    test_object.spark = test_initial_pyspark
    test_object.sns_client = boto3.client("sns")
    test_object.s3_resource = boto3.resource("s3")
    test_object.sns_name = "s3-validation-demo"
    test_object.sns_topic_arn = "arn:aws:sns:us-west-2:064047601590:s3-validation-demo"
    test_object.time_zone = 'US/Mountain'
    test_object.time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
    return test_object

# def test_get_target_location():
#     # From glue job instance, not able to test in sagemaker

# def test_get_file_location():
#     # From glue job instance, not able to test in sagemaker

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

def test_generate_result_location(test_object):
    target_bucket = "s3-validation-demo"
    target_prefix = "consilience-export-manifest-files/2022"
    expected_result_target_prefix = f"s3a://{test_object.target_bucket}/s3_to_s3_validation_result_{test_object.target_bucket}_consilience-export-manifest-files_2022/"
    incorrect_result_target_prefix = f"s3a://{test_object.target_bucket}/s3_to_s3_validation_result_{test_object.target_bucket}_{test_object.target_prefix}/"
    actual_result_target_prefix = test_object.generate_result_location()
    assert actual_result_target_prefix == expected_result_target_prefix
    assert actual_result_target_prefix != incorrect_result_target_prefix

# def test_initial_pyspark():
#     # From glue job instance, not able to test in sagemaker
#     spark = test_object.initial_pyspark()
#     assert spark != None

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
    expected_output_name = "s3-validation-demo"
    incorrect_output_name = "a.b.c.d"
    actual_output_name = test_object.get_sns_name()
    assert actual_output_name == expected_output_name
    assert actual_output_name != incorrect_output_name

def test_get_sns_arn(test_object, test_initial_boto3_client_fixture):
    # This is an existing sns_name while coding, however our SageMaker instance is not able to reach out there.
    response = None
    expected_response = "arn:aws:sns:us-west-2:064047601590:s3-validation-demo"
    response = test_object.get_sns_arn()
    assert response != None
    assert response == expected_response
       
# def test_sns_send(test_object):
#     # SageMaker is not connect with SNS with our roles, cannot test now.
#     response = test_object.sns_send("test", "subject")
#     assert response != None

def test_rename_columns(test_object, test_second_df):
    original_column_names = test_second_df.columns
    rename_columns = {"Size": "bSize","Path": "bPath"}
    renamed_df_column_names = test_object.rename_columns(test_second_df, **rename_columns).columns
    expected_names = ["bPath", "bSize", "Date"]
    assert set(renamed_df_column_names) == set(expected_names)
    assert set(renamed_df_column_names) != set(original_column_names)

@pytest.fixture
def test_rename_bucket_df_fixture(test_object, test_second_df):
    rename_columns = {"Size": "bSize","Path": "bPath"}
    renamed_df = test_object.rename_columns(test_second_df, **rename_columns)
    return renamed_df

def test_file_to_pyspark_df(test_object):
    result = None
    file_df_columns = {"_c0": "Site","_c1": "Accessment","_c2": "Path","_c3": "Size"}
    result = test_object.file_to_pyspark_df(**file_df_columns)
    assert result != None

def test_list_to_pyspark_df(test_object):
    test_list = [{"name":"alice", "age":19},{"name":"bob", "age":20},{"name":"cindy", "age":21} ]
    result = test_object.list_to_pyspark_df(test_list)
    assert result.count() == 3
    assert result != None

def test_s3_obj_to_pyspark_df(test_object):
    result = None
    result = test_object.s3_obj_to_pyspark_df()
    assert result != None

def test_get_script_prefix(test_object):
    result = test_object.get_script_prefix("fake_name")
    assert result == "consilience-export-manifest-files/2022/fake_name"
    assert result != None

def test_remove_script_from_df(test_object, test_second_df):
    remove_value = "consilience-export-manifest-files/2022/s3_to_s3_validation.csv"
    column_name = "Path"
    result = None
    result = test_object.remove_script_from_df(test_second_df, remove_value, column_name)
    assert result != None
    assert result.count() == 27

def test_get_missing_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture):
    missing_df = test_object.get_missing_objects(test_file_to_df, test_rename_bucket_df_fixture,
                                                "Path", "bPath")
    should_missing = ['consilience-export-manifest-files/2022/shouldbemissing1',
                        'consilience-export-manifest-files/2022/shoudbemissing2',
                        'consilience-export-manifest-files/2022/shouldbemissing3',
                        'consilience-export-manifest-files/2022/3moreshouldbemissing']
    missing_list = list(missing_df.select('Path').toPandas()['Path'])
    assert set(should_missing).issubset(set(missing_list)) == 1

def test_get_df_count(test_object, test_second_df):
    row_count = None
    row_count = test_object.get_df_count(test_second_df)
    assert row_count == 27
    assert row_count != None

def test_get_match_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture):
    match_df = test_object.get_match_objects(test_file_to_df, test_rename_bucket_df_fixture,
                            "Path", "Size", "bPath", "bSize", "Date")
    row_count = None
    row_count = match_df.count()
    assert row_count == 11
    assert row_count != 10

@pytest.fixture
def test_get_match_objects_fixture(test_object, test_file_to_df, test_rename_bucket_df_fixture):
    match_df = test_object.get_match_objects(test_file_to_df, test_rename_bucket_df_fixture,
                            "Path", "Size", "bPath", "bSize", "Date")
    return match_df

def test_get_wrong_size_objects(test_object, test_get_match_objects_fixture):
    wrong_size_df = test_object.get_wrong_size_objects(test_get_match_objects_fixture, "Size", "bSize")
    row_count = None
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
    obj_name = "test_object"
    message_empty = None
    message_non_empty = None
    message_empty = test_object.save_result(row_count_empty, result_location, current, test_empty_dataframe, obj_name)
    message_non_empty = test_object.save_result(row_count_non_empty, result_location, current, test_file_to_df, obj_name)
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
    test_get_sns_arn(test_object)
    test_rename_columns(test_object, test_second_df)
    test_file_to_pyspark_df(test_object)
    test_list_to_pyspark_df(test_object)
    test_s3_obj_to_pyspark_df(test_object)
    test_get_script_prefix(test_object)
    test_remove_script_from_df(test_object, test_second_df)
    test_get_missing_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture)
    test_get_df_count(test_object, test_second_df)
    test_get_match_objects(test_object, test_file_to_df, test_rename_bucket_df_fixture)
    test_get_wrong_size_objects(test_object, test_get_match_objects_fixture)
    test_save_result(test_object, test_file_to_df, test_empty_dataframe, test_get_current_denver_time_fixture)
    # test_result_to_subscriber(test_object, test_initial_boto3_client_fixture, test_get_current_denver_time_fixture)




if __name__ == "__main__":
    main()
    print("Tests completed")