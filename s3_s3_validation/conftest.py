"""
Pytest file to test s3_to_s3_validation_script.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""

import pytest
from datetime import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField
from s3_to_s3_validation_script import *

@pytest.fixture(scope='module')
def fixture_empty_dataframe():
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

@pytest.fixture(scope='module')
def fixture_setup_spark():
    packages = (",".join(["io.delta:delta-core_2.12:2.2.0","org.apache.hadoop:hadoop-aws:3.3.4"]))
    # spark_driver_memory = '8g'
    # spark_executor_memory = '8g'
    # spark_memory_offHeap_enabled = True
    # spark_memory_offHeap_size =  '10g'
    # spark_driver_maxResultSize = '2g'
    # print('packages: '+packages)

    # Instantiate Spark via builder
    # Note: we use the `ContainerCredentialsProvider` to give us access to underlying IAM role permissions

    spark = (SparkSession
        .builder
        .appName("PySparkApp") 
        .config("spark.jars.packages", packages) 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .config("fs.s3a.aws.credentials.provider",'com.amazonaws.auth.ContainerCredentialsProvider') 
        # .config("spark.driver.memory", spark_driver_memory)
        # .config("spark.executor.memory", spark_executor_memory)
        # .config("spark.memory.offHeap.enabled", spark_memory_offHeap_enabled)
        # .config("spark.memory.offHeap.size", spark_memory_offHeap_size)
        # .config("spark.sql.broadcastTimeout", "36000")

    ).getOrCreate()
    return spark

@pytest.fixture(scope='module')
def fixture_file_to_df(fixture_setup_spark):
    bucket = "s3-validation-demo"
    data_key = "test/s3_to_s3_validation.csv"
    data_location = f"s3a://{bucket}/{data_key}"
    schema_str = 'Site string, Assessment string, Path string, Size long'
    df = fixture_setup_spark.read.csv(data_location, header = False, schema = schema_str)
    # df.show(truncate = False)
    return df

@pytest.fixture(scope='module')
def fixture_second_df(fixture_setup_spark):
    bucket = "s3-validation-demo"
    data_key = "test/s3_to_s3_validation_second.csv"
    data_location = f"s3a://{bucket}/{data_key}"

    df = fixture_setup_spark.read.csv(data_location, header = True, inferSchema = True)
    # df.show(truncate = False)
    return df

@pytest.fixture(scope='module')
def fixture_get_current_denver_time():
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    return datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)

@pytest.fixture(scope='module')
def fixture_initialize_boto3_client():
    aws_service = "sns"
    aws_client = initialize_boto3_client(aws_service)
    return aws_client

@pytest.fixture(scope='module')
def fixture_initialize_boto3_resource():
    aws_service = "s3"
    aws_resource = initialize_boto3_resource(aws_service)
    return aws_resource

@pytest.fixture(scope='module')
def fixture_rename_bucket_df(fixture_second_df):
    rename_cols = {"size": "b_size","path": "b_path"}
    renamed_df = rename_columns(fixture_second_df, **rename_cols)
    return renamed_df

@pytest.fixture(scope='module')
def fixture_get_match_objects(fixture_file_to_df, fixture_rename_bucket_df):
    match_df = get_match_objects(fixture_file_to_df, fixture_rename_bucket_df,
                            "path", "b_path",{"df_1": {"path", "size"}, 
							  "df_2": {"b_path", "b_size", "date"}})
    return match_df
