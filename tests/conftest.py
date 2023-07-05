
"""
Pytest file to test s3_to_s3_validation_script.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""

from datetime import datetime
import pytest
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from s3_to_s3_validation_script import (rename_columns, initialize_boto3_client,initialize_boto3_resource, get_match_objects)
from data_quality_validation import DatatypeRulebook
from data_quality_validation import QualityReport

@pytest.fixture(scope='module')
def data_filepath():
    return 's3a://metadata-graphdb/testing/data_quality/test_data.csv'

@pytest.fixture(scope='module')
def metadata_filepath():
    return 's3a://metadata-graphdb/testing/data_quality/test_metadata.csv'

@pytest.fixture(scope='module')
def vendor_name():
    return 'testing'

@pytest.fixture(scope='module')
def bucket_name():
    return 'metadata-graphdb'

@pytest.fixture(scope='module')
def dr(data_filepath, metadata_filepath):
    return DatatypeRulebook(data_filepath, metadata_filepath)

@pytest.fixture(scope='module')
def qr(data_filepath, metadata_filepath, vendor_name, bucket_name):
    return QualityReport(data_filepath, metadata_filepath, vendor_name, bucket_name)

@pytest.fixture(scope='module')
def dataframe_with_row_id(qr):
    return qr.assign_row_id(qr.data_df)

@pytest.fixture(scope='module')
def columns_in_both(qr):
    return qr.validate_columns()

@pytest.fixture(scope='module')
def datatype_dictionary(qr, columns_in_both):
    return qr.separate_columns_by_datatype(columns_in_both)

@pytest.fixture(scope='module')
def qr_numeric_check(qr):
    return qr.numeric_check

@pytest.fixture(scope='module')
def qr_integer_check(qr):
    return qr.integer_check




@pytest.fixture(scope='module')
def fixture_empty_dataframe():
    """
    Return: an empty pyspark dataframe
    """
    # Create a spark session
    spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
    # Create an empty RDD
    emp_rdd = spark.sparkContext.emptyRDD()
    # Create an expected schema
    columns = StructType([StructField('Path',
                                      StringType(), True),
                        StructField('Size',
                                    StringType(), True),
                        StructField('Date',
                                    StringType(), True)])
    # Create an empty RDD with expected schema
    pyspark_df = spark.createDataFrame(data = emp_rdd,
                               schema = columns)
    return pyspark_df

@pytest.fixture(scope='module')
def fixture_setup_spark():
    """
    Return: spark
    """
    packages = (",".join(["io.delta:delta-core_2.12:2.2.0",
                          "org.apache.hadoop:hadoop-aws:3.3.4"]))
    spark = (SparkSession
        .builder
        .appName("PySparkApp")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("fs.s3a.aws.credentials.provider",
                'com.amazonaws.auth.ContainerCredentialsProvider')
    ).getOrCreate()
    return spark

@pytest.fixture(scope='module')
def fixture_file_to_df(fixture_setup_spark):
    """
    Return: a pyspark dataframe
    """
    bucket = "s3-validation-demo"
    data_key = "test/s3_to_s3_validation.csv"
    data_location = f"s3a://{bucket}/{data_key}"
    schema_str = 'id long, path string, size long'
    pyspark_df = fixture_setup_spark.read.csv(data_location, header = True, schema = schema_str)
    return pyspark_df

@pytest.fixture(scope='module')
def fixture_second_df(fixture_setup_spark):
    """
    Return: another pyspark dataframe
    """
    bucket = "s3-validation-demo"
    data_key = "test/s3_to_s3_validation_second.csv"
    data_location = f"s3a://{bucket}/{data_key}"
    pyspark_df = fixture_setup_spark.read.csv(data_location, header = True, inferSchema = True)
    return pyspark_df

@pytest.fixture(scope='module')
def fixture_second_df_renamed(fixture_setup_spark):
    """
    Return: another pyspark dataframe with renamed columns
    """
    bucket = "s3-validation-demo"
    data_key = "test/s3_to_s3_validation_second.csv"
    data_location = f"s3a://{bucket}/{data_key}"
    initil_df = fixture_setup_spark.read.csv(data_location, header = True, inferSchema = True)
    rename_cols = {"size": "b_size","path": "b_path"}
    second_df_renamed = rename_columns(initil_df, **rename_cols)
    return second_df_renamed

@pytest.fixture(scope='module')
def fixture_get_current_denver_time():
    """
    Return: current Denver local time
    """
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    return datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)

@pytest.fixture(scope='module')
def fixture_initialize_boto3_client():
    """
    Return: a aws sns client
    """
    aws_service = "sns"
    aws_client = initialize_boto3_client(aws_service)
    return aws_client

@pytest.fixture(scope='module')
def fixture_initialize_boto3_resource():
    """
    Return: a aws s3 client
    """
    aws_service = "s3"
    aws_resource = initialize_boto3_resource(aws_service)
    return aws_resource

@pytest.fixture(scope='module')
def fixture_get_match_objects(fixture_file_to_df, fixture_second_df_renamed):
    """
    Return: inner joined dataframe based on criteria from two dataframes.
    """
    match_df = get_match_objects(fixture_file_to_df, fixture_second_df_renamed,
                            "path", "b_path",{"df_1": {"path", "size"},
                              "df_2": {"b_path", "b_size", "date"}})
    return match_df

@pytest.fixture(scope='module')
def fixture_schema():
    """
    Return: a pyspark dataframe schema
    """
    schema = (StructType()
      .add("id",LongType(),True)
      .add("path",StringType(),True)
      .add("size",LongType(),True))
    return schema
