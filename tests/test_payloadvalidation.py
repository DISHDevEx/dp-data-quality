"""
Module to test methods from PayloadValidation class.
"""
import pytest
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list
import pyspark.sql.functions as F
from dp_data_quality import PayloadValidation


bucket_folder_path_standard ='metadata-graphdb/ProcessValidation'
bucket_folder_path_input ='metadata-graphdb/ProcessValidation/input'
bucket_name = "metadata-graphdb"
standard_file = "standard.json"
input_file = "test1.json"


@pytest.fixture
def pv():
    return PayloadValidation()

def test_create_spark_session(pv):
    """
    Method to test that spark variable is created.
    """
    assert not isinstance(pv.create_spark_session(), type(None))

def test_read_s3_json_to_df_input_file(pv):
    """
    Method to test that input json_to_df variable is a Pyspark dataframe or not.
    """
    assert isinstance(pv.read_s3_json_to_df(input_file, bucket_folder_path_input), DataFrame)
    
def test_read_s3_json_to_standard_file(pv):
    """
    Method to test standard json_to_df variable is a Pyspark dataframe or not.
    """
    assert isinstance(pv.read_s3_json_to_df(standard_file, bucket_folder_path_standard), DataFrame)
    

def test_flatten_standard_df(pv):
    """
    Method to test standard flatten dataframe.
    """

    df_standard_format = pv.read_s3_json_to_df(standard_file, bucket_folder_path_standard)

    for i in df_standard_format.columns:
        df_standard_format = df_standard_format.withColumn(
            i, F.explode(i))

    df_standard_flattened = df_standard_format.select(pv.flatten_df(df_standard_format.schema))

    assert isinstance(df_standard_flattened, DataFrame)
        
def test_flatten_input_df(pv):
    """
    Method to test input flatten dataframe.
    """
        
    df_input_format = pv.read_s3_json_to_df(input_file, bucket_folder_path_input)

    for i in df_input_format.columns:
        df_input_format = df_input_format.withColumn(i, F.explode(i))

    df_input_flattened = df_input_format.select(pv.flatten_df(df_input_format.schema))

    assert isinstance(df_input_flattened, DataFrame)
        
def test_schema_check_report_to_s3(pv):
    """
    Method to test both standard and input files and store the missing values to s3 report or not
    """
    
    df_standard_format = pv.read_s3_json_to_df(standard_file, bucket_folder_path_standard)
    df_input_format = pv.read_s3_json_to_df(input_file, bucket_folder_path_input)

    result = pv.schema_check_report_to_s3(
        df_standard_format,
        df_input_format,
        bucket_name,
        input_file
    )

    assert isinstance(result, type(None))      