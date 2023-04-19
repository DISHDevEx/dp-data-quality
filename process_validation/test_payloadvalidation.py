"""
Module to test methods from PayloadValidation class.
"""
import pytest
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list
from payloadvalidation import PayloadValidation

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