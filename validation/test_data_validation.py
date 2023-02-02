"""
Module to test methods from Generic Validation class.
"""
from pyspark.sql import DataFrame
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal, assert_frame_equal
from pyspark.sql.functions import collect_list
import pytest
from data_validation import DatatypeValidation

dv = DatatypeValidation(data_filepath = 's3a://metadata-graphdb/Testing/Data_Quality/test_data.csv',
                metadata_filepath = 's3a://metadata-graphdb/Testing/Data_Quality/test_metadata.csv', 
                report_filepath = 's3a://metadata-graphdb/Testing/Data_Quality/test_quality_report.csv', 
                bucket_name = 'metadata-graphdb')

def test_init():
    """
    Method to test __init__ method.
    """
    assert isinstance(dv.data_df, DataFrame) == True
    assert isinstance(dv.metadata_df, pd.DataFrame) == True
    
def test_validate_data_columns():
    """
    Method to test validate_data_columns method.
    """
    expected_columns = ['INT_GER', 'WRONG', 'SHORTS', 'STRINGS']
    expected_validation = 'COLUMN NOT IN METADATA'
    actual_columns, actual_validation = dv.validate_data_columns()
    assert actual_columns == expected_columns
    assert actual_validation == expected_validation
    
def test_validate_metadata_columns():
    """
    Method to test validate_metadata_columns method.
    """
    expected_columns = ['DUMMY','NON_EXISTENT']
    expected_validation = 'COLUMN NOT IN DATA'
    actual_columns, actual_validation = dv.validate_metadata_columns()
    assert actual_columns == expected_columns
    assert actual_validation == expected_validation
    
def test_validate_columns():
    """
    Method to test validate_columns method.
    """
    expected_columns_in_both = ['INTEGER','SHORT','LONG','FLOAT','DOUBLE',
                    'STRING','STRING_NAME','NUMERIC','VARCHAR', 'BLANK']
    actual_columns_in_both = dv.validate_columns()
    assert actual_columns_in_both == expected_columns_in_both
    
def test_assign_row_id():
    """
    Method to test assign_row_id method.
    """
    actual_df = dv.assign_row_id(dv.data_df)
    length = actual_df.count()
    actual_list = actual_df.select(collect_list('ROW_ID')).first()[0]
    expected_list = [i for i in range(1,length+1)]
    assert 'ROW_ID' in actual_df.columns
    assert actual_list == expected_list

@pytest.fixture
def dataframe_with_row_id():
    return dv.assign_row_id(dv.data_df)

def test_null_check(dataframe_with_row_id):
    """
    Method to test null_check method. 
    """
    expected_validation = 'NULL'
    expected_column = 'Blank'
    expected_fail_row_id = list(range(1,20))
    actual_validation, actual_column, actual_fail_row_id = dv.null_check(dataframe_with_row_id, expected_column)
    assert actual_validation == expected_validation
    assert actual_column == expected_column
    assert actual_fail_row_id == expected_fail_row_id
    expected_validation = 'NULL'
    expected_column = 'double'
    expected_fail_row_id = [3,10]
    actual_validation, actual_column, actual_fail_row_id = dv.null_check(dv.assign_row_id(dv.data_df), expected_column)
    assert actual_validation == expected_validation
    assert actual_column == expected_column
    assert actual_fail_row_id == expected_fail_row_id

@pytest.fixture
def columns_in_both():
    return dv.validate_columns()

def test_separate_columns_by_datatypes(columns_in_both):
    """
    Method to test separate_columns_by_datatype method.
    """
    actual_dictionary = dv.separate_columns_by_datatype(columns_in_both)
    
    wrong_dictionary = {'integer' : ['INTEGER', 'BLANK', 'DUMMY'],
             'short': ['SHORT'], 'long': ['LONG'],'float': ['FLOAT'],
             'double': ['DOUBLE'],'string':['STRING','STRING_NAME',
            'NON_EXISTENT'],'numeric': ['NUMERIC'],'varchar': ['VARCHAR']}
    
    expected_dictionary =  {'integer' : ['INTEGER', 'BLANK'],
             'short': ['SHORT'], 'long': ['LONG'],'float': ['FLOAT'],
             'double': ['DOUBLE'],'string':['STRING','STRING_NAME']
                ,'numeric': ['NUMERIC'],'varchar': ['VARCHAR']}
    
    assert actual_dictionary == expected_dictionary
    assert actual_dictionary != wrong_dictionary

@pytest.fixture
def datatype_dictionary(columns_in_both):
    return dv.separate_columns_by_datatype(columns_in_both)

def test_separate_df_by_datatypes(datatype_dictionary, dataframe_with_row_id):
    """
    Module to test separate_df_by_datatypes method.
    """
    actual_df = dv.separate_df_by_datatype(dataframe_with_row_id, datatype_dictionary, 'integer')
    expected_df = dataframe_with_row_id.select('INTEGER','BLANK','ROW_ID')
    #Compare schema
    assert actual_df.schema == expected_df.schema
    #Compare data
    assert set(actual_df.collect()) == set(expected_df.collect())
    
    actual_df = dv.separate_df_by_datatype(dataframe_with_row_id, datatype_dictionary, 'string')
    expected_df = dataframe_with_row_id.select('STRING','STRING_NAME','ROW_ID')
    #Compare schema
    assert actual_df.schema == expected_df.schema
    # Compare data
    assert set(actual_df.collect()) == set(expected_df.collect())
