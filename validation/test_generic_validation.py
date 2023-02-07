"""
Module to test methods from Generic Validation class.
"""
import pytest
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list
from data_validation import DatatypeValidation

dv = DatatypeValidation(data_filepath = 's3a://metadata-graphdb/testing/data_quality/test_data.csv',
            metadata_filepath = 's3a://metadata-graphdb/testing/data_quality/test_metadata.csv',
            vendor_name = 'testing', bucket_name = 'metadata-graphdb')

def test_init():
    """
    Method to test __init__ method.
    """
    assert isinstance(dv.data_df, DataFrame)
    assert isinstance(dv.metadata_df, pd.DataFrame)

def test_validate_data_columns():
    """
    Method to test validate_data_columns method.
    """
    expected_columns = ['INT_GER', 'WRONG', 'SHORTS', 'STRINGS']
    expected_validation = 1
    actual_columns, actual_validation = dv.validate_data_columns()
    assert actual_columns == expected_columns
    assert actual_validation == expected_validation

def test_validate_metadata_columns():
    """
    Method to test validate_metadata_columns method.
    """
    expected_columns = ['DUMMY','NON_EXISTENT']
    expected_validation = 2
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
    expected_list = list(range(1, length + 1))
    assert 'ROW_ID' in actual_df.columns
    assert actual_list == expected_list

@pytest.fixture
def dataframe_with_row_id():
    """
    Method to create fixture that adds ROW_ID column to a dataframe and
    returns the dataframe.
    """
    return dv.assign_row_id(dv.data_df)

@pytest.mark.parametrize(['expected_column', 'expected_fail_row_id'],\
            [('Blank', list(range(1,20))),('double',[3,10])])
def test_null_check(expected_column, expected_fail_row_id, dataframe_with_row_id):
    """
    Method to test null_check method.
    """
    expected_validation = 3
    actual_validation, actual_column, actual_fail_row_id = dv.null_check(dataframe_with_row_id, \
                                                                            expected_column)
    assert actual_validation == expected_validation
    assert actual_column == expected_column
    assert actual_fail_row_id == expected_fail_row_id


@pytest.fixture
def columns_in_both():
    """
    Method to create fixture that returns list of columns in both
    data and metadata.
    """
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
    """
    Method to create fixture that returns dictionary of datatypes
    and correspinding columns based on metadata.
    """
    return dv.separate_columns_by_datatype(columns_in_both)

@pytest.mark.parametrize(['datatype', 'expected_columns'], [('integer', ['INTEGER','BLANK',\
                          'ROW_ID']),('string',['STRING','STRING_NAME','ROW_ID'])])
def test_separate_df_by_datatypes(datatype, expected_columns, datatype_dictionary, \
                                            dataframe_with_row_id):
    """
    Module to test separate_df_by_datatypes method.
    """
    actual_df = dv.separate_df_by_datatype(dataframe_with_row_id, datatype_dictionary, datatype)
    expected_df = dataframe_with_row_id.select(*expected_columns)

    #Test schema
    assert actual_df.schema == expected_df.schema
    #Test data
    assert set(actual_df.collect()) == set(expected_df.collect())

@pytest.mark.parametrize(['valid','invalid'],[('integer','foo_bar'),('float','dummy'),
                                    ('long','random')])
def test_datatype_validation_functions(valid, invalid):
    """
    Method to test datatype_validation_functions method.
    """
    assert callable(dv.datatype_validation_functions(valid))
    assert not callable(dv.datatype_validation_functions(invalid))
