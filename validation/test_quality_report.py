"""
Module to test methods from Quality Report class.
"""
from datetime import datetime
import pytest
import boto3
import pandas as pd
import numpy as np
from pytz import timezone
from botocore.errorfactory import ClientError
from quality_report import QualityReport

qr = QualityReport(data_filepath = 's3a://metadata-graphdb/testing/data_quality/test_data.csv',
            metadata_filepath = 's3a://metadata-graphdb/testing/data_quality/test_metadata.csv',
            vendor_name = 'testing', bucket_name = 'metadata-graphdb')

def test_init():
    """
    Method to test __init__ method.
    """
    assert qr.table_name in qr.data_filepath

def test_get_aws_account_name():
    """
    Method to test get_aws_account_name method.
    """
    assert not isinstance(qr.get_aws_account_name(), type(None))

@pytest.mark.parametrize(['valid','invalid'],[(5,'integer'), (1, 100)])
def test_category_message(valid, invalid):
    """
    Method to test category_message method.
    """
    assert len(qr.category_message(valid)) == 2
    assert len(qr.category_message(invalid)) == 2
    assert not isinstance(qr.category_message(valid)[0], type(None))
    assert isinstance(qr.category_message(invalid)[0], type(None))

def test_table_validation_results():
    """
    Method to test table_validation_results method.
    """
    assert isinstance(qr.table_validation_results(qr.validate_data_columns), pd.DataFrame)
    assert isinstance(qr.table_validation_results('abcd'), type(None))

@pytest.fixture
def dataframe_with_row_id():
    """
    Method to create fixture that adds ROW_ID column to a dataframe and
    returns the dataframe.
    """
    return qr.assign_row_id(qr.data_df)

@pytest.fixture
def columns_in_both():
    """
    Method to create fixture that returns list of columns in both
    data and metadata.
    """
    return qr.validate_columns()

@pytest.fixture
def datatype_dictionary(columns_in_both):
    """
    Method to create fixture that returns dictionary of datatypes
    and correspinding columns based on metadata.
    """
    return qr.separate_columns_by_datatype(columns_in_both)

@pytest.mark.parametrize(['datatype','valid_function','invalid_function'],
     [('numeric',qr.numeric_check, 'abcd'),
      ('integer',qr.integer_check, qr.validate_data_columns)])
def test_column_validation_result(datatype, valid_function, invalid_function,
                    datatype_dictionary, dataframe_with_row_id):
    """
    Method to test column_validation_results.
    """
    datatype_df = qr.separate_df_by_datatype(dataframe_with_row_id, datatype_dictionary, datatype)
    assert isinstance(qr.column_validation_results(datatype_df, valid_function), pd.DataFrame)
    assert isinstance(qr.column_validation_results(datatype_df, invalid_function), type(None))

@pytest.mark.parametrize(['df1','df2'], [(pd.DataFrame(), pd.DataFrame(np.array([[1, 2, 3],
                    [4, 5, 6], [7, 8, 9]]),columns=['a', 'b', 'c'])), (None, pd.DataFrame())])
def test_add_to_report_dataframe(df1, df2):
    """
    Method to test add_to_report_dataframe method.
    """
    assert isinstance(qr.add_to_report_dataframe(df1, df2), pd.DataFrame)

def test_save_report_to_s3():
    """
    Method to test save_report_to_s3 method.
    """
    df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),columns=['a', 'b', 'c'])
    now = datetime.now(timezone('US/Mountain')).strftime("%Y-%m-%d")
    client = boto3.client('s3')
    key = f'QualityReport/Testing/test_data_{now}.csv'
    qr.save_report_to_s3(df)
    try:
        client.head_object(Bucket='metadata-graphdb', Key=key)
    except ClientError as error:
        print(error)
        if error:
            assert False, 'Reading file from S3 was unsuccessful.'
