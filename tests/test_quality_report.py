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

def test_init(qr):
    """
    Method to test __init__ method of QualityReport class.
    """
    assert qr.table_name in qr.data_filepath

def test_get_aws_account_name(qr):
    """
    Method to test get_aws_account_name method.
    """
    assert not isinstance(qr.get_aws_account_name(), type(None))

@pytest.mark.parametrize('validation',[5,1])
def test_category_message_true(qr, validation):
    """
    Method to test that category_message method returns validation
    category and message from existing validation ID.
    """
    assert not isinstance(qr.category_message(validation)[0], type(None))

@pytest.mark.parametrize('validation',['integer', 100])
def test_category_message(qr, validation):
    """
    Method to test that category_message method retuns None
    from non-existing validation ID.
    """
    assert isinstance(qr.category_message(validation)[0], type(None))

def test_table_validation_results_true(qr):
    """
    Method to test that table_validation_results method retruns
    a dataframe for valid input.
    """
    assert isinstance(qr.table_validation_results(qr.validate_data_columns), pd.DataFrame),\
    'Method does not return dataframe with valid table function as input.'

def test_table_validation_results_false(qr):
    """
    Method to test that table_validation_results method
does not retrun a dataframe for invalid input.
    """
    assert isinstance(qr.table_validation_results('abcd'), type(None)),\
    'Method does not return None with invalid table function as input.'

@pytest.mark.parametrize(['datatype','function'],
     [('numeric','qr_numeric_check'),('integer','qr_integer_check')])
def test_column_validation_result_valid_input(qr, request, datatype, function,
                    datatype_dictionary, dataframe_with_row_id):
    """
    Method to test that column_validation_results method returns a
    dataframe for valid input.
    """
    datatype_df = qr.separate_df_by_datatype(dataframe_with_row_id, datatype_dictionary, datatype)
    assert isinstance(qr.column_validation_results(datatype_df, request.getfixturevalue(function)),\
            pd.DataFrame), 'Method does not return dataframe with valid datatype function as input.'

def test_column_validation_result_invalid_input(qr,
                    datatype_dictionary, dataframe_with_row_id):
    """
    Method to test that column_validation_results method returns a
    dataframe for invalid input.
    """
    datatype_df = qr.separate_df_by_datatype(dataframe_with_row_id, datatype_dictionary, 'numeric')
    assert isinstance(qr.column_validation_results(datatype_df, 'foo-bar'), type(None)),\
    'Method does not return None with non-existent datatype function as input.'

@pytest.mark.parametrize(['df1','df2'], [(pd.DataFrame(), pd.DataFrame(np.array([[1, 2, 3],
            [4, 5, 6], [7, 8, 9]]),columns=['a', 'b', 'c'])), (None, pd.DataFrame())])
def test_add_to_report_dataframe(qr, df1, df2):
    """
    Method to test add_to_report_dataframe method.
    """
    assert isinstance(qr.add_to_report_dataframe(df1, df2), pd.DataFrame)

def test_save_report_to_s3(qr):
    """
    Method to test save_report_to_s3 method.
    """
    df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),columns=['a', 'b', 'c'])
    now = datetime.now(timezone('US/Mountain')).strftime("%Y-%m-%d")
    client = boto3.client('s3')
    key = f'qualityreport/testing/test_data_{now}.csv'
    qr.save_report_to_s3(df)
    try:
        client.head_object(Bucket='metadata-graphdb', Key=key)
    except ClientError as error:
        if error:
            assert False, 'Reading file from S3 was unsuccessful.'
