
"""
Pytest file to test s3_to_s3_validation_script.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""

from datetime import datetime
import pytest
import pytz
import os
from dotenv import load_dotenv

from dp_data_quality import DatatypeRulebook
from dp_data_quality import QualityReport

load_dotenv()

@pytest.fixture(scope='module')
def data_filepath():
    return 's3a://' + os.environ.get('BUCKET_NAME') + '/' + os.environ.get('DATA_FILE_PATH')

@pytest.fixture(scope='module')
def metadata_filepath():
    return 's3a://' + os.environ.get('BUCKET_NAME') + '/' + os.environ.get('METADATA_FILE_PATH')

@pytest.fixture(scope='module')
def vendor_name():
    return 'testing'

@pytest.fixture(scope='module')
def bucket_name():
    return os.environ.get('BUCKET_NAME')

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




