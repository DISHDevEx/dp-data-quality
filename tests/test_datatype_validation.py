'''
This module tests the DatatypeRulebook in the validation folder
'''
from validation import DatatypeRulebook
from validation import spark_setup
import pandas as pd

spark = spark_setup()
rulebook = DatatypeRulebook(
    's3a://metadata-graphdb/testing/data_quality/test_data.csv',
    's3a://metadata-graphdb/testing/data_quality/test_metadata.csv'
)

def test_integer_check():
    '''Tests the integer datatype, testdata is created within this method'''
    integer_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'-1'],
         [1,'2'],
         [2,''],
         [3,'-2147483649'],
         [4,'2147483648'],
         [5,'2e8'],[6,'1.0']], columns=['ROW_ID','integers']))
    integer_column = 'integers'
    actual = rulebook.integer_check(integer_data_df,integer_column)
    expected = 5, 'integers',[2,3,4,5]
    assert actual == expected
    