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

def test_short_check():
    '''Tests the short datatype, testdata is created within this method'''
    short_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'32768'],
         [1,'-32769'],
         [2,'1'],
         [3,'1.0'],
         [4,'2e8']], columns= ['ROW_ID','shorts']))
    short_column = 'shorts'
    actual = rulebook.short_check(short_data_df,short_column)
    expected = 6,'shorts',[0,1,4]
    assert actual == expected

def test_long_check():
    '''Tests the long datatype, testdata is created within this method'''
    long_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'9223372036854775809'],
         [1,'-9223372036854775809'],
         [2,'1'],
         [3,'1.0'],[4,'2e8']], columns = ['ROW_ID','longs']))
    long_column = 'longs'
    actual = rulebook.long_check(long_data_df,long_column)
    expected = 7, 'longs', [0,1,4]
    assert actual == expected

def test_float_check():
    '''Tests the float datatype, testdata is created within this method'''
    float_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'1.175494352e-38'],
         [1,'3.402823467e38'],
         [2,'-1.175494351e-38'],
         [3,'1.0'],
         [4,'-3.402823466e38'],
         [5,'0.0']], columns = ['ROW_ID','floats']))
    float_column = 'floats'
    actual = rulebook.float_check(float_data_df,float_column)
    expected = 8, 'floats', [0,1,2,4]
    assert actual == expected

def test_double_check():
    '''Tests the double datatype, testdata is created within this method'''
    double_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'9223372036854775809'],
         [1,'-9223372036854775809'],
         [2,'1'],
         [3,'1.0'],[4,'2e8']], columns = ['ROW_ID','doubles']))
    double_column = 'doubles'
    actual = rulebook.double_check(double_data_df,double_column)
    expected = 9, 'doubles', [1]
    assert actual == expected

def test_string_check():
    '''Tests the string datatype, testdata is created within this method'''
    string_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'monkey'],
         [1,'stringy string']], columns=['ROW_ID','String']))
    string_column = 'string'
    actual = rulebook.string_check(string_data_df, string_column)
    expected = 10,'string',[]
    assert actual == expected

def test_varchar_check():
    '''Tests the varchar datatype, testdata is created within this method'''
    varchar_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'monkey'],
         [1,'stringy string']], columns=['ROW_ID','VARCHAR']))
    varchar_column = 'varchar'
    actual = rulebook.varchar_check(varchar_data_df, varchar_column)
    expected = 11,'varchar',[0,1]
    assert actual == expected
