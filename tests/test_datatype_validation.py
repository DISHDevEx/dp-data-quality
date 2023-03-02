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

def test_numeric_check():
    """
    Tests the numeric datatype
    """
    numeric_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'-1'],
         [1,'2'],
         [2,''],
         [3,'-2147483649'],
         [4,'2147483648'],
         [5,'2e8'],
         [6, 'panda-bear'],
         [7,'1.0']], columns=['ROW_ID','numeric']))
    numeric_column = 'numeric'
    actual = rulebook.numeric_check(numeric_data_df,numeric_column)
    expected = 4, 'numeric',[2,6]
    assert actual == expected

def test_integer_check():
    """
    Tests the integer datatype
    """
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
    """
    Tests the short datatype
    """
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
    """
    Tests the long datatype
    """
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
    """
    Tests the float datatype
    """
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
    """
    Tests the double datatype
    """
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
    """
    Tests the string datatype
    """
    string_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'monkey'],
         [1,'stringy string']], columns=['ROW_ID','STRING_NAME']))
    string_column = 'STRING_NAME'
    actual = rulebook.string_check(string_data_df, string_column)
    expected = 10,'STRING_NAME',[]
    assert actual == expected

def test_varchar_check():
    """
    Tests the varchar datatype
    """
    varchar_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'monkey'],
         [1,'stringy string']], columns=['ROW_ID','VARCHAR']))
    varchar_column = 'VARCHAR'
    actual = rulebook.varchar_check(varchar_data_df, varchar_column)
    expected = 11,'VARCHAR',[]
    assert actual == expected
