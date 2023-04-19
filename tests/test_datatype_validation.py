'''
This module tests the DatatypeRulebook in the validation folder
'''
from data_quality_validation import DatatypeRulebook
from data_quality_validation import spark_setup
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
         [1,'stringy string'],
         [2,'cat']], columns=['ROW_ID','VARCHAR']))
    varchar_column = 'varchar'
    actual = rulebook.varchar_check(varchar_data_df, varchar_column)
    expected = 11,'varchar',[0,1]
    assert actual == expected
    
def test_ipv4_check():
    '''Tests the ipv4 datatype, testdata is created within this method'''
    ipv4_data_df = spark.createDataFrame(pd.DataFrame(
        [[0,'1 . 2 . 3 . 4'],
         [1,'01 . 102 . 103 . 104']], columns= ['ROW_ID','IPv4addresses']))
    ipv4_column = 'IPv4addresses'
    actual = rulebook.ipv4_check(ipv4_data_df,ipv4_column)
    expected = 12,'IPv4addresses',[0,1]
    assert actual == expected

def test_ipv6_check():
    '''
    Tests the IPv6 datatype, testdata is created within this method
    The following test uses example IPv6 addresses from IBM, source here: 
    https://www.ibm.com/docs/en/ts3500-tape-library?topic=functionality-ipv4-ipv6-address-formats
    '''
    ipv6_data_df = spark.createDataFrame(pd.DataFrame([
        [0,'2001 : db8: 3333 : 4444 : 5555 : 6666 : 7777 : 8888'],
        [1,'2001 : db8 : 3333 : 4444 : CCCC : DDDD : EEEE : FFFF'],
        [2,': :'],[3,'2001: db8: :'],
        [4,': : 1234 : 5678'],
        [5,'2001 : db8: : 1234 : 5678'],
        [6,'2001:0db8:0001:0000:0000:0ab9:C0A8:0102'],
        [7,'2001:db8:1::ab9:C0A8:102']], columns= ['ROW_ID','IPv6addresses']))
    ipv6_column = 'IPv6addresses'
    actual = rulebook.ipv6_check(ipv6_data_df, ipv6_column)
    expected = 13,'IPv6addresses',[0,1,2,3,4,5]
    assert actual == expected

def test_epoch_check():
    '''Tests the epoch datatype.'''
    epoch_data_df = spark.createDataFrame(pd.DataFrame([
        [1,0],
        [2,1675295204357],
        [3,1675295204631],
        [4,1675001470307],
        [6,1680195228],
        [5,1676408186]], columns= ['ROW_ID', 'epochs']))
    epoch_column = 'epochs'
    actual = rulebook.epoch_check(epoch_data_df, epoch_column)
    expected = 14, 'epochs', []
    assert actual == expected

def test_timestamp_check():
    '''Tests the timestamp datatype, testdata is created within this method'''
    timestamp_data_df = spark.createDataFrame(pd.DataFrame(
        [[1,'0'],
         [2,'1970-01-01 00:00:01.000000'],
         [3,'12:1:2000'],
         [4,'12:31:1999'],
         [5,'1985/09/25 17:45:30.005'],
         [6,'1680210134'],
         [7,'25/11/22 06:43:14']], 
        columns= ['ROW_ID','timestamps']))
    timestamp_column = 'timestamps'
    actual = rulebook.timestamp_check(timestamp_data_df,timestamp_column)
    expected = 15, 'timestamps', [1,3,4,6]
    assert actual == expected