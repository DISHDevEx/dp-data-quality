"""
!/usr/bin/env python3  
#--------------------------------------------------------------------
# File    :   glue-catalog-valiation
# Time    :   2022/12/13 10:59:01
# Author  :   Zheng Liu
# Version :   1.0
# Desc    :   read tables from Glue Database and compare with object in asoociated S3 bucket to do validation.

---------------------------Version History---------------------------
SrNo    DateModified    ModifiedBy   Description
1       2022/12/13      Zheng        Initial Version
2       2022/12/13      Zheng        S3: d.use1.dish-boost.cxo.obf.g
3       2022/12/14      Zheng        Tested on other databases
4       2022/12/14      Zheng        Scan top level folders in S3 bucket and complete validation by comparing glue tables'names and s3 top level folders'name
5       2022/12/14      Zheng        Replace all string.punctuation from S3 folders' names into underscore
6       2022/12/27      Zheng        Add docstring in function, add path for result S3 bucket, add SNS, catch exceptions
7       2022/12/28      Zheng        Add print to troubleshoot, if result cannot be sent to SNS
8       2023/01/03      Zheng        Validate tables under a database appear in related S3 bucket as folders' names
9       2023/01/06      Zheng        Change the arguements passed from Glue Job name


#--------------------------------------------------------------------
"""

import string
import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

import boto3
from datetime import datetime, timezone,timedelta
import time
import pytz
import pprint

# Get local time
DEN = pytz.timezone('US/Mountain')
datetime_den = datetime.now(DEN)
current = datetime_den.strftime('%Y-%m-%d_%H-%M-%S_%Z_%z')
print(f'current time: {current}')

# Read stackname to use later in result_path and SNS name:
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']
stack_name = job_name

# Read target Glue Calalog Database's tables and compare with associated S3 bucket first layer folders' name. (this database has same name as this S3 bucket)

# 5 parameters: 
# database_name (should be same as S3 bucket name):
database_name = stack_name.split('---')[1].replace('--','.')
s3bucketname = database_name

# snsname = "dish.vendor.glue.catalog.validation.sns.demo" sns cannot use dot, so have to replace them with underscore
snsname = stack_name.split('---')[0]+'---gluevalidation'

# result S3 bucket and path name, this bucket must be set up already:
# result_bucket = f's3://stack_name.split('---')[0]+'---gluevalidation'' should follow S3 naming convention.
result_bucket = snsname
result_key = f'test-result/{database_name}-{current}.json'

print(f'database_name: {database_name}')
print(f's3bucketname: {s3bucketname}')
print(f'snsname: {snsname}')
print(f'result_bucket: {result_bucket}')
print(f'result_key: {result_key}')

# Function to replace punctuations with underscore
def remove_punctuation(astring):
    """
    replace all punctuation with underscore, because Glue Catalog will do the same by itself
    EXAMPLE:
    input = ,./<>?;':"[]{}\|!@#$%^&*()-=+`~
    output = _______________________________
    """
    for char in string.punctuation:
        astring = astring.replace(char, '_')
    return astring

# Get tables from target Glue Catalog Database
glue_client = boto3.client('glue')

# The output will be saved as json format
json_dict={}

# Read the tables in the database
try:
    response = glue_client.get_tables(
        DatabaseName = database_name
        )
        # return format:
    # Name:  response['TableList'][iterator]['Name']
    # Location: response['TableList'][iterator]['StorageDescriptor']['Location']

    count = 0
    table_list = response['TableList']


    # Create a list of tables' names from glue database: glue_table_names.
    glue_table_names = []
    for item in table_list:
        count+=1
        theName = item['Name']
        theLocation = item['StorageDescriptor']['Location']
        print(f'Table {count} Name: {theName}')
        print(f'Table {count} Location: {theLocation}')
        glue_table_names.append(theName)

    # If table names end with /, we should remove /
    glue_table_names_noslash = []
    for item in glue_table_names:
        glue_table_names_noslash.append(item.replace("/",""))
    glue_table_names = glue_table_names_noslash

    print('glue_table_names')
    print(glue_table_names)
except:
    json_dict['response from Glut Catalog Database'] = 'failed'
    print(json_dict)

# Scan target S3's folders
try:
    # scan only top level
    s3_client = boto3.client('s3')
    s3_paginator = s3_client.get_paginator('list_objects')
    s3_result = s3_paginator.paginate(Bucket=s3bucketname, Delimiter='/')

    # Create a list of top level folders in s3 bucket: s3_prefix_list.
    s3_prefix_list = []
    for s3_prefix in s3_result.search('CommonPrefixes'):
        s3_prefix_list.append(s3_prefix.get('Prefix'))
        
    s3_prefix_list_noslash = []
    for item in s3_prefix_list:
        # replace all punctuations with underscore, convert upper case to lower case and remove forward slash.
        s3_prefix_list_noslash.append(remove_punctuation(item.lower().replace("/","")))
    s3_prefix_list = s3_prefix_list_noslash

    print('s3_prefix_list')
    print(s3_prefix_list)


except:
    json_dict['scanning result of target S3'] = 'failed'
    print(json_dict)

try:
    # A list holds items in glue_table_names but not in s3_prefix_list: missing_at_s3.
    missing_in_s3 = []
    for item in glue_table_names:
        if item not in s3_prefix_list:
            missing_in_s3.append(item)

    # A list holds items in s3_prefix_list but not in glue_table_names: missing_at_glue.
    missing_in_glue_database = []
    for item in s3_prefix_list:
        if item not in glue_table_names:
            missing_in_glue_database.append(item)
            
    print(f'missing_in_s3: {missing_in_s3}')
    print(f'missing_in_glue_database: {missing_in_glue_database}')

    # Store result in the result S3 bucket with the Key:
    json_dict = {'missing_in_s3':missing_in_s3, 'missing_in_glue_database':missing_in_glue_database}
    json_ob = json.dumps(json_dict, indent=2)
    print(json_ob)
    s3_result_client = boto3.client('s3')
    s3_result_client.put_object(Body = json_ob, Bucket = result_bucket, Key = result_key)

except:
    json_dict['validation result'] = 'can not be generated'
    print(json_dict)


try:
    # Notice user the result by SNS:
    snsclient = boto3.client('sns')
    snstopicarn = [tp['TopicArn'] for tp in snsclient.list_topics()['Topics'] if snsname in tp['TopicArn']][0]
    print(f"sns topic arn is {snstopicarn}")
    response = snsclient.publish(
            TargetArn=snstopicarn,
            Message=json.dumps({'default': json.dumps(json_dict, indent = 2)}),
            Subject='An AWS Glue Catalog Validation result today',
            MessageStructure='json')
    print("response:")
    print(response)
except:
    print('message cannot be sent out to desired SNS topic')
print('End of the validation code.')