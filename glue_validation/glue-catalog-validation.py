"""
Glue Validation Python Script
"""

import string
import sys
import json

from datetime import datetime
import boto3
from botocore.client import ClientError
from botocore.exceptions import ConnectionClosedError
import pytz
from pytz.exceptions import UnknownTimeZoneError
from awsglue.utils import getResolvedOptions




# Function to replace punctuations with underscore
def remove_punctuation(astring):
    """
    Replace all punctuation with underscore, because Glue Catalog will do the same by itself
    EXAMPLE:
    input - ,./<>?;':"[]{}\|!@#$%^&*()-=+`~
    output = _______________________________
    """
    for char in string.punctuation:
        astring = astring.replace(char, '_')
    return astring

def get_glue_database_name():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']
    stack_name = job_name
    database_name = stack_name.split('---')[1].replace('--','.')
    return database_name

def glue_database_validation(glue_database_name):
    """
    Function to validated that an glue database exists.

    PARAMETERS:
        glue_database_name -> glue database name

    RETURNS:
        # s3_bucket_info_dict -> s3 bucket info dict (if s3 bucket is valid)
        None -> if any invalid input, permission or connection issue
    """
    if glue_database_name.__class__.__name__ != "Glue":
        print("Not a valid glue database.")
        print('"glue_database_validation" function completed unsuccessfully.')


def bucket_validation(s3_bucket, s3_resource):
    """
    Function to validated that an S3 bucket exists.

    PARAMETERS:
        s3_bucket -> s3 bucket name
        s3_resource -> boto3 s3 resource

    RETURNS:
        s3_bucket_info_dict -> s3 bucket info dict (if s3 bucket is valid)
        None -> if any invalid input, permission or connection issue
    """
    if s3_resource.__class__.__name__ != "s3.ServiceResource":
        print("Not a valid s3 resource.")
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    if not isinstance(s3_bucket, str):
        print("s3_bucket should be a string.")
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    try:
        s3_bucket_info_dict = s3_resource.meta.client.head_bucket(Bucket=s3_bucket)
    except ClientError as err:
        print(err)
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    except ConnectionClosedError as err:
        print(err)
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    else:
        print('"bucket_validation" function completed successfully.')
        return s3_bucket_info_dict

def get_current_denver_time(time_zone, time_format):
    """
    Function to get current denver local time.

    PARAMETERS:
        time_zone -> time zone argument, such as US/Mountain
        time_format -> time format, such as %Y%m%d_%H%M%S_%Z_%z

    RETURNS:
        cannot_get_timestamp -> an error string if time_zone is invalid or invalid input
        current -> a time string
    """
    if not isinstance(time_zone,str) or not isinstance(time_format, str):
        print('time_zone and time_format must be string.')
        print('"get_current_denver_time" function completed unsuccessfully.')
        return 'cannot_get_timestamp'
    try:
        denver_time = pytz.timezone(time_zone)
    except UnknownTimeZoneError as err:
        print(err)
        print('"get_current_denver_time" function completed unsuccessfully.')
        return 'cannot_get_timestamp'
    else:
        datetime_den = datetime.now(denver_time)
        current = datetime_den.strftime(time_format)
        print(f'Current local time is {current}.')
        print('"get_current_denver_time" function completed successfully.')
        return current

# Get local time
DEN = pytz.timezone('US/Mountain')
datetime_den = datetime.now(DEN)
current = datetime_den.strftime('%Y-%m-%d_%H-%M-%S_%Z_%z')
print(f'current time: {current}')

# Read stackname to use later in result_path and SNS name:

s3bucketname = database_name

# snsname = "dish.vendor.glue.catalog.validation.sns.demo" sns cannot use dot, so have to replace them with underscore
snsname = stack_name.split('---')[0]+'---gluevalidation'

# result S3 bucket and path name, this bucket must be set up already:
# result_bucket = f's3://stack_name.split('---')[0]+'---gluevalidation'' should follow S3 naming convention.
result_bucket = snsname
result_key = f'test-result/{database_name}-{current}.json'

print(f'\ndatabase_name: {database_name}')
print(f'\ns3bucketname: {s3bucketname}')
print(f'\nsnsname: {snsname}')
print(f'\nresult_bucket: {result_bucket}')
print(f'\nresult_key: {result_key}')

# Get tables from target Glue Catalog Database
glue_client = boto3.client('glue')

# The output will be saved as json format
json_dict={}

#***************************************#
#*** Read the tables in the database ***#
#***************************************#
# Read the tables in the database
try:
    # Need grant database and table access in lake formation.
    # Otherwise, will return an empty list of tables.
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
        print(f'\nTable {count} Name: {theName}')
        print(f'\nTable {count} Location: {theLocation}')
        glue_table_names.append(theName)

    # If table names end with /, we should remove /
    glue_table_names_noslash = []
    for item in glue_table_names:
        glue_table_names_noslash.append(item.replace("/",""))
    glue_table_names = glue_table_names_noslash

    print('\nglue_table_names')
    print(glue_table_names)
except:
    # If not created in same account.
    glue_client.exceptions.EntityNotFoundException
    json_dict['response from Glue Catalog Database'] = 'failed'
    print(json_dict)

#***************************************#
#***Read the folders in the S3 bucket***#
#***************************************#
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

    print('\ns3_prefix_list')
    print(s3_prefix_list)


except:
    json_dict['scanning result of target S3'] = 'failed'
    print(json_dict)

#***************************************************************************************#
#*** Do comparisons between tables and folders, and save result in designed location ***#
#***************************************************************************************#
try:
    # A set holds items in glue_table_names but not in s3_prefix_list: missing_at_s3.
    missing_in_s3 = set(glue_table_names).difference(set(s3_prefix_list))

    # A set holds items in s3_prefix_list but not in glue_table_names: missing_at_glue.
    missing_in_glue_database = set(s3_prefix_list).difference(set(glue_table_names))

    print(f'\nmissing_in_s3: {missing_in_s3}')
    print(f'\nmissing_in_glue_database: {missing_in_glue_database}')

    # Store result in the result S3 bucket with the Key:
    json_dict = {'missing_in_s3':missing_in_s3, 'missing_in_glue_database':missing_in_glue_database}
    json_ob = json.dumps(json_dict, indent=2)
    print(json_ob)
    s3_result_client = boto3.client('s3')
    s3_result_client.put_object(Body = json_ob, Bucket = result_bucket, Key = result_key)

except:
    json_dict['validation result'] = 'can not be generated'
    print(json_dict)

#*************************************************#
#*** Send validation result to SNS subscribers ***#
#*************************************************#
try:
    # Notice user the result by SNS:
    snsclient = boto3.client('sns')
    snstopicarn = [tp['TopicArn'] for tp in snsclient.list_topics()['Topics'] if snsname in tp['TopicArn']][0]
    print(f"\nsns topic arn is {snstopicarn}")
    response = snsclient.publish(
            TargetArn=snstopicarn,
            Message=json.dumps({'default': json.dumps(json_dict, indent = 2)}),
            Subject='An AWS Glue Catalog Validation result today',
            MessageStructure='json')
    print("\nresponse:")
    print(response)
except:
    print('\nmessage cannot be sent out to desired SNS topic')
print('\nEnd of the validation code.')
