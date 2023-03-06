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

def glue_database_list(glue_database_name):
    """
    Function to validated that an glue database exists.

    PARAMETERS:
        glue_database_name -> glue database name

    RETURNS:
        glue_table_names -> a list of table names from glue database
        None -> if any invalid input, permission or connection issue
    """
    if glue_database_name.__class__.__name__ != "Glue":
        print("Not a valid glue database.")
        print('"glue_database_list" function completed unsuccessfully.')
        return None
    try:
        # Need grant database and table access in lake formation.
        # Otherwise, will return an empty list of tables.
        response = glue_client.get_tables(
            DatabaseName = glue_database_name
            )
        # return format:
        # Name:  response['TableList'][iterator]['Name']
        # Location: response['TableList'][iterator]['StorageDescriptor']['Location']

    except:
        # If not created in same account.
        glue_client.exceptions.EntityNotFoundException
        json_dict['response from Glue Catalog Database'] = 'failed'
        print(json_dict)
        print('"glue_database_list" function completed unsuccessfully.')
        return None
    else:
        glue_table_names = []
        table_count = 0
        table_list = response['TableList']
        if len(table_list) > 0:
            # Create a list of tables' names from glue database: glue_table_names.
            for item in table_list:
                table_count += 1
                the_name = item['Name']
                the_location = item['StorageDescriptor']['Location']
                print(f'\nTable {table_count} Name: {the_name}.')
                print(f'\nTable {table_count} Location: {the_location}.')
                glue_table_names.append(the_name)
            # If table names end with /, we should remove /
            glue_table_names_noslash = []
            for item in glue_table_names:
                glue_table_names_noslash.append(item.replace("/",""))
            glue_table_names = glue_table_names_noslash
        print('\nglue_table_names:')
        print(glue_table_names)
        print('"glue_database_list" function completed successfully.')
        return glue_table_names

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

def generate_result_location(target_bucket):
    """
    Function to generate the saving location of the validation result.

    PARAMETERS:
        target_bucket -> target bucket name

    RETURNS:
        result_location -> the folder to save validation result
        None -> if any invalid input
    """
    if not isinstance(target_bucket, str):
        print("target_bucket and target_prefix should be strings.")
        print('"generate_result_location" function completed unsuccessfully.')
        return None
    result_location = \
    f"{target_bucket}/glue_database_validation/"
    print('"generate_result_location" function completed successfully.')
    return result_location

def scan_s3_bucket_folder_to_list(target_bucket):
    """
    Function to scan first level objects in the bucket.

    PARAMETERS:
        target_bucket -> target bucket name

    RETURNS:
        s3_prefix_list -> list of objects on top level in bucket
        None -> if any invalid input
    """
    if not isinstance(target_bucket, str):
        print("target_bucket should be a string.")
        print('"generate_result_location" function completed unsuccessfully.')
        return None
    try:
        # Scan only top level.
        s3_client = boto3.client('s3')
        print('s3_client generated successfully.')
        s3_paginator = s3_client.get_paginator('list_objects')
        print('s3_paginator generated successfully.') 
        s3_scan_result = s3_paginator.paginate(Bucket=target_bucket, Delimiter='/')
    except:
        json_dict['Scanning result of target S3'] = 'failed'
        print(json_dict)
        print('"scan_s3_bucket_folder_to_list" function completed unsuccessfully.')
        return None
    else:
        # Create a list of top level folders in s3 bucket: s3_prefix_list.
        s3_prefix_list = []
        for s3_prefix in s3_scan_result.search('CommonPrefixes'):
            s3_prefix_list.append(s3_prefix.get('Prefix'))
        s3_prefix_list_noslash = []
        for item in s3_prefix_list:
            # Replace all punctuations with underscore, convert upper case to lower case and remove forward slash.
            s3_prefix_list_noslash.append(remove_punctuation(item.lower().replace("/","")))
        s3_prefix_list = s3_prefix_list_noslash
        print('\ns3_prefix_list:')
        print(s3_prefix_list)
        print('"scan_s3_bucket_folder_to_list" function completed successfully.')
        return s3_prefix_list

def get_missing_sets(list_a, list_b):
    """
    Function to get missing values from two list mutually.

    PARAMETERS:
        list_a -> a list
        list_b -> the other list

    RETURNS:
        missing_in_list_b -> a set of values, which are in list_a but not in list_b
        missing_in_list_a -> a set of values, which are in list_b but not in list_a
    """
    if not isinstance(list_a,list) or not isinstance(list_b, list):
        print('list_a and list_b must be lists.')
        print('"get_missing_sets" function completed unsuccessfully.')
        return None
    # A set holds items in list_a but not in list_b: missing_in_list_b.
    missing_in_list_b = set(list_a).difference(set(list_b))
    # A set holds items in list_b but not in list_a: missing_in_list_a.
    missing_in_list_a = set(list_b).difference(set(list_a))

    print(f'\nMissing_in_list_b: {missing_in_list_b}.')
    print(f'\nMissing_in_list_a: {missing_in_list_a}.')
    print('"get_missing_sets" function completed successfully.')
    return missing_in_list_a, missing_in_list_b

def save_validation_missing_result(missing_in_s3, missing_in_glue_database,
    saving_bucket, saving_prefix):
    """
    Function to save validation result to S3.

    PARAMETERS:
        missing_in_s3 -> a set of values in glue database but not in S3
        missing_in_glue_database -> a set of values in S3 but not in glue database
        saving_bucket -> result would be saved in this bucket
        saving_prefix -> result would be saved in this path

    RETURNS:
        True -> result saved successfully
        None -> result saved unsuccessfully
    """
    if not isinstance(missing_in_s3,set) or not isinstance(missing_in_glue_database, set):
        print('missing_in_s3 and missing_in_glue_database must be sets.')
        print('"save_validation_missing_result" function completed unsuccessfully.')
        return None
    if not isinstance(saving_bucket,str) or not isinstance(saving_prefix, str):
        print('saving_bucket and saving_prefix must be strings.')
        print('"save_validation_missing_result" function completed unsuccessfully.')
        return None

    json_dict = {'missing_in_s3':missing_in_s3, 
        'missing_in_glue_database':missing_in_glue_database}
    json_ob = json.dumps(json_dict, indent=2)
    print('json_ob:')
    print(json_ob)
    s3_result_client = boto3.client('s3')
    try:
        s3_result_client.put_object(Body = json_ob,
            Bucket = saving_bucket, Key = saving_prefix)
    except:
        print('Cannot send validation result to S3.')
        print('"save_validation_missing_result" function completed unsuccessfully.')
        return None
    else:
        print('"save_validation_missing_result" function completed successfully.')
        return True

def get_sns_name_from_stack_name(stack_name):
    """
    Function to get sns name from stack name.

    PARAMETERS:
        stack_name -> the stack name

    RETURNS:
        sns_name -> the sns name to send result
        None -> if get invalid input
    """
    if not isinstance(stack_name,str):
        print('stack_name must be string.')
        print('"get_sns_name_from_stack_name" function completed unsuccessfully.')
        return None
    # snsname = "dish.vendor.glue.catalog.validation.sns.demo" sns cannot use dot, so have to replace them with underscore
    sns_name = stack_name.split('---')[0]+'---gluevalidation'
    print('"get_sns_name_from_stack_name" function completed successfully.')
    return sns_name

def get_sns_arn(sns_client, sns_name):
    """
    Function to get sns arn from sns name.

    PARAMETERS:
        sns_client -> sns boto3 client
        sns_name -> sns topic name

    RETURNS:
        sns_topic_arn -> SNS topic arn (if there sns_name is valid)
        None -> if any invalid input
    """
    if sns_client.__class__.__name__ != "SNS":
        print("Not a valid sns client.")
        print('"get_sns_arn" seciton done unsuccessfully.')
        return None
    if not isinstance(sns_name, str):
        print("sns_name should be a string.")
        print('"get_sns_arn" seciton done unsuccessfully.')
        return None
    sns_topic_list = sns_client.list_topics()['Topics']
    sns_topic_arn_list = [topic['TopicArn'] for topic in sns_topic_list]
    for sns_topic_arn in sns_topic_arn_list:
        if sns_topic_arn.split(":")[-1] == sns_name:
            print('"get_sns_arn" seciton done successfully.')
            return sns_topic_arn
    print('Cannot get sns_topic_arn.')
    print('"get_sns_arn" seciton done unsuccessfully.')
    return None

def send_sns_to_subscriber(saving_bucket, saving_prefix, current,
    sns_client, sns_topic_arn, message):
    """
    Function to sent email to sns subscriber about the validation.

    PARAMETERS:
        saving_bucket -> saving s3 bucket name
        saving_prefix -> saving s3 prefix/path
        current -> current denver local time as timestamp
        sns_client -> sns boto3 client
        sns_topic_arn -> sns topic arn
        message -> message in format of dict

    RETURNS:
        response -> sns api call response
        None -> invalid input or connection/permission issue
    """
    if (not isinstance(saving_bucket, str) or
        not isinstance(saving_prefix, str) or
        not isinstance(current, str) or
        not isinstance(sns_topic_arn, str)):
        print('target_bucket, target_prefix, current, sns_topic_arn should be strings.')
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    if sns_client.__class__.__name__ != "SNS":
        print("Not a valid sns client.")
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    if not isinstance(message, dict):
        print('message should be a dict.')
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    subject = f'{saving_bucket} {saving_prefix} validation done.'
    try:
        response = sns_client.publish(
                TargetArn=sns_topic_arn,
                Message=json.dumps({'default': json.dumps(message, indent = 6)}),
                Subject=subject,
                MessageStructure='json')
    except sns_client.exceptions.InvalidParameterException as err:
        print('Not a valid sns_topic_arn.')
        print(err)
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    except sns_client.exceptions.NotFoundException as err:
        print('Not a valid sns_topic_arn.')
        print(err)
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    else:
        print('"send_sns_to_subscriber" function completed successfully.')
        return response



# Get local time

print(f'current time: {current}')

# Read stackname to use later in result_path and SNS name:

s3bucketname = database_name



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
