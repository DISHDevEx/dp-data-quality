"""
Glue Validation Python Script
"""

import string
import sys
import json
import time
from datetime import datetime
import boto3
from botocore.client import ClientError
from botocore.exceptions import ConnectionClosedError
import pytz
from pytz.exceptions import UnknownTimeZoneError
from awsglue.utils import getResolvedOptions # pylint: disable=import-error



def get_stack_name():
    """
    Function to get glue database name

    PARAMETERS:
        None

    RETURNS:
        stack_name -> the CFT stack name
    """
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    stack_name = args['JOB_NAME']
    return stack_name

def get_glue_database_name():
    """
    Function to get glue database name

    PARAMETERS:
        None

    RETURNS:
        database_name -> the glue database name
    """
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    stack_name = args['JOB_NAME']
    database_name = stack_name.split('---')[1].replace('--','.')
    return database_name

def glue_database_list(glue_database_name):
    """
    Function to validate that a glue database exists.

    PARAMETERS:
        glue_database_name -> glue database name

    RETURNS:
        glue_table_names -> a list of table names from glue database
        None -> if any invalid input, permission or connection issue
    """
    if not isinstance(glue_database_name, str):
        print("glue_database_name should be a string.")
        print('"glue_database_list" function completed unsuccessfully.')
        return None
    try:
        glue_client = boto3.client('glue')
        print('glue_client is set up in "glue_database_list".')
        # Need grant database and table access in lake formation.
        # Otherwise, will return an empty list of tables.
        response = glue_client.get_tables(
            DatabaseName = glue_database_name
            )
        print('Response gotten in "glue_database_list".')
        # return format:
        # Name:  response['TableList'][iterator]['Name']
        # Location: response['TableList'][iterator]['StorageDescriptor']['Location']
    except glue_client.exceptions.EntityNotFoundException as err:
        print(err)
        print('"glue_database_list" function completed unsuccessfully.')
        return None
    except Exception as err: # pylint: disable=broad-except
        print(err)
        print('Error generating "glue_database_list".')
        print('"glue_database_list" function completed unsuccessfully.')
        return None
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
    glue_table_names = [item.replace("/","") for item in glue_table_names]
    print('\nGlue table names:')
    print(glue_table_names)
    print('"glue_database_list" function completed successfully.')
    return glue_table_names

def bucket_validation(s3_bucket):
    """
    Function to validate that an S3 bucket exists in current account.

    PARAMETERS:
        s3_bucket -> s3 bucket name

    RETURNS:
        s3_bucket_info_dict -> s3 bucket info dict (if s3 bucket is valid)
        None -> if any invalid input, permission or connection issue
    """
    if not isinstance(s3_bucket, str):
        print("s3_bucket should be a string.")
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    try:
        s3_resource = boto3.resource('s3')
        print('S3 resource in "bucket_validation"is setup.')
        s3_bucket_info_dict = s3_resource.meta.client.head_bucket(Bucket=s3_bucket)
    except ClientError as err:
        print(err)
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    except ConnectionClosedError as err:
        print(err)
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    except Exception as err: # pylint: disable=broad-except
        print(err)
        print('"bucket_validation" function caught other errors.')
        print('"bucket_validation" function completed unsuccessfully.')
        return None
    print('"bucket_validation" function completed successfully.')
    return s3_bucket_info_dict

def get_current_time(time_zone, time_format):
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
        print('"get_current_time" function completed unsuccessfully.')
        return None
    try:
        current_time_zone = pytz.timezone(time_zone)
    except UnknownTimeZoneError as err:
        print(err)
        print('"get_current_time" function completed unsuccessfully.')
        return None
    datetime_current_time_zone = datetime.now(current_time_zone)
    current_time_str = datetime_current_time_zone.strftime(time_format)
    print(f'Current local time is {current_time_str}.')
    print('"get_current_time" function completed successfully.')
    return current_time_str

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
        print("target_bucket should be a string.")
        print('"generate_result_location" function completed unsuccessfully.')
        return None
    result_location = \
    f"{target_bucket}/glue_database_validation/"
    print('"generate_result_location" function completed successfully.')
    return result_location

def remove_punctuation(a_string):
    """
    Function to replace all punctuation with underscore to replicate what 
        Glue Catalog does to generate Glue Database.

    PARAMETERS:
        a_string -> a string

    RETURNS:
        a_string -> a string with all punctuations replaced by underscores
        None -> if any invalid input
    """
    if not isinstance(a_string, str):
        print('"remove_punctuation" function completed unsuccessfully.')
        return None
    for char in string.punctuation:
        a_string = a_string.replace(char, '_')
    print('"remove_punctuation" function completed successfully.')
    return a_string

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
        print('s3_scan_result generated successfully.')
        # Create a list of top level folders in s3 bucket: s3_prefix_list.
        s3_prefix_list = [s3_prefix.get('Prefix') for s3_prefix 
                          in s3_scan_result.search('CommonPrefixes')]
        print('s3_prefix_list can be listed.')
    except Exception as err: # pylint: disable=broad-except
        print(err)
        print('"scan_s3_bucket_folder_to_list" function completed unsuccessfully.')
        return None
    # Replace all punctuations with underscore,
    # convert upper case to lower case and remove forward slash.
    s3_prefix_list = [remove_punctuation(item.lower().replace("/",""))
                       for item in s3_prefix_list]
    print('\ns3_prefix_list:')
    print(s3_prefix_list)
    print('"scan_s3_bucket_folder_to_list" function completed successfully.')
    return s3_prefix_list

def get_missing_sets(list_a, list_b):
    """
    Function to compare two lists and return discrepancies.

    PARAMETERS:
        list_a -> list number 1
        list_b -> list number 2

    RETURNS:
        missing_in_list_b -> a set of values, which are in list_a but not in list_b
        missing_in_list_a -> a set of values, which are in list_b but not in list_a
    """
    if not isinstance(list_a,list) or not isinstance(list_b, list):
        print('list_a and list_b must be lists.')
        print('"get_missing_sets" function completed unsuccessfully.')
        return None
    # Items in list_a but not in list_b.
    missing_in_list_b = set(list_a).difference(set(list_b))
    # Items in list_b but not in list_a.
    missing_in_list_a = set(list_b).difference(set(list_a))

    print(f'\nItems missing in list_b: {missing_in_list_b}.')
    print(f'\nItems missing in list_a: {missing_in_list_a}.')
    print('"get_missing_sets" function completed successfully.')
    return missing_in_list_a, missing_in_list_b

def save_validation_missing_result(missing_in_s3,
                                missing_in_glue_database,
                                saving_location,
                                current):
    """
    Function to save result of mismatch between S3 bucket and Glue database in S3.

    PARAMETERS:
        missing_in_s3 -> a list of values in glue database but not in S3 or a string
        missing_in_glue_database -> a list of values in S3 but not in glue database or a string
        saving_location -> result would be saved under this location
        current -> a timestamp string

    RETURNS:
        True -> result saved successfully
        None -> result saved unsuccessfully
    """
    if (
            (
            not isinstance(missing_in_s3,list)
            and
            not isinstance(missing_in_s3,str)
            )
            or
            (
            not isinstance(missing_in_glue_database, list)
            and
            not isinstance(missing_in_glue_database, str)
            )
        ):
        print('missing_in_s3 and missing_in_glue_database must be sets or strings.')
        print('"save_validation_missing_result" function completed unsuccessfully.')
        return None
    if not isinstance(saving_location,str) or not isinstance(current, str):
        print('saving_location and current must be strings.')
        print('"save_validation_missing_result" function completed unsuccessfully.')
        return None
    json_dict = {'missing_in_s3':missing_in_s3,
        'missing_in_glue_database':missing_in_glue_database}
    json_ob = json.dumps(json_dict, indent=2)
    print('JSON object:')
    print(json_ob)
    saving_bucket = saving_location.split('/')[0]
    saving_prefix = saving_location.replace(saving_bucket, '')[1:]
    s3_result_client = boto3.client('s3')
    try:
        s3_result_client.put_object(Body = json_ob,
            Bucket = saving_bucket, Key = saving_prefix+f'glue_validation_{current}.txt')
    except Exception as err: # pylint: disable=broad-except
        print(err)
        print('Cannot send validation result to S3.')
        print('"save_validation_missing_result" function completed unsuccessfully.')
        return None
    print('"save_validation_missing_result" function completed successfully.')
    return True

def get_sns_arn(sns_name):
    """
    Function to get sns arn from sns name.

    PARAMETERS:
        sns_client -> sns boto3 client
        sns_name -> sns topic name

    RETURNS:
        sns_topic_arn -> SNS topic arn (if there sns_name is valid)
        None -> if any invalid input
    """
    if not isinstance(sns_name, str):
        print("sns_name should be a string.")
        print('"get_sns_arn" seciton completed unsuccessfully.')
        return None
    try:
        sns_client = boto3.client('sns')
    except Exception as err: # pylint: disable=broad-except
        print(err)
        print("SNS client setup unsuccessful.")
        print('"get_sns_arn" seciton completed unsuccessfully.')
        return None
    sns_topic_list = sns_client.list_topics()['Topics']
    sns_topic_arn_list = [topic['TopicArn'] for topic in sns_topic_list]
    for sns_topic_arn in sns_topic_arn_list:
        if sns_topic_arn.split(":")[-1] == sns_name:
            print('"get_sns_arn" seciton completed successfully.')
            return sns_topic_arn
    print('Cannot get SNS Topic ARN.')
    print('"get_sns_arn" seciton completed unsuccessfully.')
    return None

def send_sns_to_subscriber(saving_location, current,
    sns_topic_arn, message):
    """
    Function to send email to SNS subscriber with validation results.

    PARAMETERS:
        saving_location -> where are results saved
        current -> current denver local time as timestamp
        sns_topic_arn -> sns topic arn
        message -> message in format of dict

    RETURNS:
        response -> sns api call response
        None -> invalid input or connection/permission issue
    """
    if (not isinstance(saving_location, str) or
        not isinstance(current, str) or
        not isinstance(sns_topic_arn, str)):
        print('saving_location, current, sns_topic_arn should be strings.')
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    if not isinstance(message, dict):
        print('message should be a dict.')
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    saving_bucket = saving_location.split('/')[0]
    saving_prefix = saving_location.replace(saving_bucket, '')[1:]
    subject = f'{saving_bucket} {saving_prefix} validation done.'
    try:
        sns_client = boto3.client('sns')
        print('sns_client in "send_sns_to_subscriber" is set up.')
        response = sns_client.publish(
                TargetArn=sns_topic_arn,
                Message=json.dumps({'default': json.dumps(message, indent = 6)}),
                Subject=subject,
                MessageStructure='json')
        print('Response gotten in "send_sns_to_subscriber".')
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
    except Exception as err:  # pylint: disable=broad-except
        print(err)
        print('Caught other errors in "send_sns_to_subscriber".')
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    print('"send_sns_to_subscriber" function completed successfully.')
    return response

def main():
    """
    main
    """
    ##################################################
    ## 1. Get S3 bucket name and glue database name ##
    ##################################################
    glue_database_name = get_glue_database_name()
    s3_bucket_name = glue_database_name
    #################################
    ## 2. Make sure S3 above exist ##
    #################################
    if bucket_validation(s3_bucket_name) is None:
        sys.exit("S3 bucket is not valid to proceed.")
    ############################
    ## 3. Generate time stamp ##
    ############################
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    current = get_current_time(time_zone, time_format)
    ########################################
    ## 3. Generate result saving location ##
    ########################################
    result_saving_location = generate_result_location(s3_bucket_name)
    ##########################################
    ## 4. Scan S3 bucket to generate a list ##
    ##########################################
    s3_obj_list = scan_s3_bucket_folder_to_list(s3_bucket_name)
    if s3_obj_list is None:
        sys.exit("s3_obj_list is not valid to proceed.")
    ##############################################
    ## 5. Scan glue database to generate a list ##
    ##############################################
    glue_database_table_list = glue_database_list(glue_database_name)
    if glue_database_table_list is None:
        sys.exit("glue_database_table_list is not valid to proceed.")
    ###############################################
    ## 6. Generate missing sets from lists above ##
    ###############################################
    missing_in_s3, missing_in_glue = get_missing_sets(s3_obj_list,
                                     glue_database_table_list)
    missing_in_s3 = list(missing_in_s3)
    missing_in_glue = list(missing_in_glue)
    if len(missing_in_s3) == 0:
        missing_in_s3 = 'no missing in s3.'
    if len(missing_in_glue) == 0:
        missing_in_glue = 'no missing in glue.'
    ################################
    ## 7. Save missing sets in S3 ##
    ################################
    save_result = save_validation_missing_result(missing_in_s3,
                                missing_in_glue,
                                result_saving_location,
                                current)
    if save_result is not None:
        print(f'Glue validaiton result is saved in {result_saving_location}.')
    #####################################
    ## 8. Send email to SNS subscriber ##
    #####################################
    stack_name = get_stack_name()
    sns_name = stack_name
    if sns_name is None:
        sys.exit("sns_name is not valid to proceed.")
    sns_arn = get_sns_arn(sns_name)
    if sns_arn is None:
        sys.exit("sns_arn is not valid to proceed.")
    message = {'missing_in_s3': missing_in_s3,
             'missing_in_glue': missing_in_glue,
             'current_time': current}
    if send_sns_to_subscriber(result_saving_location, current,
    sns_arn, message) is None:
        print('Glue validation result email is not sent out.')
    else:
        print('Glue validation result email is sent out.')

if __name__ == "__main__":
    # Start execution
    totalstart = time.time()
    main()
    # The end of the validaiton execution
    totalend = time.time()
    print(f"Total execution time: {(totalend-totalstart):.06f}s.")
    print("\n")
    print("Executin completed.")
