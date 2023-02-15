"""
Validation Python Script
"""
import sys
import json
from datetime import datetime
import time
import boto3
from botocore.client import ClientError
from botocore.exceptions import ConnectionClosedError
import pytz
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StringType, LongType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

def get_target_location():
    """
    Function to get target bucket and target prefix of folder to validate.

    PARAMETERS:
    	None

	RETURNS:
		target_bucket -> s3 bucket of folder to validate
		target_prefix -> folder in bucket to validate
   	"""
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']
    target_bucket_and_prefix = job_name
    target_bucket = target_bucket_and_prefix.split("/")[0]
    target_prefix = target_bucket_and_prefix.replace(target_bucket, "")[1:]
    print('"get_target_location" function completed successfully.')
    return target_bucket, target_prefix

def bucket_validation(s3_bucket, s3_resource):
    """
   	Function to validate s3 bucket.

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


def prefix_to_list(s3_bucket, s3_prefix, s3_resource):
    """
   	Function to get object list under the s3 prefix path within the s3 bucket.

	PARAMETERS:
		s3_bucket -> s3 bucket name
        s3_prefix -> s3 prefix path
        s3_resource -> boto3 s3 resource

	RETURNS:
		s3_prefix_list -> object list under s3 prefix (if s3 prefix is valid)
		None -> if any invalid input, permission or connection issue
   	"""
    if s3_resource.__class__.__name__ != "s3.ServiceResource":
        print("Not a valid s3 resource.")
        print('"prefix_to_list" function completed unsuccessfully.')
        return None
    if not isinstance(s3_bucket, str) or not isinstance(s3_prefix, str):
        print("s3_bucket and s3_prefix should be strings.")
        print('"prefix_to_list" function completed unsuccessfully.')
        return None
    if s3_prefix[-1]!="/":
        s3_prefix+="/"
    try:
        s3_bucket_objects_collection = s3_resource.Bucket(s3_bucket) \
            .objects.filter(Prefix=s3_prefix)
    except ClientError as err:
        print(err)
        print('"prefix_to_list" function completed unsuccessfully.')
        return None
    else:
        s3_prefix_list = []
        for item in s3_bucket_objects_collection:
            s3_prefix_list.append(item.key)
        print('"prefix_to_list" function completed successfully.')
        return s3_prefix_list

def prefix_validation(s3_prefix, s3_prefix_list):
    """
   	Function to validate whether s3 prefix can be considered as a folder.

	PARAMETERS:
		s3_prefix -> s3 prefix path
        s3_prefix_list -> object list under s3 prefix

	RETURNS:
		s3_prefix -> if s3 prefix can be considered as a folder
		None -> if any invalid input or s3_prefix can not be a folder
   	"""
    if not isinstance(s3_prefix, str):
        print("s3_prefix should be a string.")
        print('"prefix_validation" function completed unsuccessfully.')
        return None
    if not isinstance(s3_prefix_list, list):
        print("s3_prefix_list should be a list.")
        print('"prefix_validation" function completed unsuccessfully.')
        return None
    if s3_prefix[-1]!="/":
        s3_prefix+="/"
    if s3_prefix in s3_prefix_list:
        print('"prefix_validation" function completed successfully.')
        return s3_prefix
    print('"prefix_validation" function completed unsuccessfully.')
    return None

def get_file_location(trigger_s3_bucket, trigger_s3_path):
    """
   	Function to get file location of the triggering file from Glue Job system.

	PARAMETERS:
		trigger_s3_bucket -> s3 bucket name
        trigger_s3_path -> s3 path/prefix

	RETURNS:
		file_bucket -> s3 bucket name
		file_prefix -> folder in bucket to validate
        file_name -> trigger file name
        None -> if any invalid input
   	"""
    if not isinstance(trigger_s3_bucket, str) or not isinstance(trigger_s3_path, str):
        print("trigger_s3_bucket and trigger_s3_path should be strings.")
        print('"get_file_location" function completed unsuccessfully.')
        return None
    args = getResolvedOptions(sys.argv, [trigger_s3_bucket, trigger_s3_path])
    file_bucket = args[trigger_s3_bucket]
    file_prefix = args[trigger_s3_path]
    file_name = file_prefix.split("/")[-1]
    print(f'Bucket from lambda: {file_bucket}.')
    print(f'Folder from lambda: {file_prefix}.')
    print(f'file_name: {file_name}.')
    print('"get_file_location" function completed successfully.')
    return file_bucket, file_prefix, file_name

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
    except pytz.UnknownTimeZoneError as err_time_zone:
        print(err_time_zone)
        print('"get_current_denver_time" function completed unsuccessfully.')
        return 'cannot_get_timestamp'
    else:
        datetime_den = datetime.now(denver_time)
        current = datetime_den.strftime(time_format)
        print(f'Current local time is {current}.')
        print('"get_current_denver_time" function completed successfully.')
        return current

def generate_result_location(target_bucket, target_prefix):
    """
   	Function to get target bucket and target prefix of folder to validate.

	PARAMETERS:
		target_bucket -> target bucket name
        target_prefix -> target prefix/path

	RETURNS:
		result_location -> the folder to save validation result
		None -> if any invalid input
   	"""
    if not isinstance(target_bucket, str) or not isinstance(target_prefix, str):
        print("target_bucket and target_prefix should be strings.")
        print('"generate_result_location" function completed unsuccessfully.')
        return None
    target_prefix_no_slash = target_prefix.replace("/", "_")
    result_location = \
    f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_{target_prefix_no_slash}/"
    print('"generate_result_location" function completed successfully.')
    return result_location

def setup_spark():
    """
   	Function to setup spark.

	PARAMETERS:
		None

	RETURNS:
		spark -> can be used to generate pyspark dataframe
   	"""
    s_c = SparkContext()
    glue_context = GlueContext(s_c)
    spark = glue_context.spark_session
    print('"setup_spark" function completed successfully.')
    return spark


def initial_boto3_client(aws_service):
    """
   	Function to initial boto3 client for aws service.

	PARAMETERS:
		aws_service -> aws service argument

	RETURNS:
		the_client -> aws service boto3 client
		None -> if any invalid input
   	"""
    if not isinstance(aws_service, str):
        print("aws_service should be a string.")
        print('"initial_boto3_client" function completed unsuccessfully.')
        return None
    the_client = boto3.client(aws_service)
    print('"initial_boto3_client" function completed successfully.')
    return the_client

def initial_boto3_resource(aws_service):
    """
   	Function to initial boto3 resource for aws service.

	PARAMETERS:
		aws_service -> aws service argument
	RETURNS:
		the_resource -> aws service boto3 resource
		None -> if any invalid input
   	"""
    if not isinstance(aws_service, str):
        print("aws_service should be a string.")
        print('"initial_boto3_resource" function completed unsuccessfully.')
        return None
    the_resource = boto3.resource(aws_service)
    print('"initial_boto3_resource" function completed successfully.')
    return the_resource

def get_sns_name(target_bucket):
    """
   	Function to get sns name based on target_bucket.

	PARAMETERS:
		target_bucket -> target bucket name

	RETURNS:
		sns_name -> sns topic name
		None -> if any invalid input
   	"""
    if not isinstance(target_bucket, str):
        print("target_bucket should be a string.")
        print('"get_sns_name" function completed unsuccessfully.')
        return None
    sns_name = target_bucket.replace(".", "")
    print('"get_sns_name" function completed successfully.')
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


def sns_send(sns_client, sns_topic_arn, message, subject):
    """
   	Function to sent out sns api call.

	PARAMETERS:
		sns_client -> sns boto3 client
        sns_topic_arn -> sns topic arn
        message -> message string
        subject -> subject string

	RETURNS:
		response -> sns api call response
		None -> if any invalid input
   	"""
    if sns_client.__class__.__name__ != "SNS":
        print("Not a valid sns client.")
        print('"sns_send" function completed unsuccessfully.')
        return None
    if not isinstance(sns_topic_arn, str) or not isinstance(message) or \
        not isinstance(subject):
        print("sns_topic_arn, message and subject should be strings.")
        print('"sns_send" function completed unsuccessfully.')
        return None
    try:
        response = sns_client.publish(
            TargetArn=sns_topic_arn,
            Message=json.dumps({'default': json.dumps(message, indent = 6)}),
            Subject=subject,
            MessageStructure='json')
    except sns_client.exceptions.InvalidParameterException as err:
        print('Not a valid sns_topic_arn.')
        print(err)
        print('"sns_send" function completed unsuccessfully.')
        return None
    except sns_client.exceptions.NotFoundException as err:
        print('Not a valid sns_topic_arn.')
        print(err)
        print('"sns_send" function completed unsuccessfully.')
        return None
    else:
        print(f'{message} under {subject} is sent to sns.')
        print('"sns_send" function completed successfully.')
        return response

def rename_columns(pyspark_df, **kwargs):
    """
   	Function to rename columns in a pyspark dataframe.

	PARAMETERS:
		df -> pyspark dataframe with current columns
        **kwargs -> key value pairs for current column name and new column name

	RETURNS:
		renamed_df -> pyspark dataframe with renamed columns (incorrect key in
            kwargs will not make change)
		None -> if any invalid input (except incorrect value for the key in kwargs)
   	"""
    if not isinstance(pyspark_df, DataFrame):
        print('pyspark_df should be a pyspark dataframe.')
        print('"rename_columns" function completed unsuccessfully.')
        return None
    if not isinstance(kwargs, dict):
        print('kwargs should be a dictionary.')
        print('"rename_columns" function completed unsuccessfully.')
        return pyspark_df
    have_all_key = True
    for key, value in kwargs.items():
        if key in pyspark_df.columns:
            renamed_df = pyspark_df.withColumnRenamed(key, value)
        else:
            have_all_key = False
            print(f'{key} is not in this pyspark dataframe.')
    if have_all_key:
        print('"rename_columns" function completed successfully.')
    else:
        print('"rename_columns" function completed unsuccessfully.')
    return renamed_df

def file_to_pyspark_df(spark, file_bucket, file_prefix, schema):
    """
   	Function to generate a pyspark dataframe from a csv file.

	PARAMETERS:
		spark -> pyspark
        file_bucket -> s3 bucket name, in which the file is stored
        file_prefix -> path of the file in the bucket
        schema -> pyspark dataframe schema

	RETURNS:
		file_df -> pyspark dataframe generated with values from the file
		None -> if any invalid input
   	"""
    if not isinstance(spark, SparkSession):
        print('spark should be a spark session.')
        print('"file_to_pyspark_df" function completed unsuccessfully.')
        return None
    if not isinstance(file_bucket, str) or not isinstance(file_prefix, str):
        print('file_bucket and file_prefix should be strings.')
        print('"file_to_pyspark_df" function completed unsuccessfully.')
        return None
    if not isinstance(schema, StructType):
        print('schema should be a struct type.')
        print('"file_to_pyspark_df" function completed unsuccessfully.')
        return None
    starttime = time.time()
    file_df = (spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"s3a://{file_bucket}/{file_prefix}"))
    print('Original file_df:')
    file_df.show(truncate=False)
    endtime = time.time()
    print(f"Csv dataframe read in time: {(endtime-starttime):.06f}s.")
    print('"file_to_pyspark_df" function completed successfully.')
    return file_df

def s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format):
    """
   	Function to generate a list by scanning objects under target folder in target s3 bucket.

	PARAMETERS:
		s3_resource -> s3 boto3 resource
        target_bucket -> s3 bucket name, where to scan
        target_prefix -> s3 prefix in the bucket, where to scan
        time_format -> time format string

	RETURNS:
		obj_list -> list of scanned objects
		None -> if any invalid input, permission or connection issue
   	"""
    if s3_resource.__class__.__name__ != "s3.ServiceResource":
        print("Not a valid s3 resource.")
        print('"s3_obj_to_list" function completed unsuccessfully.')
        return None
    if (not isinstance(target_bucket, str) or not isinstance(target_prefix)
        or not isinstance(time_format, str)):
        print("target_bucket, target_prefix and time_format should be strings.")
        print('"s3_obj_to_list" function completed unsuccessfully.')
        return None
    starttime = time.time()
    s3_bucket = s3_resource.Bucket(target_bucket)
    obj_counter = 0
    obj_list = []
    try:
        s3_bucket_objects_collection = s3_bucket.objects.filter(Prefix=target_prefix)
    except ClientError as err:
        print(err)
        print('Not a valid bucket name.')
        print('"s3_obj_to_list" function completed unsuccessfully.')
        return None
    else:
        for obj in s3_bucket_objects_collection:
            key_size_date_dict = {'path':obj.key.strip(), 'size':obj.size,
                'date':obj.last_modified.strftime(time_format)}
            obj_list.append(key_size_date_dict)
            obj_counter += 1
            print(f'Fetching {obj_counter} object in target folder.')
        endtime = time.time()
        print(f"S3 objects list read in time: {(endtime-starttime):.06f}s.")
        print('"s3_obj_to_list" function completed successfully.')
        return obj_list

def list_to_pyspark_df(spark, obj_list):
    """
   	Function to generate a pyspark dataframe by reading in a list,and this function
    needs to be used together with function of 'valid_list_to_pyspark_df'

	PARAMETERS:
		spark -> pyspark
        obj_list -> list of dict with same keys and same amount of elements

	RETURNS:
		pyspark_df -> pyspark dataframe with values from the list
		None -> if any invalid input
   	"""
    if not isinstance(spark, SparkSession):
        print('spark should be a spark session.')
        print('"list_to_pyspark_df" function done unsuccessfully.')
        return None
    if valid_list_to_pyspark_df(obj_list) is None:
        print('obj_list is not a good input for pyspark dataframe.')
        print('"list_to_pyspark_df" function done unsuccessfully.')
        return None
    pyspark_df = spark.createDataFrame(obj_list)
    pyspark_df.show()
    print('"list_to_pyspark_df" function done successfully.')
    return pyspark_df

def valid_list_to_pyspark_df(a_list):
    """
   	Function to validation if a list can be used to generate pypark dataframe.

	PARAMETERS:
		a_list -> a list

	RETURNS:
		a_list -> if it can be used to generate pyspark dataframe
		None -> if any invalid input
   	"""
    if not isinstance(a_list, list):
        print('a_list is not a list.')
        print('"valid_list_to_pyspark_df" function completed unsuccessfully.')
        return None
    if len(a_list) == 0:
        print('a_list has no value.')
        print('"valid_list_to_pyspark_df" function completed unsuccessfully.')
        return None
    key_set = set()
    if isinstance(a_list[0], dict):
        for item in a_list[0].keys():
            if not isinstance(item, str):
                print('Key in dict must be a string.')
                print('"valid_list_to_pyspark_df" function completed unsuccessfully.')
                return None
            key_set.add(item)
    for item in a_list:
        if not isinstance(item, dict):
            print('An element in a_list is not a dict.')
            print('"valid_list_to_pyspark_df" function completed unsuccessfully.')
            return None
        if set(item.keys()) != key_set:
            print('Keys in dict are not consistant.')
            print('"valid_list_to_pyspark_df" function completed unsuccessfully.')
            return None
    print('"valid_list_to_pyspark_df" function completed successfully.')
    return a_list

def get_script_prefix(target_prefix, script_file_name):
    """
   	Function to get validation script prefix in target bucket.

	PARAMETERS:
		target_prefix -> s3 prefix/path
        script_file_name -> file name of the python code in target_prefix

	RETURNS:
		script_prefix -> absolute path of the python code file in the bucket
		None -> if any invalid input
   	"""
    if (not isinstance(target_prefix, str) or not
        isinstance(script_file_name, str)):
        print('target_prefix and script_file_name should be strings.')
        print('"get_script_prefix" function completed unsuccessfully.')
        return None
    if target_prefix[-1]=="/":
        script_prefix = target_prefix+script_file_name
    else:
        script_prefix = target_prefix+"/"+script_file_name
    print('"get_script_prefix" function completed successfully.')
    return script_prefix

def remove_script_from_df(pyspark_df, remove_value, column_name):
    """
   	Function to remove script prefix/path from the dataframe.

	PARAMETERS:
		pyspark_df -> pyspark dataframe
        remove_value -> the value needs to be removed
        column_name -> remove_value should be in this column

	RETURNS:
		pyspark_df_updated -> updated dataframe by removing remove_value in column_name column
		pyspark_df -> original dataframe if there is no valid column_name
		None -> if any invalid input
   	"""
    if not isinstance(pyspark_df, DataFrame):
        print('pyspark_df should be a pyspark dataframe.')
        print('"remove_script_from_df" function completed unsuccessfully.')
        return None
    if not isinstance(remove_value, str):
        print('remove_value should be a string.')
        print('"remove_script_from_df" function completed unsuccessfully.')
        return pyspark_df
    if column_name in pyspark_df.columns:
        pyspark_df_updated = pyspark_df.filter(pyspark_df[column_name]!=remove_value)
        print(f'After remove value {remove_value} :')
        pyspark_df_updated.show(truncate=False)
        print('"remove_script_from_df" function completed successfully.')
        return pyspark_df_updated
    print(f'{column_name} is not in bucket_df.')
    print('"remove_script_from_df" function completed unsuccessfully.')
    return pyspark_df

def get_missing_objects(df_1, df_2, df_1_column, df_2_column):
    """
   	Function to generate pyspark dataframe for missing objects.

	PARAMETERS:
		df_1 -> pyspark dataframe with the values from triggering file
        df_2 -> pyspark dataframe with the values by scanning target s3
        df_1_column -> column name in df_1 for comparison, such as path
        df_2_column -> column name in df_2 for comparison, such as b_path

	RETURNS:
		missing_df -> pyspark dataframe, items under column path are in df_1 but not in df_2
		None -> if any invalid input, df_1_column not in df_1.columns or
            df_2_column not in df_2.columns
   	"""
    if not isinstance(df_1, DataFrame) or not isinstance(df_2, DataFrame):
        print('df_1 and df_2 should be pyspark dataframes.')
        print('"get_missing_objects" function completed unsuccessfully.')
        return None
    if df_1_column not in df_1.columns or df_2_column not in df_2.columns:
        print('df_1_column and df_2_column should be column names in pyspark dataframes.')
        print('"get_missing_objects" function completed unsuccessfully.')
        return None
    join_expr = df_1[df_1_column] == df_2[df_2_column]
    join_type = "anti"
    missing_df = df_1.join(df_2, join_expr, join_type)
    print('missing_df:')
    missing_df.show(truncate=False)
    print('"get_missing_objects" function completed successfully.')
    return missing_df

def get_df_count(pyspark_df):
    """
   	Function to count number of rows from a pyspark dataframe.

	PARAMETERS:
		pypark_df -> a pyspark dataframe

	RETURNS:
		df_count -> number of rows of the dataframe
		None -> if any invalid input
   	"""
    if not isinstance(pyspark_df, DataFrame):
        print('pyspark_df should be a pyspark dataframe.')
        print('"get_df_count" function completed unsuccessfully.')
        return None
    df_count = pyspark_df.count()
    print('"get_df_count" function completed successfully.')
    return df_count

def get_match_objects(df_1, df_2, df_1_column, df_2_column, columns_dict):
    """
   	Function to generate pyspark dataframe for matched objects.

	PARAMETERS:
		df_1 -> pyspark dataframe with the values from triggering file
        df_2 -> pyspark dataframe with the values by scanning target s3
        df_1_column -> path in df_1
        df_1_column_1 -> size in df_1
        df_2_column -> b_path in df_2
        df_2_column_1 -> b_size in df_2
        df_2_column_2 -> date in df_2

	RETURNS:
		match_df -> pyspark dataframe, items under column path are in both df_1 and df_2
		None -> if any invalid input
   	"""
    if not isinstance(df_1, DataFrame) or not isinstance(df_2, DataFrame):
        print('df_1 and df_2 should be pyspark dataframes.')
        print('"get_match_objects" function completed unsuccessfully.')
        return None
    if df_1_column not in df_1.columns or df_2_column not in df_2.columns:
        print('df_1_column and df_2_column should be column names in df_1 and df_2.')
        print('"get_match_objects" function completed unsuccessfully.')
        return None
    if not isinstance(columns_dict, dict):
        print('columns_dict should be a dict.')
        print('"get_match_objects" function completed unsuccessfully.')
        return None
    if set(columns_dict.keys()) != {"df_1", "df_2"}:
        print('columns_dict should has keys: df_1 and df_2.')
        print('"get_match_objects" function completed unsuccessfully.')
        return None
    df_1_column_set = columns_dict["df_1"]
    df_2_column_set = columns_dict["df_2"]
    if (len(df_1_column_set.difference(set(df_1.columns))) > 0 or
       len(df_2_column_set.difference(set(df_2.columns))) > 0):
        print('Cannot find some column names in df_1 or df_2.')
        print('"get_match_objects" function completed unsuccessfully.')
        return None
    select_arry = []
    for item in df_1_column_set:
        select_arry.append(df_1[item])
    for item in df_2_column_set:
        select_arry.append(df_2[item])
    join_expr = df_1[df_1_column] == df_2[df_2_column]
    join_type = "inner"
    match_df = df_1.join(df_2, join_expr, join_type).select(select_arry)
    print('match_df:')
    match_df.show(truncate=False)
    print('"get_match_objects" function completed successfully.')
    return match_df

def get_wrong_size_objects(pyspark_df, df_1_column, df_2_column):
    """
   	Function to generate pyspark dataframe for wrong size object.

	PARAMETERS:
		pyspark_df -> a pyspark dataframe
        df_column_1 -> column name, under which values are used for comparison
        df_column_2 -> the other column name, under which values are used for comparison

	RETURNS:
		wrong_size_df -> pyspark dataframe, items under column size are different from df_1 to df_2
		None -> if any invalid input
   	"""
    if not isinstance(pyspark_df, DataFrame):
        print('pyspark_df should be a pyspark dataframe.')
        print('"get_wrong_size_objects" function completed unsuccessfully.')
        return None
    if df_1_column not in pyspark_df or df_2_column not in pyspark_df:
        print('df_1_column and df_2_column should be column names in pyspark_df.')
        print('"get_wrong_size_objects" function completed unsuccessfully.')
        return None
    wrong_size_df = pyspark_df.filter(pyspark_df[df_1_column]!=pyspark_df[df_2_column])
    print('wrong_size_df:')
    wrong_size_df.show(truncate=False)
    print('"get_wrong_size_objects" function completed successfully.')
    return wrong_size_df



def save_result_to_s3(row_count, result_location, current, pyspark_df, obj_name):
    """
   	Function to save the validation results.

	PARAMETERS:
		row_count -> how many row in dataframe
        result_location -> where to save the results
        current -> current denver local time as timestamp
        pyspark_df -> pyspark dataframe to save
        obj_name -> object name for the result in s3

	RETURNS:
		message -> a string about the saving status (Glue Job no error with invalid savepath)
		None -> if any invalid input, permission or connection issue
   	"""
    if not isinstance(row_count, int):
        print('row_count should be an integer.')
        print('"save_result_to_s3" function finished unsuccessfully.')
        return None
    if (not isinstance(result_location, str) or
        not isinstance(current, str) or
        not isinstance(obj_name, str)):
        print('result_location, current and obj_name should be strings.')
        print('"save_result_to_s3" function finished unsuccessfully.')
        return None
    if not isinstance(pyspark_df, DataFrame):
        print('pyspark_df should be a pyspark dataframe.')
        print('"save_result_to_s3" function finished unsuccessfully.')
        return None
    if row_count > 0:
        savepath = f"{result_location}{obj_name}_{current}.csv"
        try:
            pyspark_df.toPandas().to_csv(savepath, index = False)
        except ClientError as err:
            print(err)
            print('"save_result_to_s3" function finished unsuccessfully.')
            return None
        else:
            message = f"saved at {result_location[6:]}_{obj_name}_{current}.csv"
    else:
        print(f"No {obj_name} object.")
        message = f"No {obj_name} item found."
    print('"save_result_to_s3" function finished successfully.')
    return message

def send_sns_to_subscriber(target_bucket, target_prefix, current,
    sns_client, sns_topic_arn, missing_message, wrong_size_message):
    """
   	Function to sent email to sns subscriber about the validation.

	PARAMETERS:
		target_bucket -> target s3 bucket name
		target_prefix -> target s3 prefix/path
        current -> current denver local time as timestamp
		sns_client -> sns boto3 client
		sns_topic_arn -> sns topic arn
        missing_message -> a string to state the missing objects
        wrong_size_message -> a string to state the wrong size objects

	RETURNS:
		response -> sns api call response
   	"""
    if (not isinstance(target_bucket, str) or
        not isinstance(target_prefix, str) or
        not isinstance(current, str) or
        not isinstance(sns_topic_arn, str) or
        not isinstance(missing_message, str) or
        not isinstance(wrong_size_message, str)):
        print('target_bucket, target_prefix, current, sns_topic_arn, \
missing_message and wrong_size_messageshould be strings.')
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    if sns_client.__class__.__name__ != "SNS":
        print("Not a valid sns client.")
        print('"send_sns_to_subscriber" function completed unsuccessfully.')
        return None
    message = {"Missing items: ":missing_message,"Wrong size objects: ":
        wrong_size_message,"Validation started at: ":current}
    subject = f'{target_prefix} {target_bucket} validation done.'
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

def main():
    """
    main
    """

    #######################################################
    ## 1. Setup basic arguements for s3 to s3 validation ##
    #######################################################
    trigger_s3_bucket_key = 's3_bucket'
    trigger_s3_prefix_key = 's3_path'
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    aws_sns_client = 'sns'
    aws_s3_resource = 's3'

    ############################################################################
    ## 2. Get initial arguements from Glue Job sys and other helper functions ##
    ############################################################################
    target_bucket, target_prefix = get_target_location()
    current = get_current_denver_time(time_zone, time_format)
    spark = setup_spark()
    s3_resource = initial_boto3_resource(aws_s3_resource)
    sns_client = initial_boto3_client(aws_sns_client)
    sns_name = get_sns_name(target_bucket)
    result_location = generate_result_location(target_bucket, target_prefix)

    if (get_file_location(trigger_s3_bucket_key, trigger_s3_prefix_key) is None or
        get_sns_arn(sns_client, sns_name) is None):
        sys.exit("Can not get file_bucket, file_prefix and file_name of the trigging file.")

    file_bucket, file_prefix, file_name = get_file_location(trigger_s3_bucket_key,
                                                            trigger_s3_prefix_key)
    sns_topic_arn = get_sns_arn(sns_client, sns_name)

    #####################################################
    ## 3. Stop if trigger file is not the expected one ##
    #####################################################
    # Make sure this trigger file is for validation.

    if file_name != "s3_to_s3_validation.csv":
        sys.exit("This is not a validation request.")

    # Make sure the target bucket is valid.
    target_s3_bucket_validation = bucket_validation(target_bucket, s3_resource)
    if target_s3_bucket_validation is None:
        sys.exit("There is no such target bucket to validate.")

    # Make sure the target prefix exist.
    target_s3_prefix_list = prefix_to_list(target_bucket, target_prefix, s3_resource)
    if target_s3_prefix_list is None:
        sys.exit("There is no such target prefix to validate.")

    # Make sure the target prefix is a folder.
    target_s3_prefix_validation = prefix_validation(target_prefix, target_s3_prefix_list)
    if target_s3_prefix_validation is None:
        sys.exit("Target prefix is not a folder to validate.")


    #########################################
    ## 3. Read file into PySpark dataframe ##
    #########################################
    schema = (StructType()
      .add("id",LongType(),True)
      .add("path",StringType(),True)
      .add("size",LongType(),True))

    file_df = file_to_pyspark_df(spark, file_bucket, file_prefix, schema)

    #################################################################
    ## 4. Scan the objects' name and size under the target folder  ##
    ##  in the target bucket to generate another PySpark dataframe ##
    #################################################################
    bucket_list = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)
    bucket_df = list_to_pyspark_df(spark, bucket_list)

    ########################################################
    ## 5. remove validation script from PySpark dataframe ##
    ########################################################
    script_path_in_bucket_df = get_script_prefix(target_prefix,
     "s3_to_s3_validation_script.py")
    bucket_df = remove_script_from_df(bucket_df, script_path_in_bucket_df, "path")

    #####################################################
    ## 6. Prepare and do comparisons on two dataframes ##
    #####################################################
    # Store bucket dataframe with different columns name to
    # avoid conflict when comparing two dataframes, which have duplicate names
    rename_cols = {"size": "b_size","path": "b_path"}
    bucket_df_renamed = rename_columns(bucket_df, **rename_cols)

    # Get missing dataframe, values in file_df not in bucket_df
    missing_df = get_missing_objects(file_df, bucket_df_renamed, "path", "b_path")
    missing_count = get_df_count(missing_df)
    print(f'Missing s3 objects number: {missing_count}.')

    # Get match dataframe
    match_df = get_match_objects(file_df, bucket_df_renamed, "path", "b_path",
            {"df_1": {"path", "size"}, "df_2": {"b_path", "b_size", "date"}})

    # Get wrong size dataframe
    wrong_size_df = get_wrong_size_objects(match_df, "size", "b_size")
    wrong_size_count = get_df_count(wrong_size_df)
    print(f'Wrong size s3 objects number: {wrong_size_count}.')

    #####################################################################################
    ## 7. Save validation result to Target S3 with the same level as the Target folder ##
    #####################################################################################
    obj_name = "missing"
    missing_message = save_result_to_s3(missing_count,
        result_location, current, missing_df, obj_name)
    obj_name = "wrong_size"
    wrong_size_message = save_result_to_s3(wrong_size_count,
        result_location, current, wrong_size_df, obj_name)

    #################################################
    ## 8. Send out notification to SNS subscribers ##
    #################################################
    send_sns_to_subscriber(target_bucket, target_prefix, current,
        sns_client, sns_topic_arn, missing_message, wrong_size_message)

if __name__ == "__main__":
    # Start execution
    totalstart = time.time()
    main()
    # The end of the validaiton execution
    totalend = time.time()
    print(f"Total execution time: {(totalend-totalstart):.06f}s.")
    print("\n")
    print("Executin completed.")
