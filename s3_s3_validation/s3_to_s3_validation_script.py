import sys
import json
from datetime import datetime
import time
import boto3
from botocore.client import ClientError
import pytz
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.utils import AnalysisException as WrongPathError
from pyspark.sql.types import StructType, StringType, LongType

def get_target_location():
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']
    target_bucket_and_prefix = job_name
    target_bucket = target_bucket_and_prefix.split("/")[0]
    target_prefix = target_bucket_and_prefix.replace(target_bucket, "")[1:]
    print('get_target_location section done')
    return target_bucket, target_prefix

def bucket_validation(s3_bucket, s3_resource):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    try:
        s3_bucket_info_dict = s3_resource.meta.client.head_bucket(Bucket=s3_bucket)
        return s3_bucket_info_dict
    except ClientError as error_class:
        if isinstance(error_class, ClientError):
            print(error_class)
            return error_class
    finally:
        print('Bucket_validation section done')

def prefix_to_list(s3_bucket, s3_prefix, s3_resource):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    if s3_prefix[-1]!="/":
        s3_prefix+="/"
    try:
        s3_bucket_objects_collection = s3_resource.Bucket(s3_bucket).objects.filter(Prefix=s3_prefix)
    except ClientError as error_class:
        print(error_class)
        return error_class
    else:
        s3_prefix_list = []
        for item in s3_bucket_objects_collection:
            s3_prefix_list.append(item.key)
        return s3_prefix_list
    finally:
        print('Prefix_to_list section done')

def prefix_validation(s3_prefix, s3_prefix_list):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    if s3_prefix[-1]!="/":
        s3_prefix+="/"
    if s3_prefix in s3_prefix_list:
        return s3_prefix
    else:
        return None

def get_file_location(trigger_s3_bucket, trigger_s3_path):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Get the file location, which triggers this validation
    """
    args = getResolvedOptions(sys.argv, [trigger_s3_bucket, trigger_s3_path])
    file_bucket = args[trigger_s3_bucket]
    file_prefix = args[trigger_s3_path]
    file_name = file_prefix.split("/")[-1]
    print(f'bucket from lambda: {file_bucket}')
    print(f'path from lambda: {file_prefix}')
    print(f'file_name: {file_name}')
    return file_bucket, file_prefix, file_name

def get_current_denver_time(time_zone, time_format):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Get current Devner local time as timestamp
    """
    if not isinstance(time_zone,str) or not isinstance(time_format, str):
        print('time_zone and time_format must be string')
        print('get_current_denver_time section done.')
        return 'cannot_get_timestamp'
    else:
        try:
            denver_time = pytz.timezone(time_zone)
            datetime_den = datetime.now(denver_time)
            current = datetime_den.strftime(time_format)
            print(current)
            return current
        except pytz.UnknownTimeZoneError as err_time_zone:
            print(err_time_zone)
            return 'cannot_get_timestamp'
        finally:
            print('get_current_denver_time section done.')


def generate_result_location(target_bucket, target_prefix):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Generate result saving location based on target_bucket and target_prefix
    """
    target_prefix_no_slash = target_prefix.replace("/", "_")
    result_location = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_{target_prefix_no_slash}/"
    print('generate_result_location seciton done')
    return result_location

def setup_spark():
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Initial spark
    """
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    print('setup_spark section done')
    return spark


def initial_boto3_client(aws_service):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Initial boto3 client for aws service
    """
    the_client = boto3.client(aws_service)
    print('initial_boto3_client section done')
    return the_client

def initial_boto3_resource(aws_service):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Initial boto3 resource for aws service
    """
    the_resource = boto3.resource(aws_service)
    print('initial_boto3_resource section done')
    return the_resource

def get_sns_name(target_bucket):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Get sns name based on target_bucket
    """
    sns_name = target_bucket.replace(".", "")
    print('get_sns_name section done')
    return sns_name



def get_sns_arn(sns_client, sns_name):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Get sns arn from sns name
    """
    sns_topic_list = sns_client.list_topics()['Topics']
    sns_topic_arn_list = [topic['TopicArn'] for topic in sns_topic_list]
    for sns_topic_arn in sns_topic_arn_list:
        if sns_topic_arn.split(":")[-1] == sns_name:
            print('get_sns_arn seciton done')
            return sns_topic_arn
    else:
        print('Cannot get sns_topic_arn')
        print('get_sns_arn seciton done')
        return None


def sns_send(sns_client, sns_topic_arn, message, subject):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Module to sent out sns api call
    """
    if sns_topic_arn == None:
        print(f'{message} under {subject} cannot be sent to sns')
        return None
    else:
        response = sns_client.publish(
            TargetArn=sns_topic_arn,
            Message=json.dumps({'default': json.dumps(message, indent = 6)}),
            Subject=subject,
            MessageStructure='json')
        print(f'{message} under {subject} is sent to sns')
        return response

def rename_columns(df, **kwargs):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Rename columns in a pyspark dataframe
    """
    for key, value in kwargs.items():
        df = df.withColumnRenamed(key, value)
    print('rename_columns seciton done')
    return df

def file_to_pyspark_df(spark, file_bucket, file_prefix, schema):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Generate a pyspark dataframe from a csv file
    """
    starttime = time.time()
    try:
        file_df = spark.read\
            .format("csv")\
            .option("header", "true")\
            .schema(schema)\
            .load(f"s3a://{file_bucket}/{file_prefix}")
    except WrongPathError as e:
        print(e)
        return None
    else:
        print('original file_df:::')
        file_df.show(truncate=False)
        endtime = time.time()
        print(f"csv dataframe read in time: {(endtime-starttime):.06f}s")
        return file_df
    finally:
        print('file_to_pyspark_df section done')

def s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Generate a pyspark datafram by scanning objects under target folder in target s3 bucket
    """
    starttime = time.time()
    s3_bucket = s3_resource.Bucket(target_bucket)
    obj_counter = 0
    obj_list = []

    for obj in s3_bucket.objects.filter(Prefix=target_prefix):
        key_size_date_dict = {'path':obj.key.strip(), 'size':obj.size, \
            'date':obj.last_modified.strftime(time_format)}
        obj_list.append(key_size_date_dict)
        obj_counter += 1
        print(f'fetching {obj_counter} object in target folder')
    endtime = time.time()
    print(f"s3 objects list read in time: {(endtime-starttime):.06f}s")
    print('scan s3 section done')
    return obj_list

def list_to_pyspark_df(spark, obj_list):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Generate a pyspark dataframe by reading in a list
    """
    pyspark_df = spark.createDataFrame(obj_list)
    pyspark_df.show()
    print('list_to_pyspark_df function done:::')
    return pyspark_df


def get_script_prefix(target_prefix, script_file_name):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Get validation script prefix in target bucket
    """

    if target_prefix[-1]=="/":
        script_prefix = target_prefix+script_file_name
    else:
        script_prefix = target_prefix+"/"+script_file_name
    print('get_script_prefix section done')
    return script_prefix

def remove_script_from_df(pyspark_df, remove_value, column_name):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Remove script prefix/path from the dataframe
    """
    try:
        pyspark_df_updated = pyspark_df.filter(pyspark_df[column_name]!=remove_value)
    except WrongPathError as e:
        print(e)
        return pyspark_df
    else:
        print(f'after remove value {remove_value} :::')
        pyspark_df_updated.show(truncate=False)
        return pyspark_df_updated
    finally:
        print('remove_script_from_df section done')

def get_missing_objects(df_1, df_2, df_1_column, df_2_column):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Generate pyspark dataframe for missing objects
    """
    try:
        join_expr = df_1[df_1_column] == df_2[df_2_column]
        join_type = "anti"
        missing_df = df_1.join(df_2, join_expr, join_type)
        print('missing_df:::')
        missing_df.show(truncate=False)
    except WrongPathError as e:
        print(e)
        return None
    else:
        print('missing_df:::')
        missing_df.show(truncate=False)
        return missing_df
    finally:
        print('get_missing_objects section done')

def get_df_count(pypark_df):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Count number of rows from a pyspark dataframe
    """
    df_count = pypark_df.count()
    print('getting rows in a pyspark dataframe section done')
    return df_count

def get_match_objects(df_1, df_2, df_1_column, df_1_column_1,\
        df_2_column, df_2_column_1, df_2_column_2):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Generate pyspark dataframe for matched objects
    """
    try:
        join_expr = df_1[df_1_column] == df_2[df_2_column]
        join_type = "inner"
        match_df = df_1.join(df_2, join_expr, join_type).select(df_1[df_1_column], \
            df_1[df_1_column_1], df_2[df_2_column_1], df_2[df_2_column_2],)
    except WrongPathError as e:
        print(e)
        return None
    else:
        print('match_df:::')
        match_df.show(truncate=False)
        return match_df
    finally:
        print('getting match objects in a pyspark dataframe section done')

def get_wrong_size_objects(df, df_column_1, df_column_2):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Generate pyspark dataframe for wrong size object
    """
    try:
        wrong_size_df = df.filter(df[df_column_1]!=df[df_column_2])
    except WrongPathError as e:
        print(e)
        return None
    else:
        print('wrong_size_df:::')
        wrong_size_df.show(truncate=False)
        return wrong_size_df
    finally:
        print('getting wrong size objects in a pyspark dataframe section done')

def save_result(row_count, result_location, current, df, obj_name):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Save result
    """
    if row_count > 0:
        savepath = f"{result_location}{obj_name}_{current}.csv"
        message = f"saved at {result_location[6:]}_{obj_name}_{current}.csv"
        df.toPandas().to_csv(savepath, index = False)
    else:
        print(f"no {obj_name} object")
        message = f"no {obj_name} item found"
    print('save_result section done')
    return message

def send_sns_to_subscriber(target_bucket, target_prefix, current, sns_client, sns_topic_arn, missing_message, wrong_size_message):
    """
   	Function to get target bucket and target prefix of folder to validate.
	
	PARAMETERS:
		None

	RETURNS:
		target_bucket - s3 bucket of folder to validate
		target_prefix - folder in bucket to validate
   	"""
    """
    Sent email to sns subscriber
    """
    message = {"Missing items: ":missing_message,"Wrong size objects: ":wrong_size_message,"Validation started at: ":current}
    subject = f'{target_prefix} {target_bucket} validation done'
    response = sns_client.publish(
            TargetArn=sns_topic_arn,
            Message=json.dumps({'default': json.dumps(message, indent = 6)}),
            Subject=subject,
            MessageStructure='json')
    print('senting to sns section done')
    return response

def main():
    """
    main
    """

    #######################################################
    ## 1. Setup basic arguements for s3 to s3 validation ##
    #######################################################
    trigger_s3_bucket = 's3_bucket'
    trigger_s3_path = 's3_path'
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    aws_sns_client = 'sns'
    aws_s3_resource = 's3'

    ############################################################################
    ## 2. Get initial arguements from Glue Job sys and other helper functions ##
    ############################################################################
    target_bucket, target_prefix = get_target_location()
    file_bucket, file_prefix, file_name = get_file_location(trigger_s3_bucket, trigger_s3_path)
    current = get_current_denver_time(time_zone, time_format)
    spark = setup_spark()
    s3_resource = initial_boto3_resource(aws_s3_resource)
    sns_client = initial_boto3_client(aws_sns_client)
    sns_name = get_sns_name(target_bucket)
    sns_topic_arn = get_sns_arn(sns_client, sns_name)
    result_location = generate_result_location(target_bucket, target_prefix)

    #####################################################
    ## 3. Stop if trigger file is not the expected one ##
    #####################################################
    # Make sure this trigger file is for validation.
    if file_name != "s3_to_s3_validation.csv":
        sys.exit("This is not a validation request.")
    
    # Make sure the target bucket is valid.
    target_s3_bucket_validation = bucket_validation(target_bucket, s3_resource)
    if type(target_s3_bucket_validation) != dict:
        sys.exit("There is no such target bucket to validate.")

    # Make sure the target prefix exist.
    target_s3_prefix_list = prefix_to_list(target_bucket, target_prefix, s3_resource)
    if type(target_s3_prefix_list) != list:
        sys.exit("There is no such target prefix to validate.")
    
    # Make sure the target prefix is a folder.
    target_s3_prefix_validation = prefix_validation(target_prefix, target_s3_prefix_list)
    if target_s3_prefix_validation is None:
        sys.exit("Target prefix is not a folder to validate.")


    #########################################
    ## 3. Read file into PySpark dataframe ##
    #########################################
    schema = StructType() \
      .add("id",LongType(),True) \
      .add("path",StringType(),True) \
      .add("size",LongType(),True) 

    file_df = file_to_pyspark_df(spark, file_bucket, file_prefix, schema)

    ###########################################################################################################################
    ## 4. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe ##
    ###########################################################################################################################
    bucket_list = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)
    bucket_df = list_to_pyspark_df(spark, bucket_list)

    ########################################################
    ## 5. remove validation script from PySpark dataframe ##
    ########################################################
    remove_value_location = get_script_prefix(target_prefix, "s3_to_s3_validation_script.py")
    bucket_df = remove_script_from_df(bucket_df, remove_value_location, "path")

    #####################################################
    ## 6. Prepare and do comparisons on two dataframes ##
    #####################################################
    # Store bucket dataframe with different columns name to avoid conflict when comparing two dataframes, which have duplicate names
    rename_cols = {"size": "b_size","path": "b_path"}
    bucket_df_renamed = rename_columns(bucket_df, **rename_cols)

    # Get missing dataframe, values in file_df not in bucket_df
    missing_df = get_missing_objects(file_df, bucket_df_renamed, "path", "b_path")
    missing_count = get_df_count(missing_df)
    print(f'missing s3 objects number: {missing_count}')

    # Get match dataframe
    match_df = get_match_objects(file_df, bucket_df_renamed,
                                    "path", "size", "b_path", "b_size", "date")

    # Get wrong size dataframe
    wrong_size_df = get_wrong_size_objects(match_df, "size", "b_size")
    wrong_size_count = get_df_count(wrong_size_df)
    print(f'wrong size s3 objects number: {wrong_size_count}')

    #####################################################################################
    ## 7. Save validation result to Target S3 with the same level as the Target folder ##
    #####################################################################################
    obj_name = "missing"
    missing_message = save_result(missing_count, result_location, current, missing_df, obj_name)
    obj_name = "wrong_size"
    wrong_size_message = save_result(wrong_size_count, result_location, current, wrong_size_df, obj_name)

    #################################################
    ## 8. Send out notification to SNS subscribers ##
    #################################################
    send_sns_to_subscriber(target_bucket, target_prefix, current, sns_client, sns_topic_arn, missing_message, wrong_size_message)

if __name__ == "__main__":
    """
    Start execution
    """
    # Start execution
    totalstart = time.time()
    main()
    # The end of the validaiton execution
    totalend = time.time()
    print(f"total execution time: {(totalend-totalstart):.06f}s")
    print("end of job")
    print("Executin completed.")
