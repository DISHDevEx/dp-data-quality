"""
!/usr/bin/env python3
#--------------------------------------------------------------------------------
# File    :   s3-validation-with-glue-prod
# Time    :   2022/11/28 13:52:01
# Author  :   Zheng Liu
# Version :   1.0
# Desc    :   create glue job to read file from S3 and scan S3 and then
#             do validation before sending the result to SNS and saving in S3.

---------------------------Version History---------------------------------------
SrNo    DateModified    ModifiedBy   Description
1       2022/11/28      Zheng        Initial Version
2       2022/12/21      Zheng        Add comment on adf and bdf, delete atbdf and btbdf, convert result to pandas dataframe
                                     to save in one file without extra folder, catch empty dataframe error
3       2023/01/26      Zheng        Wrap up for GitHub pull
4       2023/01/30      Zheng        Change code to OOP style
5       2023/01/31      Zheng        Generate Pytest test cases
6       2023/02/01      Zheng        Update functions
7       2023/02/02      Zheng        Update Pytest test cases
8       2023/02/07      Zheng        Code reviewer suggest using functional programming
#--------------------------------------------------------------------------------
"""
# Steps:
# 1. Setup basic arguements for s3 to s3 validation
# 2. Get initial arguements from Glue Job sys and other helper functions
# 3. Read file into PySpark dataframe
# 4. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe
# 5. remove validation script from PySpark dataframe
# 6. Prepare and do comparisons on two dataframes
# 7. Save validation result to Target S3 with the same level as the Target folder
# 8. Send out notification to SNS subscribers

# Please comment from awsglue.utils import getResolvedOptions and from awsglue.context import GlueContext,
# if using pytest with this file

import sys
import json
from datetime import datetime
import time
import boto3
import pytz
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *

def get_target_location():
    """
    Get target location to scan s3 objects
    """
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job_name = args['JOB_NAME']
        target_bucket_and_prefix = job_name
        target_bucket = target_bucket_and_prefix.split("/")[0]
        target_prefix = target_bucket_and_prefix.replace(target_bucket, "")[1:]
    except RuntimeError:
        sys.exit("cannot get target_bucket and target_prefix")
    else:
        return target_bucket, target_prefix
    finally:
        print('get_target_location section done')

def get_file_location(trigger_bucket_name, trigger_path_name):
    """
    Get the file location, which triggers this validation
    """
    # bucket_name is "s3_bucket" and path_name is "s3_path" in this product
    try:
        args = getResolvedOptions(sys.argv, [trigger_bucket_name, trigger_path_name])
        file_bucket = args[trigger_bucket_name]
        file_prefix = args[trigger_path_name]
        file_name = file_prefix.split("/")[-1]
        print(f'bucket from lambda: {file_bucket}')
        print(f'path from lambda: {file_prefix}')
        print(f'file_name: {file_name}')
    except RuntimeError:
        print('cannot get_file_location')
        sys.exit("cannot get_file_location")
    else:
        # file_name should be 's3_to_s3_validation.csv' unless there is a change
        # Stop if the file is not for validation
        if file_name != 's3_to_s3_validation.csv':
            sys.exit("not for s3 to s3 validation")
        return file_bucket, file_prefix
    finally:
        print('get_file_location section done')

def get_current_denver_time(time_zone, time_format):
    """
    Get current Devner local time as timestamp
    """
    # use '%Y-%m-%d_%H-%M-%S_%Z_%z'for time_format in this validation product
    # use 'US/Mountain' for time_zone of Denver
    try:
        denver_time = pytz.timezone(time_zone)
        datetime_den = datetime.now(denver_time)
        current = datetime_den.strftime(time_format)
    except ValueError:
        print('Wrong time_zone or time_format value')
    except TypeError:
        print('Wrong time_zone or time_format type')
    except RuntimeError:
        print('cannot get_current_denver_time')
    else:
        print(f'current Denver time: {current}')
        return current
    finally:
        print('get_current_denver_time section done')

def generate_result_location(target_bucket, target_prefix):
    """
    Generate result saving location based on target_bucket and target_prefix
    """
    try:
        target_prefix_no_slash = target_prefix.replace("/", "_")
        result_location = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_{target_prefix_no_slash}/"
    except AttributeError as e:
        print(e)
    except RuntimeError:
        print('cannot generate result location')
    else:
        return result_location
    finally:
        print('generate_result_location seciton done')

def initial_pyspark():
    """
    Initial spark
    """
    try:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
    except RuntimeError:
        sys.exit("no spark available")
    else:
        return spark
    finally:
        print('initial_boto3_client section done')

def initial_boto3_client(aws_service):
    """
    Initial boto3 client for aws service
    """
    try:
        the_client = boto3.client(aws_service)
    except RuntimeError:
        sys.exit("no boto3 client available")
    else:
        return the_client
    finally:
        print('initial_boto3_client section done')

def initial_boto3_resource(aws_service):
    """
    Initial boto3 resource for aws service
    """
    try:
        the_resource = boto3.resource(aws_service)
    except RuntimeError:
        sys.exit("no boto3 resource available")
    else:
        return the_resource
    finally:
        print('initial_boto3_resource section done')

def get_sns_name(target_bucket):
    """
    Get sns name based on target_bucket
    """
    try:
        sns_name = target_bucket.replace(".", "")
    except AttributeError as e:
        print(e)
        return None
    except RuntimeError:
        print('cannot get sns_name')
        return None
    else:
        return sns_name
    finally:
        print('get_sns_name section done')

def get_sns_arn(sns_client, sns_name):
    """
    Get sns arn from sns name
    """
    try:
        sns_topic_arn = [tp['TopicArn'] for tp in sns_client.list_topics()['Topics'] if sns_name in tp['TopicArn']][0]
    except AttributeError as e:
        print(e)
        return None
    except RuntimeError:
        print('cannot get sns_topic_arn')
        return None
    else:
        print(f'sns_topic_arn: {sns_topic_arn}')
        return sns_topic_arn
    finally:
        print('get_sns_arn seciton done')

def sns_send(sns_client, sns_topic_arn, message, subject):
    """
    Module to sent out sns api call
    """
    try:
        response = sns_client.publish(
                TargetArn=sns_topic_arn,
                Message=json.dumps({'default': json.dumps(message, indent = 6)}),
                Subject=subject,
                MessageStructure='json')
    except AttributeError as e:
        print(e)
    except TypeError as e:
        print(e)
    except ValueError as e:
        print(e)
    except RuntimeError:
        print(f'{message} under {subject} cannot be sent to sns')
        return None
    else:
        print(f'{message} under {subject} is sent to sns')
        return response
    finally:
        print(f'section of {message} under {subject} to sns finished')

def rename_columns(df, **kwargs):
    """
    Rename columns in a pyspark dataframe
    """
    # In this product:
    #   **kwargs -> {"Size": "bSize","Path": "bPath"} for bucket_df
    try:
        for key, value in kwargs.items():
            df = df.withColumnRenamed(key, value)
    except AttributeError as e:
        print(e)
    except TypeError as e:
        print(e)
    except RuntimeError:
        print('cannot rename columns in this dataframe')
    else:
        print(f'rename {kwargs} df done:::')
        df.show(truncate=False)
        return df
    finally:
        print('rename_columns seciton done')

def file_to_pyspark_df(spark, file_bucket, file_prefix, schema_str):
    """
    Generate a pyspark dataframe from a csv file
    """
    starttime = time.time()
    try:
        file_df = spark.read\
            .format("csv")\
            .option("header", "false")\
            .schema(schema_str)\
            .load(f"s3a://{file_bucket}/{file_prefix}")
    except RuntimeError:
        print('file_to_pyspark_df return None')
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
    Generate a pyspark datafram by scanning objects under target folder in target s3 bucket
    """
    starttime = time.time()
    s3_bucket = s3_resource.Bucket(target_bucket)
    obj_counter = 0
    obj_list = []
    try:
        for obj in s3_bucket.objects.filter(Prefix=target_prefix):
            # time_format can be '%Y-%m-%d_%H-%M-%S_%Z_%z' in this product
            key_size_date_dict = {'Path':obj.key.strip(), 'Size':obj.size, \
                'Date':obj.last_modified.strftime(time_format)}
            obj_list.append(key_size_date_dict)
            obj_counter += 1
            print(f'fetching {obj_counter} object in target folder')
    except RuntimeError:
        print('s3_obj_to_list return None')
        return None
    else:
        endtime = time.time()
        print(f"s3 objects list read in time: {(endtime-starttime):.06f}s")
        return obj_list
    finally:
        print('scan s3 section done')

def list_to_pyspark_df(spark, obj_list):
    """
    Generate a pyspark dataframe by reading in a list
    """
    try:
        pyspark_df = spark.createDataFrame(obj_list)
    except RuntimeError:
        print('list_to_pyspark_df return None')
        return None
    except TypeError:
        print('list_to_pyspark_df TypeError return None')
        return None
    else:
        pyspark_df.show()
        return pyspark_df
    finally:
        print('list_to_pyspark_df function done:::')

def get_script_prefix(target_prefix, script_file_name):
    """
    Get validation script prefix in target bucket
    """
    try:
        if target_prefix[-1]=="/":
            script_prefix = target_prefix+script_file_name
        else:
            script_prefix = target_prefix+"/"+script_file_name
    except AttributeError as e:
        print(e)
    except RuntimeError:
        print('cannot get_script_prefix')
    else:
        return script_prefix
    finally:
        print('get_script_prefix section done')

def remove_script_from_df(pyspark_df, remove_value, column_name):
    """
    Remove script prefix/path from the dataframe
    """
    # In this product
    #   pyspark_df -> bucket_df
    #   column_name -> Path
    try:
        pyspark_df_updated = pyspark_df.filter(pyspark_df[column_name]!=remove_value)
    except AttributeError as e:
        print(e)
        return pyspark_df
    except RuntimeError:
        print('cannot remove script from df')
        return pyspark_df
    else:
        print(f'after remove value {remove_value} :::')
        pyspark_df_updated.show(truncate=False)
        return pyspark_df_updated
    finally:
        print('remove_script_from_df section done')

def get_missing_objects(df_1, df_2, df_1_column, df_2_column):
    """
    Generate pyspark dataframe for missing objects
    """
    # In this product:
    #   df_1 -> file_df
    #   df_2 -> bucket_df_renamed
    #   df_1_column -> Path
    #   df_2_column -> bPath
    try:
        join_expr = df_1[df_1_column] == df_2[df_2_column]
        joinType = "anti"
        missing_df = df_1.join(df_2, join_expr, joinType)
        print('missing_df:::')
        missing_df.show(truncate=False)
    except AttributeError as e:
        print(e)
        return None
    except RuntimeError as e:
        print(e)
        return None
    else:
        return missing_df
    finally:
        print('getting missing objects section done')

def get_df_count(pypark_df):
    """
    Count number of rows from a pyspark dataframe
    """
    try:
        df_count = pypark_df.count()
    except AttributeError as e:
        print(e)
        return None
    except RuntimeError:
        print('cannot count dataframe rows')
        return None
    else:
        print('Dataframe count:')
        print(df_count)
        return df_count
    finally:
        print('getting rows in a pyspark dataframe section done')

def get_match_objects(df_1, df_2, df_1_column, df_1_column_1,\
        df_2_column, df_2_column_1, df_2_column_2):
    """
    Generate pyspark dataframe for matched objects
    """
    # In this product:
    #   df_1 -> file_df
    #   df_2 -> bucket_df_renamed
    #   df_1_column -> Path
    #   df_1_column_1 -> Size
    #   df_2_column -> bPath
    #   df_2_column_1 -> bSize
    #   df_2_column_2 -> Date
    try:
        join_expr = df_1[df_1_column] == df_2[df_2_column]
        joinType = "inner"
        match_df = df_1.join(df_2, join_expr, joinType).select(df_1[df_1_column], \
            df_1[df_1_column_1], df_2[df_2_column_1], df_2[df_2_column_2],)
    except AttributeError as e:
        print(e)
        return None
    except RuntimeError:
        print("cannot generate pyspark dataframe for matching objects")
        return None
    else:
        print('match number:')
        print(match_df.count())
        match_df.printSchema()
        print('match_df:::')
        match_df.show(truncate=False)
        return match_df
    finally:
        print('getting match objects in a pyspark dataframe section done')

def get_wrong_size_objects(df, df_column_1, df_column_2):
    """
    Generate pyspark dataframe for wrong size object
    """
    # In this product:
    #   df -> match_df
    #   df_column_1 -> Size
    #   df_column_2 -> bSize
    try:
        wrong_size_df = df.filter(df[df_column_1]!=df[df_column_2])
    except AttributeError as e:
        print(e)
        return None
    except RuntimeError:
        print("cannot genereate pyspark dataframe for wrong size objects")
        return None
    else:
        print('wrong_size_df:::')
        wrong_size_df.show(truncate=False)
        return wrong_size_df
    finally:
        print('getting wrong size objects in a pyspark dataframe section done')

def save_result(row_count, result_location, current, df, obj_name):
    """
    Save result
    """
    try:
        if row_count > 0:
            savepath = f"{result_location}{obj_name}_{current}.csv"
            message = f"saved at {result_location[6:]}_{obj_name}_{current}.csv"
            try:
                df.toPandas().to_csv(savepath, index = False)
                message = "result saved"
            except AttributeError as e:
                print(e)
                return e
            except RuntimeError:
                print(f"{obj_name} object cannot be save in S3.")
                message = f"no {obj_name} item found"
                return message
            else:
                message = "result saved"
                return message
            finally:
                print('section of saving objects is done')
        else:
            print(f"no {obj_name} object")
            message = f"no {obj_name} item found"
            return message
    except AttributeError as e:
        print(e)
    except RuntimeError:
        print('cannot save result')
        return message
    finally:
        print('save_result section done')

def result_to_subscriber(target_bucket, target_prefix, current, sns_client, sns_topic_arn, missing_message, wrong_size_message):
    """
    Sent email to sns subscriber
    """
    try:
        message = {"Missing items: ":missing_message,"Wrong size objects: ":wrong_size_message,"Validation started at: ":current}
        subject = f'{target_prefix} {target_bucket} validation done'
        response = sns_client.publish(
                TargetArn=sns_topic_arn,
                Message=json.dumps({'default': json.dumps(message, indent = 6)}),
                Subject=subject,
                MessageStructure='json')
    except AttributeError as e:
        print(e)
        return e
    except RuntimeError:
        message = "cannot send message to sns"
        print(message)
        return message
    else:
        print(response)
        return response
    finally:
        print('senting to sns section done')

def main():
    """
    main
    """

    #######################################################
    ## 1. Setup basic arguements for s3 to s3 validation ##
    #######################################################
    trigger_bucket_name = 's3_bucket'
    trigger_path_name = 's3_path'
    time_zone = 'US/Mountain'
    time_format = '%Y%m%d_%H%M%S_%Z_%z'
    aws_sns_client = 'sns'
    aws_s3_resource = 's3'

    ############################################################################
    ## 2. Get initial arguements from Glue Job sys and other helper functions ##
    ############################################################################
    target_bucket, target_prefix = get_target_location()
    file_bucket, file_prefix = get_file_location(trigger_bucket_name, trigger_path_name)
    current = get_current_denver_time(time_zone, time_format)
    spark = initial_pyspark()
    s3_resource = initial_boto3_resource(aws_s3_resource)
    sns_client = initial_boto3_client(aws_sns_client)
    sns_name = get_sns_name(target_bucket)
    sns_topic_arn = get_sns_arn(sns_client, sns_name)
    result_location = generate_result_location(target_bucket, target_prefix)

    #########################################
    ## 3. Read file into PySpark dataframe ##
    #########################################
    schema_str = 'Site string, Assessment string, Path string, Size long'

    file_df = file_to_pyspark_df(spark, file_bucket, file_prefix, schema_str)
    if file_df is None:
        error_msg = {f"s3a://{file_bucket}/{file_prefix} :":" is not a valid \
            manifest file path for validation","Validation started at: ":current}
        subject = f'{target_prefix} {target_bucket} file fail'
        response = sns_send(sns_client, sns_topic_arn, error_msg, subject)
        print(error_msg)
        print(response)
        sys.exit("cannot generate file pyspark dataframe")

    ###########################################################################################################################
    ## 4. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe ##
    ###########################################################################################################################
    bucket_list = s3_obj_to_list(s3_resource, target_bucket, target_prefix, time_format)
    if bucket_list is None:
        error_msg = {f"s3a://{target_bucket}/{target_prefix} :":" \
            is not a valid S3 scanning path for validation","Validation started at: ":current}
        subject = f'{target_prefix} {target_bucket} scan fail'
        response = sns_send(sns_client, sns_topic_arn, error_msg, subject)
        print(error_msg)
        print(response)
        sys.exit("cannot generate s3 object pyspark dataframe")
    bucket_df = list_to_pyspark_df(spark, target_bucket, target_prefix,\
         current, sns_client, sns_topic_arn, sns_send, bucket_list)
    if bucket_df is None:
        error_msg = {f"s3a://{target_bucket}/{target_prefix} :":" \
            cannot turn bucket_list to bucket_df","Validation started at: ":current}
        subject = f'{target_prefix} {target_bucket} list to dataframe fail'
        response = sns_send(sns_client, sns_topic_arn, error_msg, subject)
        print(error_msg)
        print(response)

    ########################################################
    ## 5. remove validation script from PySpark dataframe ##
    ########################################################
    remove_value_location = get_script_prefix(target_prefix, "s3_to_s3_validation_script.py")
    print(bucket_df.count())
    bucket_df = remove_script_from_df(bucket_df, remove_value_location, "Path")
    print(bucket_df.count())

    #####################################################
    ## 6. Prepare and do comparisons on two dataframes ##
    #####################################################
    # Store bucket dataframe with different columns name to avoid conflict when comparing two dataframes, which have duplicate names
    rename_cols = {"Size": "bSize","Path": "bPath"}
    bucket_df_renamed = rename_columns(bucket_df, **rename_cols)
    if bucket_df_renamed is None:
        sys.exit("cannot generate renamed s3 object pyspark dataframe")

    # Get missing dataframe, values in file_df not in bucket_df
    missing_df = get_missing_objects(file_df, bucket_df_renamed, "Path", "bPath")
    if missing_df is None:
        print("cannot generate pyspark dataframe for missing s3 objects")
    else:
        missing_count = get_df_count(missing_df)
        print(f'missing s3 objects number: {missing_count}')

    # Get match dataframe
    match_df = get_match_objects(file_df, bucket_df_renamed,
                                    "Path", "Size", "bPath", "bSize", "Date")

    # Get wrong size dataframe
    if match_df is not None:
        wrong_size_df = get_wrong_size_objects(match_df, "Size", "bSize")
        if wrong_size_df is not None:
            wrong_size_count = get_df_count(wrong_size_df)
            print(f'wrong size s3 objects number: {wrong_size_count}')
    else:
        print("because there is no match_df, no need to get wrong_size_df")

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
    result_to_subscriber(target_bucket, target_prefix, current, sns_client, sns_topic_arn, missing_message, wrong_size_message)

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