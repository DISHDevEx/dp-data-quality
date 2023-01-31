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
#--------------------------------------------------------------------------------
"""
# Steps:
# 1. Get location of the file, which is passed here from LambdaAsTrigger
# 2. Read local time as time stamp
# 3. Get location of the target bucket and target folder from this GlueJob Name
# 4. Generate result saving location based on values of target bucket and target folder
# 5. Initial PySpark
# 6. Read file into PySpark dataframe
# 7. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe
# 8. Prepare and do comparisons on two dataframes
# 9. Save validation result to Target S3 with the same level as the Target folder
# 10. Send out notification to SNS subscribers

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

print("all lib imported")

# Create helper functions in Validation class:
class Validation:
    """
    Validaiton class is for s3 to s3 validaiton in this product
    """
    def __init__(self, target_bucket=None, target_prefix=None, path_lambda_trigger_by_file=None):
        """
        Object constructor
        """

    def get_target_location(self):
        """
        Get target location to scan s3 objects
        """
        try:
            args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            job_name = args['JOB_NAME']
            target_bucket_and_prefix = job_name
            target_bucket = target_bucket_and_prefix.split("/")[0]
            target_prefix = target_bucket_and_prefix.replace(target_bucket, "")[1:]
            return target_bucket, target_prefix
        except:
            sys.exit("cannot get target_bucket and target_prefix")

    def get_file_location(self, bucket_name, path_name, file_name='s3_to_s3_validation.csv'):
        """
        Get the file location, which triggers this validation
        """
        # bucket_name is "s3_bucket" and path_name is "s3_path" in this product
        # file_name should be 's3_to_s3_validation.csv' unless there is a change
        if file_name != 's3_to_s3_validation.csv':
            sys.exit("not for s3 to s3 validation")
        args = getResolvedOptions(sys.argv, [bucket_name, path_name])
        bucket_lambda_trigger_by_file = args[bucket_name]
        path_lambda_trigger_by_file = args[path_name]
        print(f'bucket from lambda: {bucket_lambda_trigger_by_file}')
        print(f'path from lambda: {path_lambda_trigger_by_file}')
        # Stop if the file is not for validation
        file_name = path_lambda_trigger_by_file.split("/")[-1]
        print('file_name')
        print(file_name)
        return bucket_lambda_trigger_by_file, path_lambda_trigger_by_file

    def get_current_denver_time(self, time_zone, time_format):
        """
        Get current Devner local time as timestamp
        """
        # use '%Y-%m-%d_%H-%M-%S_%Z_%z'for time_format in this validation product
        # use 'US/Mountain' for time_zone of Denver
        denver_time = pytz.timezone(time_zone)
        datetime_den = datetime.now(denver_time)
        current = datetime_den.strftime(time_format)
        print(f'current Denver time: {current}')
        return current

    def generate_result_location(self, target_bucket, target_prefix):
        """
        Generate result saving location based on target_bucket and target_prefix
        """
        target_prefix_no_slash = target_prefix.replace("/", "_")
        result_location = f"s3a://{target_bucket}/{target_prefix}/s3_to_s3_validation_result_{target_bucket}_{target_prefix_no_slash}/"
        return result_location

    def initial_pyspark(self):
        """
        Initial spark
        """
        try:
            sc = SparkContext()
            glueContext = GlueContext(sc)
            spark = glueContext.spark_session
            return spark
        except:
            sys.exit("no spark available")

    def initial_boto3_client(self, aws_service):
        """
        Initial boto3 client for aws service
        """
        try:
            return boto3.client(aws_service)
        except:
            sys.exit("no boto3 client available")

    def initial_boto3_resource(self, aws_service):
        """
        Initial boto3 resource for aws service
        """
        try:
            return boto3.resource(aws_service)
        except:
            sys.exit("no boto3 resource available")

    def get_sns_name(self, target_bucket):
        """
        Get sns name based on target_bucket
        """
        sns_name = target_bucket.replace(".", "")
        return sns_name

    def get_sns_arn(self, sns_client, sns_name):
        """
        Get sns arn from sns name
        """
        sns_topic_arn = [tp['TopicArn'] for tp in sns_client.list_topics()['Topics'] if sns_name in tp['TopicArn']][0]
        print(f'sns_topic_arn: {sns_topic_arn}')
        return sns_topic_arn

    def file_to_pyspark_df(self, spark, bucket_lambda_trigger_by_file, path_lambda_trigger_by_file, target_bucket, \
        target_prefix, current, sns_client, sns_topic_arn):
        """
        Generate a pyspark dataframe from a csv file
        """
        starttime = time.time()
        try:
            file_df = spark.read \
                .format("csv") \
                .option("header", "false") \
                .option("inferSchema", "true") \
                .load(f"s3a://{bucket_lambda_trigger_by_file}/{path_lambda_trigger_by_file}")
            file_df = file_df.withColumnRenamed("_c0", "Site").withColumnRenamed("_c1", "Accessment") \
                .withColumnRenamed("_c2", "Path").withColumnRenamed("_c3", "Size")
            file_df.show()
            endtime = time.time()
            print(f"csv dataframe read in time: {(endtime-starttime):.06f}s")
            return file_df
        except:
            error_msg = {f"s3a://{bucket_lambda_trigger_by_file}/{path_lambda_trigger_by_file} :": \
                " is not a valid manifest file path for validation","Validation started at: ":current}
            print(error_msg)
            subject = f'{target_prefix} {target_bucket} file fail'
            response = sns_client.publish(
                    TargetArn=sns_topic_arn,
                    Message=json.dumps({'default': json.dumps(error_msg, indent = 6)}),
                    Subject=subject,
                    MessageStructure='json')
            print('bad input file path sns response:')
            print(response)
            return None

    def list_to_pyspark_df(self, spark, obj_list, target_bucket, target_prefix, current, sns_client, sns_topic_arn):
        """
        Generate a pyspark dataframe by reading in a list
        """
        try:
            bucket_df = spark.createDataFrame(obj_list)
            bucket_df.show()
            return bucket_df
        except:
            error_msg = {f"s3a://{target_bucket}/{target_prefix} :":" \
                is not a valid S3 scanning path for validation","Validation started at: ":current}
            print(error_msg)
            subject = f'{target_prefix} {target_bucket} scan fail'
            response = sns_client.publish(
                    TargetArn=sns_topic_arn,
                    Message=json.dumps({'default': json.dumps(error_msg, indent = 6)}),
                    Subject=subject,
                    MessageStructure='json')
            print('can not generate pyspark dataframe for s3 objects in list_to_pyspark_df')
            print(response)
            return None

    def s3_obj_to_pyspark_df(self, spark, s3_resource, target_bucket, target_prefix, time_format, current, sns_client, sns_topic_arn):
        """
        Generate a pyspark datafram by scanning objects under target folder in target s3 bucket
        """
        starttime = time.time()
        s3_bucket = s3_resource.Bucket(target_bucket)
        obj_counter = 0
        obj_list = []
        for obj in s3_bucket.objects.filter(Prefix=target_prefix):
            # time_format can be '%Y-%m-%d_%H-%M-%S_%Z_%z' in this product
            key_size_date_dict = {'Path':obj.key.strip(), 'Size':obj.size, 'Date':obj.last_modified.strftime(time_format)}
            obj_list.append(key_size_date_dict)
            obj_counter += 1
            print(f'fetching {obj_counter} object in target folder')

        bucket_df = self.list_to_pyspark_df(spark, obj_list, target_bucket, target_prefix, current, sns_client, sns_topic_arn)
        if bucket_df is None:
            print("can not generate pyspark dataframe for s3 objects in s3_obj_to_pyspark_df")
            return None
        endtime = time.time()
        print(f"s3 objects dataframe read in time: {(endtime-starttime):.06f}s")
        return bucket_df

    def rename_bucket_df(self, bucket_df):
        """
        Rename column so column names are unique from two dataframes
        """
        try:
            bucket_df_renamed = bucket_df.withColumnRenamed("size", "bSize").withColumnRenamed("Path", "bPath")
            bucket_df_renamed.show()
            return bucket_df_renamed
        except:
            return None

    def get_missing_objects(self, file_df, bucket_df_renamed):
        """
        Generate pyspark dataframe for missing objects
        """
        try:
            join_expr = file_df.Path == bucket_df_renamed.bPath
            joinType = "anti"
            missing_df = file_df.join(bucket_df_renamed, join_expr, joinType)
            missing_df.show()
            return missing_df
        except:
            return None

    def get_df_count(self, df):
        """
        Count number of rows from a pyspark dataframe
        """
        try:
            df_count = df.count()
            print('Dataframe count:')
            print(df_count)
            return df_count
        except:
            print('cannot count dataframe rows')
            return None

    def get_match_objects(self, file_df, bucket_df_renamed):
        """
        Generate pyspark dataframe for matched objects
        """
        try:
            join_expr = file_df.Path == bucket_df_renamed.bPath
            joinType = "inner"
            match_df = file_df.join(bucket_df_renamed, join_expr, joinType).select(file_df.Path, file_df.Site, \
                file_df.Size, bucket_df_renamed.bSize, bucket_df_renamed.Date,)
            match_df.show()

            print('match number:')
            print(match_df.count())
            match_df.printSchema()
            return match_df
        except:
            print("cannot generate pyspark dataframe for matching objects")
            return None

    def get_wrong_size_objects(self, match_df):
        """
        Generate pyspark dataframe for wrong size object
        """
        try:
            wrong_size_df = match_df.filter(match_df["Size"]!=match_df["bSize"])
            wrong_size_df.show()
            return wrong_size_df
        except:
            print("cannot genereate pyspark dataframe for wrong size objects")
            return None

    def save_result(self, row_count, result_location, current, df):
        """
        Save result
        """
        if row_count > 0:
            savepath = f"{result_location}missing_{current}.csv"
            message = f"saved at {result_location[6:]}_missing_{current}.csv"
            df.toPandas().to_csv(savepath, index = False)
            message = "result saved"
        else:
            print("no missing object")
            message = "no missing item found"
        print(message)
        return message

    def result_to_subscriber(self, missing_message, wrong_size_message, current, target_prefix, target_bucket, sns_client, sns_topic_arn):
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
            print(response)
            return response
        except:
            print("cannot send message to sns")
            return None

def main():
    """
    Start execution
    """
    # Start execution
    totalstart = time.time()
    validation_obj = Validation()

    ############################################################################
    ## 1. Get location of the file, which is passed here from LambdaAsTrigger ##
    ############################################################################
    bucket_lambda_trigger_by_file, path_lambda_trigger_by_file = validation_obj.get_file_location("s3_bucket", "s3_path")

    ######################################
    ## 2. Read local time as time stamp ##
    ######################################
    current = validation_obj.get_current_denver_time('US/Mountain', '%Y-%m-%d_%H-%M-%S_%Z_%z')

    ###################################################################################
    ## 3. Get location of the target bucket and target folder from this GlueJob Name ##
    ###################################################################################
    target_bucket, target_prefix = validation_obj.get_target_location()

    ###########################################################################################
    ## 4. Generate result saving location based on values of target bucket and target folder ##
    ###########################################################################################
    result_location = validation_obj.generate_result_location(target_bucket, target_prefix)

    ###########################################################
    ## 5. Initial PySpark, SNS client, S3 client and SNS arn ##
    ###########################################################
    # Initial PySpark
    spark = validation_obj.initial_pyspark()
    # Initial SNS client
    sns_client = validation_obj.initial_boto3_client('sns')
    # Initial S3 resource and S3 client
    s3_resource = validation_obj.initial_boto3_resource('s3')
    # Get SNS name for SNS arn
    sns_name = validation_obj.get_sns_name(target_bucket)
    # Prepare SNS arn
    sns_topic_arn = validation_obj.get_sns_arn(sns_client, sns_name)

    #########################################
    ## 6. Read file into PySpark dataframe ##
    #########################################
    file_df = validation_obj.file_to_pyspark_df(spark, bucket_lambda_trigger_by_file, path_lambda_trigger_by_file, target_bucket, \
        target_prefix, current, sns_client, sns_topic_arn)
    if file_df is None:
        sys.exit("cannot generate file pyspark dataframe")

    ###########################################################################################################################
    ## 7. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe ##
    ###########################################################################################################################
    time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
    bucket_df = validation_obj.s3_obj_to_pyspark_df(spark, s3_resource, target_bucket, target_prefix, time_format,\
         current,sns_client, sns_topic_arn)
    if bucket_df is None:
        sys.exit("cannot generate s3 object pyspark dataframe")

    #####################################################
    ## 8. Prepare and do comparisons on two dataframes ##
    #####################################################
    # Extract dish site id from path and save as another column from bucket dataframe
    bucket_df = validation_obj.extract_dish_site_id(bucket_df, "Site", "Path", "/", 3)
    if bucket_df is None:
        sys.exit("cannot extract dish site id from bucket dataframe")

    # Get unique dish site id to use as unique index to compare in the following functions from file dataframe
    bucket_number = validation_obj.get_unique_dish_site_id(file_df, "Site")
    if bucket_number is None:
        sys.exit("cannot get unique dish site id")

    # Store bucket dataframe with different columns name to avoid conflict when comparing two dataframes, which have duplicate names
    bucket_df_renamed = validation_obj.rename_bucket_df(bucket_df)
    if bucket_df_renamed is None:
        sys.exit("cannot generate renamed s3 object pyspark dataframe")

    # Get missing dataframe, values in file_df not in bucket_df
    missing_df = validation_obj.get_missing_objects(file_df, bucket_df_renamed)
    if missing_df is None:
        print("cannot generate pyspark dataframe for missing s3 objects")
    else:
        missing_count = validation_obj.get_df_count(missing_df)
        print(f'missing s3 objects number: {missing_count}')

    # Get match dataframe
    match_df = validation_obj.get_match_objects(file_df, bucket_df_renamed)

    # Get wrong size dataframe
    if match_df is not None:
        wrong_size_df = validation_obj.get_wrong_size_objects(match_df)
        if wrong_size_df is not None:
            wrong_size_count = validation_obj.get_df_count(wrong_size_df)
            print(f'wrong size s3 objects number: {wrong_size_count}')
    else:
        print("because there is no match_df, no need to get wrong_size_df")

    #####################################################################################
    ## 9. Save validation result to Target S3 with the same level as the Target folder ##
    #####################################################################################
    missing_message = validation_obj.save_result(missing_count, result_location, current, missing_df)
    wrong_size_message = validation_obj.save_result(wrong_size_count, result_location, current, wrong_size_df)

    ##################################################
    ## 10. Send out notification to SNS subscribers ##
    ##################################################
    validation_obj.result_to_subscriber(missing_message, wrong_size_message, current, \
        target_prefix, target_bucket, sns_client, sns_topic_arn)

    # The end of the validaiton execution
    totalend = time.time()
    print(f"total execution time: {(totalend-totalstart):.06f}s")
    print("end of job")

if __name__ == "__main__":
    main()
    print("Executin completed.")
