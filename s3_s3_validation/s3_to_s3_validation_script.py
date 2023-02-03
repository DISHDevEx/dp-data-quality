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
8       2023/02/04      Zheng        Update Pytest test cases
#--------------------------------------------------------------------------------
"""
# Steps:
# 1. Create validation object with its attributes
# 2. Generate result saving location based on values of target bucket and target folder
# 3. Initial PySpark
# 4. Read file into PySpark dataframe
# 5. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe
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

print("all lib imported")

# Create helper functions in Validation class:
class Validation:
    """
    Validaiton class is for s3 to s3 validaiton in this product
    """
    def __init__(self, target_bucket=None, target_prefix=None, file_bucket=None,\
        file_prefix=None, current=None, spark=None, sns_client=None, s3_resource=None,\
        sns_name=None, sns_topic_arn=None, time_zone=None, time_format=None):
        """
        Object constructor
        """
        self.target_bucket = target_bucket
        self.target_prefix = target_prefix
        self.file_bucket = file_bucket
        self.file_prefix = file_prefix
        self.current = current
        self.spark = spark
        self.sns_client = sns_client
        self.s3_resource = s3_resource
        self.sns_name = sns_name
        self.sns_topic_arn = sns_topic_arn
        self.time_zone=time_zone
        self.time_format=time_format


    @classmethod
    def create_object(cls, trigger_bucket_name, trigger_path_name, time_zone, time_format, aws_sns_client, aws_s3_resource):
        """
        Create object
        """
        # In this product:
        #   trigger_bucket_name -> s3_bucket
        #   trigger_path_name -> s3_path
        #   time_zone -> US/Mountain
        #   time_format -> %Y-%m-%d_%H-%M-%S_%Z_%z
        #   aws_sns_client -> sns
        #   aws_s3_resource -> s3
        try:
            target_bucket, target_prefix = cls.get_target_location()
            file_bucket, file_prefix = cls.get_file_location(trigger_bucket_name, trigger_path_name)
            current = cls.get_current_denver_time(time_zone, time_format)
            spark = cls.initial_pyspark()
            sns_client = cls.initial_boto3_client(aws_sns_client)
            s3_resource = cls.initial_boto3_resource(aws_s3_resource)
        except:
            print('cannot create a new validation object')
            sys.exit("cannot generate validation object")
        else:
            return cls(target_bucket, target_prefix, file_bucket, file_prefix, current, spark, sns_client, s3_resource)
        finally:
            print('craete_object section done')

    @staticmethod
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
        except:
            sys.exit("cannot get target_bucket and target_prefix")
        else:
            return target_bucket, target_prefix
        finally:
            print('get_target_location section done')

    @staticmethod
    def get_file_location(trigger_bucket_name, trigger_path_name):
        """
        Get the file location, which triggers this validation
        """
        # bucket_name is "s3_bucket" and path_name is "s3_path" in this product
        try:
            args = getResolvedOptions(sys.argv, [trigger_bucket_name, trigger_path_name])
            file_bucket = args[trigger_bucket_name]
            file_prefix = args[trigger_path_name]
            print(f'bucket from lambda: {file_bucket}')
            print(f'path from lambda: {file_prefix}')
            # file_name should be 's3_to_s3_validation.csv' unless there is a change
            # Stop if the file is not for validation
            file_name = file_prefix.split("/")[-1]
            print('file_name')
            print(file_name)
            if file_name != 's3_to_s3_validation.csv':
                sys.exit("not for s3 to s3 validation")
        except:
            print('cannot get_file_location')
            sys.exit("cannot get_file_location")
        else:
            return file_bucket, file_prefix
        finally:
            print('get_file_location section done')

    @staticmethod
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
        except:
            print('cannot get_current_denver_time')
        else:
            print(f'current Denver time: {current}')
            return current
        finally:
            print('get_current_denver_time section done')

    def generate_result_location(self):
        """
        Generate result saving location based on target_bucket and target_prefix
        """
        try:
            target_prefix_no_slash = self.target_prefix.replace("/", "_")
            result_location = f"s3a://{self.target_bucket}/s3_to_s3_validation_result_{self.target_bucket}_{target_prefix_no_slash}/"
        except:
            print('cannot generate result location')
        else:
            return result_location
        finally:
            print('generate_result_location seciton done')

    @staticmethod
    def initial_pyspark():
        """
        Initial spark
        """
        try:
            sc = SparkContext()
            glueContext = GlueContext(sc)
            spark = glueContext.spark_session
        except:
            sys.exit("no spark available")
        else:
            return spark
        finally:
            print('initial_boto3_client section done')

    @staticmethod
    def initial_boto3_client(aws_service):
        """
        Initial boto3 client for aws service
        """
        try:
            the_client = boto3.client(aws_service)
        except:
            sys.exit("no boto3 client available")
        else:
            return the_client
        finally:
            print('initial_boto3_client section done')

    @staticmethod
    def initial_boto3_resource(aws_service):
        """
        Initial boto3 resource for aws service
        """
        try:
            the_resource = boto3.resource(aws_service)
        except:
            sys.exit("no boto3 resource available")
        else:
            return the_resource
        finally:
            print('initial_boto3_resource section done')

    def get_sns_name(self):
        """
        Get sns name based on target_bucket
        """
        try:
            sns_name = self.target_bucket.replace(".", "")
        except:
            print('cannot get sns_name')
        else:
            return sns_name
        finally:
            print('get_sns_name section done')

    def get_sns_arn(self):
        """
        Get sns arn from sns name
        """
        try:
            sns_topic_arn = [tp['TopicArn'] for tp in self.sns_client.list_topics()['Topics'] if self.sns_name in tp['TopicArn']][0]
        except:
            print('cannot get sns_topic_arn')
        else:
            print(f'sns_topic_arn: {sns_topic_arn}')
            return sns_topic_arn
        finally:
            print('get_sns_arn seciton done')

    def sns_send(self, message, subject):
        """
        Module to sent out sns api call
        """
        try:
            response = self.sns_client.publish(
                    TargetArn=self.sns_topic_arn,
                    Message=json.dumps({'default': json.dumps(message, indent = 6)}),
                    Subject=subject,
                    MessageStructure='json')
        except:
            print(f'{message} under {subject} cannot be sent to sns')
            return None
        else:
            print(f'{message} under {subject} is sent to sns')
            return response
        finally:
            print(f'section of {message} under {subject} to sns finished')

    @staticmethod
    def rename_columns(df, **kwargs):
        """
        Rename columns in a pyspark dataframe
        """
        # In this product:
        #   **kwargs -> {"_c0": "Site","_c1": "Accessment","_c2": "Path","_c3": "Size"} for file_df
        #   **kwargs -> {"Size": "bSize","Path": "bPath"} for bucket_df
        try:
            for key, value in kwargs.items():
                df = df.withColumnRenamed(key, value)
        except:
            print('cannot rename columns in this dataframe')
        else:
            print(f'rename {kwargs} df done:::')
            df.show(truncate=False)
            return df
        finally:
            print('rename_columns seciton done')

    def file_to_pyspark_df(self, **kwargs):
        """
        Generate a pyspark dataframe from a csv file
        """
        starttime = time.time()
        try:
            file_df = self.spark.read\
                .format("csv")\
                .option("header", "false")\
                .option("inferSchema", "true")\
                .load(f"s3a://{self.file_bucket}/{self.file_prefix}")
        except:
            error_msg = {f"s3a://{self.file_bucket}/{self.file_prefix} :":" is not a valid \
                manifest file path for validation","Validation started at: ":self.current}
            subject = f'{self.target_prefix} {self.target_bucket} file fail'
            response = self.sns_send(error_msg, subject)
            print(error_msg)
            print(response)
            return None
        else:
            file_df = self.rename_columns(file_df, **kwargs)
            print('original file_df:::')
            file_df.show(truncate=False)
            endtime = time.time()
            print(f"csv dataframe read in time: {(endtime-starttime):.06f}s")
            return file_df
        finally:
            print('file_to_pyspark_df section done')

    def list_to_pyspark_df(self, obj_list):
        """
        Generate a pyspark dataframe by reading in a list
        """
        try:
            pyspark_df = self.spark.createDataFrame(obj_list)
        except:
            error_msg = {f"s3a://{self.target_bucket}/{self.target_prefix} :":" \
                is not a valid S3 scanning path for validation","Validation started at: ":self.current}
            subject = f'{self.target_prefix} {self.target_bucket} list to dataframe fail'
            response = self.sns_send(error_msg, subject)
            print(error_msg)
            print(response)
            return None
        else:
            pyspark_df.show()
            return pyspark_df
        finally:
            print('list_to_pyspark_df function done:::')

    def s3_obj_to_pyspark_df(self):
        """
        Generate a pyspark datafram by scanning objects under target folder in target s3 bucket
        """
        starttime = time.time()
        s3_bucket = self.s3_resource.Bucket(self.target_bucket)
        obj_counter = 0
        obj_list = []
        try:
            for obj in s3_bucket.objects.filter(Prefix=self.target_prefix):
                # time_format can be '%Y-%m-%d_%H-%M-%S_%Z_%z' in this product
                key_size_date_dict = {'Path':obj.key.strip(), 'Size':obj.size, \
                    'Date':obj.last_modified.strftime(self.time_format)}
                obj_list.append(key_size_date_dict)
                obj_counter += 1
                print(f'fetching {obj_counter} object in target folder')
        except:
            error_msg = {f"s3a://{self.target_bucket}/{self.target_prefix} :":" \
                is not a valid S3 scanning path for validation","Validation started at: ":self.current}
            subject = f'{self.target_prefix} {self.target_bucket} scan fail'
            response = self.sns_send(error_msg, subject)
            print(error_msg)
            print(response)
            return None
        else:
            bucket_df = self.list_to_pyspark_df(obj_list)
            if bucket_df is None:
                print("can not generate pyspark dataframe for s3 objects in s3_obj_to_pyspark_df")
                return None
            print('original bucket_df:::')
            bucket_df.show(truncate=False)
            endtime = time.time()
            print(f"s3 objects dataframe read in time: {(endtime-starttime):.06f}s")
            return bucket_df
        finally:
            print('scan s3 section done')

    def get_script_prefix(self, script_file_name):
        """
        Get validation script prefix in target bucket
        """
        try:
            if self.target_prefix[-1]=="/":
                script_prefix = self.target_prefix+script_file_name
            else:
                script_prefix = self.target_prefix+"/"+script_file_name
        except:
            print('cannot get_script_prefix')
        else:
            return script_prefix
        finally:
            print('get_script_prefix section done')

    @staticmethod
    def remove_script_from_df(pyspark_df, remove_value, column_name):
        """
        Remove script prefix/path from the dataframe
        """
        # In this product
        #   pyspark_df -> bucket_df
        #   column_name -> Path
        try:
            pyspark_df_updated = pyspark_df.filter(pyspark_df[column_name]!=remove_value)
        except:
            print('cannot remove script from df')
        else:
            print(f'after remove value {remove_value} :::')
            pyspark_df_updated.show(truncate=False)
            return pyspark_df_updated
        finally:
            print('remove_script_from_df section done')

    @staticmethod
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
        except:
            return None
        else:
            return missing_df
        finally:
            print('getting missing objects section done')

    @staticmethod
    def get_df_count(pypark_df):
        """
        Count number of rows from a pyspark dataframe
        """
        try:
            df_count = pypark_df.count()
        except:
            print('cannot count dataframe rows')
            return None
        else:
            print('Dataframe count:')
            print(df_count)
            return df_count
        finally:
            print('getting rows in a pyspark dataframe section done')

    @staticmethod
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
        except:
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

    @staticmethod
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
        except:
            print("cannot genereate pyspark dataframe for wrong size objects")
            return None
        else:
            print('wrong_size_df:::')
            wrong_size_df.show(truncate=False)
            return wrong_size_df
        finally:
            print('getting wrong size objects in a pyspark dataframe section done')

    @staticmethod
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
                except:
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
        except:
            print('cannot save result')
            return message
        finally:
            print('save_result section done')

    def result_to_subscriber(self, missing_message, wrong_size_message):
        """
        Sent email to sns subscriber
        """
        try:
            message = {"Missing items: ":missing_message,"Wrong size objects: ":wrong_size_message,"Validation started at: ":self.current}
            subject = f'{self.target_prefix} {self.target_bucket} validation done'
            response = self.sns_client.publish(
                    TargetArn=self.sns_topic_arn,
                    Message=json.dumps({'default': json.dumps(message, indent = 6)}),
                    Subject=subject,
                    MessageStructure='json')
        except:
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
    Start execution
    """
    # Start execution
    totalstart = time.time()

    #####################################################
    ## 1. Create validation object with its attributes ##
    #####################################################
    trigger_bucket_name = 's3_bucket'
    trigger_path_name = 's3_path'
    time_zone = 'US/Mountain'
    time_format = '%Y-%m-%d_%H-%M-%S_%Z_%z'
    aws_sns_client = 'sns'
    aws_s3_resource = 's3'
    validation_obj = Validation.create_object(trigger_bucket_name,
                                            trigger_path_name,
                                            time_zone,
                                            time_format,
                                            aws_sns_client,
                                            aws_s3_resource
                                            )
    validation_obj.time_zone = time_zone
    validation_obj.time_format = time_format
    validation_obj.sns_name = validation_obj.get_sns_name()
    validation_obj.sns_topic_arn = validation_obj.get_sns_arn()

    ###########################################################################################
    ## 2. Generate result saving location based on values of target bucket and target folder ##
    ###########################################################################################
    result_location = validation_obj.generate_result_location()

    #########################################
    ## 3. Read file into PySpark dataframe ##
    #########################################
    file_df_columns = {"_c0": "Site","_c1": "Accessment","_c2": "Path","_c3": "Size"}
    file_df = validation_obj.file_to_pyspark_df(**file_df_columns)
    if file_df is None:
        sys.exit("cannot generate file pyspark dataframe")

    ###########################################################################################################################
    ## 4. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe ##
    ###########################################################################################################################
    bucket_df = validation_obj.s3_obj_to_pyspark_df()
    if bucket_df is None:
        sys.exit("cannot generate s3 object pyspark dataframe")

    ########################################################
    ## 5. remove validation script from PySpark dataframe ##
    ########################################################
    remove_value_location = validation_obj.get_script_prefix("s3_to_s3_validation_script.py")
    print(bucket_df.count())
    bucket_df = validation_obj.remove_script_from_df(bucket_df, remove_value_location, "Path")
    print(bucket_df.count())

    #####################################################
    ## 6. Prepare and do comparisons on two dataframes ##
    #####################################################
    # Store bucket dataframe with different columns name to avoid conflict when comparing two dataframes, which have duplicate names
    rename_columns = {"Size": "bSize","Path": "bPath"}
    bucket_df_renamed = validation_obj.rename_columns(bucket_df, **rename_columns)
    if bucket_df_renamed is None:
        sys.exit("cannot generate renamed s3 object pyspark dataframe")

    # Get missing dataframe, values in file_df not in bucket_df
    missing_df = validation_obj.get_missing_objects(file_df, bucket_df_renamed, "Path", "bPath")
    if missing_df is None:
        print("cannot generate pyspark dataframe for missing s3 objects")
    else:
        missing_count = validation_obj.get_df_count(missing_df)
        print(f'missing s3 objects number: {missing_count}')

    # Get match dataframe
    match_df = validation_obj.get_match_objects(file_df, bucket_df_renamed,
                                    "Path", "Size", "bPath", "bSize", "Date")

    # Get wrong size dataframe
    if match_df is not None:
        wrong_size_df = validation_obj.get_wrong_size_objects(match_df, "Size", "bSize")
        if wrong_size_df is not None:
            wrong_size_count = validation_obj.get_df_count(wrong_size_df)
            print(f'wrong size s3 objects number: {wrong_size_count}')
    else:
        print("because there is no match_df, no need to get wrong_size_df")

    #####################################################################################
    ## 7. Save validation result to Target S3 with the same level as the Target folder ##
    #####################################################################################
    current = validation_obj.current
    obj_name = "missing"
    missing_message = validation_obj.save_result(missing_count, result_location, current, missing_df, obj_name)
    obj_name = "wrong_size"
    wrong_size_message = validation_obj.save_result(wrong_size_count, result_location, current, wrong_size_df, obj_name)

    #################################################
    ## 8. Send out notification to SNS subscribers ##
    #################################################
    validation_obj.result_to_subscriber(missing_message, wrong_size_message)

    # The end of the validaiton execution
    totalend = time.time()
    print(f"total execution time: {(totalend-totalstart):.06f}s")
    print("end of job")

if __name__ == "__main__":
    main()
    print("Executin completed.")
