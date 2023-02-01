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
totalstart = time.time()

############################################################################
## 1. Get location of the file, which is passed here from LambdaAsTrigger ##
############################################################################
args = getResolvedOptions(sys.argv, ["s3_bucket", "s3_path"])
bucket_lambda_trigger_by_file = args["s3_bucket"]
path_lambda_trigger_by_file = args["s3_path"]
print(f'bucket from lambda: {bucket_lambda_trigger_by_file}')
print(f'path from lambda: {path_lambda_trigger_by_file}')
# Stop if the file is not for validation
file_name = path_lambda_trigger_by_file.split("/")[-1]
print('file_name')
print(file_name)
if file_name != 's3_to_s3_validation.csv':
    sys.exit("not for s3 to s3 validation")

######################################
## 2. Read local time as time stamp ##
######################################
DEN = pytz.timezone('US/Mountain')
datetime_den = datetime.now(DEN)
current = datetime_den.strftime('%Y-%m-%d_%H-%M-%S_%Z_%z')
print(f'current time: {current}')

###################################################################################
## 3. Get location of the target bucket and target folder from this GlueJob Name ##
###################################################################################
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']
target_bucket_and_prefix = job_name
target_bucket = target_bucket_and_prefix.split("/")[0]
target_prefix = target_bucket_and_prefix.replace(target_bucket, "")[1:]

###########################################################################################
## 4. Generate result saving location based on values of target bucket and target folder ##
###########################################################################################
target_prefix_no_slash = target_prefix.replace("/", "_")
result_location = f"s3a://{target_bucket}/s3_to_s3_validation_result_{target_bucket}_{target_prefix_no_slash}/"

###########################################################
## 5. Initial PySpark, SNS client, S3 client and SNS arn ##
###########################################################
# Initial PySpark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Initial SNS client
sns_client = boto3.client('sns')
# Initial S3 resource and S3 client
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
# Get SNS name for SNS arn
sns_name = target_bucket.replace(".", "")
# Prepare SNS arn
sns_topic_arn = [tp['TopicArn'] for tp in sns_client.list_topics()['Topics'] if sns_name in tp['TopicArn']][0]
print(f'sns_topic_arn: {sns_topic_arn}')

#########################################
## 6. Read file into PySpark dataframe ##
#########################################
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
except Exception as fileerror:
    error_msg = {f"s3a://{bucket_lambda_trigger_by_file}/{path_lambda_trigger_by_file} :": \
        " is not a valid manifest file path for validation","Validation started at: ":current}
    print(error_msg)
    subject = f'{target_prefix} {target_bucket} file fail'
    response = sns_client.publish(
            TargetArn=sns_topic_arn,
            Message=json.dumps({'default': json.dumps(error_msg, indent = 6)}),
            Subject=subject,
            MessageStructure='json')
    sys.exit("bad input file path")
endtime = time.time()
print(f"csv dataframe read in time: {(endtime-starttime):.06f}s")

###########################################################################################################################
## 7. Scan the objects' name and size under the target folder in the target bucket to generate another PySpark dataframe ##
###########################################################################################################################
starttime = time.time()
bucket = s3_resource.Bucket(target_bucket)
obj_counter = 0
obj_list = []
for obj in bucket.objects.filter(Prefix=target_prefix):
    key_size_date_dict = {'Path':obj.key.strip(), 'Size':obj.size, 'Date':obj.last_modified.strftime('%Y-%m-%d_%H-%M-%S_%Z_%z')}
    obj_list.append(key_size_date_dict)
    obj_counter += 1
    print(f'fetching {obj_counter} object in target folder')
try:
    bucket_df = spark.createDataFrame(obj_list)
    bucket_df.show()
except Exception as scanerror:
    error_msg = {f"s3a://{target_bucket}/{target_prefix} :":" \
        is not a valid S3 scanning path for validation","Validation started at: ":current}
    print(error_msg)
    subject = f'{target_prefix} {target_bucket} scan fail'
    response = sns_client.publish(
            TargetArn=sns_topic_arn,
            Message=json.dumps({'default': json.dumps(error_msg, indent = 6)}),
            Subject=subject,
            MessageStructure='json')
    sys.exit("bad target s3 path")
endtime = time.time()
print(f"s3 objects dataframe read in time: {(endtime-starttime):.06f}s")

#####################################################
## 8. Prepare and do comparisons on two dataframes ##
#####################################################
# Extract dish site id from path and save as another column from bucket dataframe

bucket_df = bucket_df.withColumn("Site", split(col("Path"), "/").getItem(3))
bucket_df.show()

# Get unique dish site id to use as unique index to compare in the following functions from file dataframe
sites = file_df.select("Site").distinct()
sites.show()
bucket_number = sites.count()

# Store bucket dataframe with different columns name to avoid conflict when comparing two dataframes, which have duplicate names
bucket_df_renamed = bucket_df.withColumnRenamed("Site", "bSite").withColumnRenamed("size", "bSize").withColumnRenamed("Path", "bPath")
bucket_df_renamed.show()

# Get missing dataframe, values in file_df not in bucket_df
join_expr = file_df.Path == bucket_df_renamed.bPath
joinType = "anti"
missing_df = file_df.join(bucket_df_renamed, join_expr, joinType)
missing_df.show()

print('missing number:')
missing_count = missing_df.count()
print(missing_count)

# Get match dataframe
join_expr = file_df.Path == bucket_df_renamed.bPath
joinType = "inner"
match_df = file_df.join(bucket_df_renamed, join_expr, joinType).select(file_df.Path, file_df.Site, \
    file_df.Size, bucket_df_renamed.bSize, bucket_df_renamed.Date,)
match_df.show()

print('match number:')
print(match_df.count())
match_df.printSchema()

# Get wrong size dataframe
wrong_size_df = match_df.filter(match_df["Size"]!=match_df["bSize"])
wrong_size_df.show()

print('wrong size number:')
wrong_size_count = wrong_size_df.count()
print(wrong_size_count)


#####################################################################################
## 9. Save validation result to Target S3 with the same level as the Target folder ##
#####################################################################################
if missing_count > 0:
    missing_savepath = f"{result_location}missing_{current}.csv"
    missing_message = f"saved at {result_location[6:]}_missing_{current}.csv"
    missing_df.toPandas().to_csv(missing_savepath, index = False)
else:
    print("no missing object")
    missing_message = "no missing item found."
if wrong_size_count > 0:
    wrongsize_savepath = f"{result_location}wrongsize_{current}.csv"
    wrong_size_message = f"saved at {result_location[6:]}wrongsize_{current}.csv"
    wrong_size_df.toPandas().to_csv(wrongsize_savepath, index = False)
else:
    print("no wrong size object")
    wrong_size_message = "no wrong size object found."

##################################################
## 10. Send out notification to SNS subscribers ##
##################################################
message = {"Missing items: ":missing_message,"Wrong size objects: ":wrong_size_message,"Validation started at: ":current}
subject = f'{target_prefix} {target_bucket} validation done'
response = sns_client.publish(
        TargetArn=sns_topic_arn,
        Message=json.dumps({'default': json.dumps(message, indent = 6)}),
        Subject=subject,
        MessageStructure='json')
print(response)
totalend = time.time()
print(f"total execution time: {(totalend-totalstart):.06f}s")
print("end of job")
