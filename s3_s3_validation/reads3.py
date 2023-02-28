import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from py4j.protocol import Py4JJavaError
import time

def fixture_setup_spark():
    packages = (",".join(["io.delta:delta-core_2.12:2.2.0","org.apache.hadoop:hadoop-aws:3.3.4"]))
    # spark_driver_memory = '8g'
    # spark_executor_memory = '8g'
    # spark_memory_offHeap_enabled = True
    # spark_memory_offHeap_size =  '10g'
    # spark_driver_maxResultSize = '2g'
    # print('packages: '+packages)

    # Instantiate Spark via builder
    # Note: we use the `ContainerCredentialsProvider` to give us access to underlying IAM role permissions

    spark = (SparkSession
        .builder
        .appName("PySparkApp") 
        .config("spark.jars.packages", packages) 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .config("fs.s3a.aws.credentials.provider",'com.amazonaws.auth.ContainerCredentialsProvider') 
        # .config("spark.driver.memory", spark_driver_memory)
        # .config("spark.executor.memory", spark_executor_memory)
        # .config("spark.memory.offHeap.enabled", spark_memory_offHeap_enabled)
        # .config("spark.memory.offHeap.size", spark_memory_offHeap_size)
        # .config("spark.sql.broadcastTimeout", "36000")

    ).getOrCreate()
    return spark
spark = fixture_setup_spark()
schema = (StructType()
  .add("id",LongType(),True)
  .add("path",StringType(),True)
  .add("size",LongType(),True))
schema_second = (StructType()
  .add("date",StringType(),True)
  .add("path",StringType(),True)
  .add("size",LongType(),True))

file_bucket = "s3-validation-demo"
file_prefix = "test/s3_to_s3_validation.csv"

try:
	file_df = (spark.read
		.format("csv")
		.option("header", "true")
		.schema(schema)
		.load(f"s3a://{file_bucket}/{file_prefix}"))
except (AnalysisException, Py4JJavaError, IllegalArgumentException) as err:
	print(err)
	print('"file_to_pyspark_df" function completed unsuccessfully.')
else:
	print('Original file_df:')
	file_df.show(truncate=False)
	print('"file_to_pyspark_df" function completed successfully.')
try:
	second_df = (spark.read
		.format("csv")
		.option("header", "true")
		.schema(schema_second)
		.load(f"s3a://{file_bucket}/test/s3_to_s3_validation_second.csv"))
except (AnalysisException, Py4JJavaError, IllegalArgumentException) as err:
	print(err)
	print('"file_to_pyspark_df" function completed unsuccessfully.')
else:
	print('Original second_df:')
	second_df.show(truncate=False)
	print('"file_to_pyspark_second_df" function completed successfully.')

second_df = second_df.withColumnRenamed('path', 'b_path')
second_df = second_df.withColumnRenamed('size', 'b_size')
join_expr = file_df['path'] == second_df['b_path']
join_type = "anti"
missing_df = file_df.join(second_df, join_expr, join_type)
missing_df.show(truncate=False)

join_expr = file_df["path"] == second_df["b_path"]
join_type = "inner"
match_df = file_df.join(second_df, join_expr, join_type).select([file_df['path'],
														file_df['size'],
														second_df['b_path'],
														second_df['b_size'],
														second_df['date']])
print('match_df:::')
match_df.show(truncate=False)
print('match_df schema:::')
match_schema = match_df.schema
print(type(match_schema))
print(set(match_schema))


wrong_size_df = match_df.filter(match_df["size"]!=match_df["b_size"])
wrong_size_df.show(truncate=False)



# a=[5,5,6,8,1]
# a.sort()
# print(a)



# s3_bucket = "s3-validation-demo"
# bucket = "s3-validation-demo"
# s3_resource = boto3.resource('s3')
# s3_bucket_info_dict = s3_resource.meta.client.head_bucket(Bucket=s3_bucket)
# print(s3_bucket_info_dict)
# print(type(s3_bucket_info_dict))


# try:
# 	s3_bucket_objects_collection = s3_resource.Bucket(s3_bucket) \
# 		.objects.filter(Prefix="test/s3_to_s3_validation.csv")
# except ClientError as err:
# 	print(err)
# 	print('"prefix_to_list" function completed unsuccessfully.')
# else:
# 	s3_prefix_list = []
# 	for item in s3_bucket_objects_collection:
# 		s3_prefix_list.append(item.key)
# 	print('"prefix_to_list" function completed successfully.')
# 	print(s3_prefix_list)


















# data_key = "pytest_result/s3_to_s3_validation_pytest_result/"
# result_location = f"{bucket}/{data_key}"

# obj_name = obj_name = "test_object"

# fake_row_count = 'this is a fake row_count'
# fake_result_location = 'this is a fake location'
# fake_current = 'fake_current'
# fake_pyspark_df = 'this_is_a_fake_df'
# fake_obj_name = ['this is a fake obj_name']

# s3_resource = boto3.resource('s3')
# s3_bucket_objects_collection = s3_resource.Bucket(bucket) \
# .objects.filter(Prefix=f'{data_key}')

# file_name = f'{data_key}{obj_name}_20230220_145443_MST_-0700.csv'

# thelist = []
# for item in s3_bucket_objects_collection:
# 	thelist.append(item.key)
# 	print(item.key)
# if file_name in thelist:
# 	print('in the list!!!')
				
