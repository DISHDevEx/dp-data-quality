"""
This module checks the nested JSON schema with Inventory Standard JSON schema
"""
import logging
import logging.config
import sys
import datetime
import boto3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window as W

class PayloadValidation:
    """
    Class to create pyspark dataframe for nested input JSON and standard JSON,
    convert JSON to dataframe and create a report for missing attributes in input JSON
    """
    def __init__(self):
        """
        Create a logger config session
        PARAMETERS:
            self
        RETURNS:
            None
        """
        logging.config.dictConfig(
        {
            'disable_existing_loggers':True,
            'version':1
        })
        logging.basicConfig(filename='logfile.log',
                        encoding='utf-8',
                        format='%(asctime)s %(message)s',
                        datefmt='%m-%d-%Y %H:%M:%S %p %Z',
                        level=logging.INFO)
        self.logger = logging.getLogger()
        self.logger.info('Logger Initiated for Nested JSON Validation.')

    def create_spark_session(self):
        """
        Create a Spark session
        PARAMETERS:
            self
        RETURNS:
            spark: spark session
        """
        spark =  SparkSession.builder \
                    .master("local[*]") \
                    .appName("myapp") \
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")\
                    .getOrCreate()
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

        return spark

    def read_s3_json_to_df(self,file_name, bucket_folder_path):
        """
        Method to read valid and invalid JSON Input from s3 and convert JSON file to dataframe.
        PARAMETERS:
            self,fileName
        RETURNS:
            df: JSON file converted to a dataframe
        """

        data_key = file_name
        data_location = f's3a://{bucket_folder_path}/{data_key}'
        self.logger.info('S3 location found for %s file',file_name)

        spark = self.create_spark_session()

        # CONVERT JSON TO DATAFRAME
        json_to_df = spark.read.option("multiline","true").json(data_location)
        json_to_df.printSchema()

        json_to_df.show(truncate=True)
        json_to_df.select("payloadContext.applicationId").show(1,False)
        self.logger.info('%s converted to dataframe',file_name)

        return json_to_df

    def flatten_df(self, schema, prefix=None):
        """
        Method to flatten a dataframe.
        PARAMETERS:
            self,schema
        RETURNS:
            fields: return all the columns in a nested JSON
        """

        fields = []
        for field in schema.fields:
            name = prefix + '.' + field.name if prefix else field.name
            dtype = field.dataType
            if isinstance(dtype, StructType):
                fields += self.flatten_df(dtype, prefix=name)
            else:
                fields.append(name)

        return fields

    def schema_check_report_to_s3(self,
                                df_standard_format,
                                df_input_format,
                                bucket_name,
                                invalid_filename):
        """
        Method to compare both standard and input files and store the missing values to s3 report
        PARAMETERS:
            self,df_standard_format, df_input_format, bucket_name, input_file
        RETURNS:
            None
        """

        # CONVERT ARRAY TYPE TO STRUCT
        for i in df_standard_format.columns:
            df_standard_format = df_standard_format.withColumn(
                i, F.explode(i))

        for i in df_input_format.columns:
            df_input_format = df_input_format.withColumn(i, F.explode(i))


        # ALL COLUMNS IN A DATAFRAME ARE FLATTENED
        df_standard_flattened = df_standard_format.select(
            self.flatten_df(
            df_standard_format.schema))
        df_input_flattened = df_input_format.select(self.flatten_df(df_input_format.schema))

        # CHECK FOR MISSING VALUES IN INPUT JSON FILE COMPARE TO STANDARD JSON FILE
        missing_standard_attributes = list(set(df_standard_flattened.columns)
                                        - set(df_input_flattened.columns))
        self.logger.info('Missing attributes from input json file are listed.')

        # MISSING ATTRIBUTES FROM INPUT JSON FILE REPORTED
        if len(missing_standard_attributes)>0:

            aws_account_id = boto3.client("sts").get_caller_identity()["Account"]
            output_df = (df_standard_format.limit(1)
                        .withColumn('ATTRIBUTE_NAME',
                                    F.array([F.lit(x) for x in missing_standard_attributes]))
                        .withColumn('ATTRIBUTE_NAME', F.explode('ATTRIBUTE_NAME'))
                        .withColumn('ID', F.row_number().over(W.orderBy("ATTRIBUTE_NAME")))
                        .select('ID', 'ATTRIBUTE_NAME'))

            output_df = (output_df.withColumn('ACCOUNT_ID', F.lit(aws_account_id))
                        .withColumn('BUCKET_NAME', F.lit(bucket_name))
                        .withColumn('FILE_NAME', F.lit(invalid_filename))
                        .withColumn('VALIDATION_TYPE', F.lit("json_schema_validation"))
                        .withColumn('VALIDATION_MESSAGE',
                                F.lit("Attribute missing in invalid json"))
                        .withColumn('TIMESTAMP', F.current_timestamp())
                        .select('ID', 'ACCOUNT_ID',
                                'BUCKET_NAME', 'ATTRIBUTE_NAME',
                                'VALIDATION_MESSAGE', 'VALIDATION_TYPE',
                                'FILE_NAME', 'TIMESTAMP'))
            current_datetime = datetime.datetime.now()
            timestamp = current_datetime.strftime('%Y%m%d_%H%M%S')
            file_name = f'json_schema_validation_{timestamp}'

            # REPORT STORED TO S3
            output_pandas_df = output_df.toPandas()
            output_pandas_df.to_csv(f's3a://metadata-graphdb/JsonSchemaReport/{file_name}.csv',
                                    index=False)
            self.logger.info('Missing attributes from input json file are stored to s3 report')
        else:
            # NO MISSING ATTRIBUTES FOUND
            self.logger.info('There are no missing attributes from input Json file.')

def main():
    """
    This function reads user parameters and checks the input and standard JSON schemas
    """
    payload_validation = PayloadValidation()

    # READ PARAMETERS FROM USER
    bucket_folder_path_standard = sys.argv[1]
    bucket_folder_path_input = sys.argv[2]
    standard_file = sys.argv[3]
    input_file = sys.argv[4]
    bucket_name = sys.argv[5]

    df_standard_format = payload_validation.read_s3_json_to_df(
        standard_file, bucket_folder_path_standard)
    df_input_format = payload_validation.read_s3_json_to_df(input_file,
                                                    bucket_folder_path_input)
    payload_validation.schema_check_report_to_s3(df_standard_format, df_input_format,
                                                bucket_name, input_file)

if __name__ == '__main__':
    main()
