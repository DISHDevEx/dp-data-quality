"""
Module to start a spark session in AWS environment.
"""
from pyspark.sql import SparkSession
from pyspark import SparkConf

def spark_setup():
    """
    Method to instantiate PySpark.

    Returns:
        spark - SparkSession with proper parameters.
    """
    packages = (','.join(['io.delta:delta-core_2.12:2.2.0','org.apache.hadoop:hadoop-aws:3.3.4']))

    conf = SparkConf()
    conf.set('spark.jars.packages', packages)
    conf.set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    conf.set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    conf.set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.ContainerCredentialsProvider')
    conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
    conf.set("spark.sql.legacy.json.allowEmptyString.enabled", True)
    conf.set("spark.hadoop.fs.s3a.path.style.access", True)
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", False)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    return spark
