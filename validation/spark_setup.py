from pyspark.sql import SparkSession
from pyspark import SparkConf

def spark_setup():
    '''
    Method to instantiate PySpark.

    INPUT: None.

    OUTPUT: SparkSession with propoer parameters.
    '''
    packages = (','.join(['io.delta:delta-core_2.12:2.1.0','org.apache.hadoop:hadoop-aws:3.3.4']))

    conf = SparkConf()
    conf.set('spark.jars.packages', packages)
    conf.set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    conf.set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    conf.set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.ContainerCredentialsProvider')

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    return spark
