"""
Module with classes to run generic and datatype-specific validations on data
and create a validation report.
"""
from datetime import datetime
from math import isnan
from pytz import timezone
import boto3
import pandas as pd
import numpy as np
from pyspark.sql import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, col, length, trim
from pyspark.sql.types import StringType, IntegerType, LongType, ShortType, FloatType, DoubleType
from read_data import ReadDataPyspark, ReadDataPandas

class GenericValidation:
    """
    Class to run generic validations on data and create a validation report.
    """

    def __init__(self, data_filepath, metadata_filepath, report_filepath, bucket_name):
        """
        Method to initiate class with spark session,  filepath, dataframe and main function.
        """

        self.data_filepath = data_filepath
        self.metadata_filepath = metadata_filepath
        self.report_filepath = report_filepath
        self.bucket_name = bucket_name
        self.data_df = ReadDataPyspark(data_filepath).dataframe
        self.metadata_df = ReadDataPandas(metadata_filepath).dataframe
        self.table_name = self.data_filepath.split('/')[-1]
        self.aws_account_name = self.get_aws_account_name()

    def get_aws_account_name(self):
        """
        Method to get AWS account name of S3 bucket data being validated.
        """
        folder_path = self.data_filepath.replace(self.table_name, '')\
                                .split(self.bucket_name + '/')[-1]
        resource = boto3.resource('s3')
        bucket = resource.Bucket(self.bucket_name)

        for obj in bucket.objects.filter(Prefix=folder_path):
            if self.table_name in obj.key:
                return obj.owner['DisplayName']

    def validate_data_columns(self):
        """
        Method to identify columns that are in data but not in metadata.

        Returns:
        columns - list of columns that are in data but not in metadata
        """
        validation = 'COLUMN NOT IN METADATA'
        metadata_columns = [i.upper() for i in self.metadata_df['Attribute_Name']]
        data_columns = [i.upper() for i in self.data_df.columns]
        columns = [i for i in data_columns if i not in metadata_columns]

        return columns, validation

    def validate_metadata_columns(self):
        """
        Method to identify columns that are in metadata but not in data.

        Returns:
        columns - list of columns that are in data but not in metadata
        """
        validation = 'COLUMN NOT IN DATA'
        metadata_columns = [i.upper() for i in self.metadata_df['Attribute_Name']]
        data_columns = [i.upper() for i in self.data_df.columns]
        columns = [i for i in metadata_columns if i not in data_columns]

        return columns, validation

    def validate_columns(self, data_df):
        """
        Method to identify columns that are both in data and metadata.

        Parameters:
        data_df - data dataframe

        Returns:
        columns_in_both - list of columns that are both in data and metadata
        """

        metadata_columns = [i.upper() for i in self.metadata_df['Attribute_Name']]
        data_columns = [i.upper() for i in data_df.columns]
        # not_in_data = [i for i in metadata_columns if i not in data_columns]
        # not_in_metadata = [i for i in data_columns if i not in metadata_columns]
        columns_in_both = [i for i in metadata_columns if i in data_columns]

        return columns_in_both

    def assign_row_id(self, data_df):
        """
        Method to add a unique identifier to each row in the data.

        Parameters:
        data_df - data dataframe

        Returns:
        data_df - data dataframe
        """

        data_df = data_df.withColumn("ROW_ID", row_number().over(Window\
                            .orderBy(monotonically_increasing_id()))-1)

        return data_df

    def column_null_check(self, data_df, column):
        """
        Method to check for nulls in dataframe.

        Parameters:
        data_df - data dataframe
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'NULL'
        data_df = data_df.select(column, 'ROW_ID').filter(col(column).isNull())
        fail_row_id = [data[0] for data in data_df.select('ROW_ID').collect()]

        return validation, column, fail_row_id

    def table_validation_results(self, function):
        """
        Method to create dataframe with results from table level validation check functions.

        Parameters:
        function - table level validation function

        Returns:
        result_df - dataframe with validation results
        """

        columns, validation = function()

        if len(columns) > 0:
            result_df = pd.DataFrame(columns=['AWS_ACCOUNT_NAME', 'S3_BUCKET', 'TABLE_NAME',
            'COLUMN_NAME', 'VALIDATION_CATEGORY', 'VALIDATION_ID', 'VALIDATION_MESSAGE',
            'PRIMARY_KEY_COLUMN', 'PRIMARY_KEY_VALUE', 'TIMESTAMP'])
            result_df['COLUMN_NAME'] = columns
            result_df['VALIDATION_ID'] = validation
            result_df['TABLE_NAME'] = self.table_name
            result_df['PRIMARY_KEY_COLUMN'] = 'N/A'
            result_df['PRIMARY_KEY_VALUE'] = 'N/A'
            result_df['TIMESTAMP'] = datetime.now(timezone('US/Mountain'))\
                            .strftime("%d/%m/%Y %H:%M:%S")
            result_df['AWS_ACCOUNT_NAME'] = self.aws_account_name
            result_df['S3_BUCKET'] = self.bucket_name
            result_df['VALIDATION_CATEGORY'] = ''
            result_df['VALIDATION_MESSAGE'] = ''

            return result_df


    def column_validation_results(self, data_df, function):
        """
        Method to create results dataframe for column level validation check functions.

        Parameters:
        data_df - slice of original dataframe with columns of particular datatype
        function - validation function

        Returns:
        result_df - dataframe with validation results
        """

        validation_check_list = []

        for column in [column for column in data_df.columns if column != 'ROW_ID']:
            validation_check_dict = {}
            validation, column, fail_row_id = function(data_df, column)

            if len(fail_row_id) > 0:
                validation_check_dict['COLUMN_NAME'] = column
                validation_check_dict['ROW_ID_LIST'] = fail_row_id
                validation_check_list.append(validation_check_dict)

        if len(validation_check_list) > 0:
            result_df = pd.DataFrame(validation_check_list)

            # Explode list of row IDs from ROW_ID_LIST column vertically
            result_df = result_df.explode('ROW_ID_LIST').rename(columns=\
                                     {'ROW_ID_LIST': 'PRIMARY_KEY_VALUE'})
            result_df['TABLE_NAME'] = self.table_name
            result_df['VALIDATION_ID'] = validation
            result_df['TIMESTAMP'] = datetime.now(timezone('US/Mountain'))\
                                        .strftime("%d/%m/%Y %H:%M:%S")
            result_df['AWS_ACCOUNT_NAME'] = self.aws_account_name
            result_df['S3_BUCKET'] = self.bucket_name
            result_df['VALIDATION_CATEGORY'] = ''
            result_df['VALIDATION_MESSAGE'] = ''

            # When unique identifier does not exist in data, ROW_ID acts as PRIMARY_KEY_COLUMN
            result_df['PRIMARY_KEY_COLUMN'] = 'ROW_ID'

            return result_df[[ 'AWS_ACCOUNT_NAME', 'S3_BUCKET', 'TABLE_NAME', 'COLUMN_NAME',
                               'VALIDATION_CATEGORY','VALIDATION_ID', 'VALIDATION_MESSAGE',
                              'PRIMARY_KEY_COLUMN', 'PRIMARY_KEY_VALUE', 'TIMESTAMP']]

    def add_to_report_dataframe(self, result_df, report_df):
        """
        Method to append validation results dataframe to report dataframe.

        Parameters:
        result_df - dataframe of validation results
        report_df - dataframe with validation results from different validation functions

        Returns:
        report_df - dataframe with validation results from different validation functions
        """

        if not isinstance(result_df, type(None)):
            report_df = pd.concat([report_df, result_df])

        return report_df

    def save_report_to_s3(self, report_df):
        """
        Method to add Report ID to report dataframe and save dataframe to CSV.

        Parameters:
        report_df - dataframe with validation results from different validation functions
        """

        report_df['DQ_REPORT_ID'] = np.arange(1,len(report_df)+1)
        report_df.set_index('DQ_REPORT_ID', inplace=True)
        report_df.to_csv(self.report_filepath)

class DatatypeValidation(GenericValidation):
    """
    Class to run datatype specific validations on data.
    """

    def __init__(self, data_filepath, metadata_filepath, report_filepath, bucket_name):
        """
        Method to initiate class with data filepath, metadata filepath,
        report filepath and bucket name.
        """
        super().__init__(data_filepath, metadata_filepath, report_filepath, bucket_name)
        self.main()

    def separate_columns_by_datatype(self, columns_in_both):
        """
        Method to group columns by their datatypes.

        Parameters:
        columns_in_both - list of columns that are both in data and metadata

        Returns:
        datatype_column_dict - dictionary with datatypes as keys and list of column names as values
        """

        datatypes = list(self.metadata_df['Data_Type'].unique())
        datatype_column_dict = {}

        for datatype in datatypes:
            datatype_column_dict[datatype] = [value.upper() for value in self.metadata_df[
                                        self.metadata_df['Data_Type'].str.contains(datatype)]\
                                ['Attribute_Name'].values if value.upper() in columns_in_both]

        return datatype_column_dict

    def separate_df_by_datatype(self, data_df, datatype_column_dict, datatype):
        """
        Method to create a subset of dataframe with columns that have given datatype.

        Parameters:
        data_df - data dataframe
        datatype_column_dict - dictionary with datatypes as keys and list of column names as values.
        datatype - unique datatype from metadata

        Returns:
        datatype_df - subset of dataframe with columns of given datatype
        """

        datatype_df = data_df[datatype_column_dict[datatype] + ['ROW_ID']]

        return datatype_df

    def column_numeric_check(self, datatype_df, column):
        """
        Method to validate a column for numeric datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of numeric datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'NUMERIC'
        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        # Regex to capture non-numeric values
        regex1 = r'^[\deE.+-]+$'
        regex2 = r'^[-+]?+\d+[.]?+\d*$'
        regex3 = r'[+-]?\d(\.\d+)?[Ee][+-]?\d+'

        datatype_df = datatype_df.filter(datatype_df[column]\
                                 .rlike(regex1)).filter((datatype_df[column]\
                        .rlike(regex2)) | (datatype_df[column].rlike(regex3)))

        pass_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def column_integer_check(self, datatype_df, column):
        """
        Method to validate a column for integer datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of numeric datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'INTEGER'

        # Integer limits
        lower = -2147483648
        upper = 2147483647

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        # Regex to capture non-integer values
        regex1 = r'^[\deE.+-]+$'
        regex2 = r'^[-+]?+\d+[.]?[0]?+$'
        regex3 = r'[+-]?\d(\.\d+)?[Ee][+-]?\d+'
        datatype_df = datatype_df.filter(datatype_df[column].rlike(regex1))\
                                 .filter((datatype_df[column]\
                      .rlike(regex2)) | (datatype_df[column].rlike(regex3)))


        datatype_df = datatype_df.withColumn(column, col(column).cast(IntegerType()))
        datatype_df = datatype_df.where((upper >= datatype_df[column])\
                                        & (lower <= datatype_df[column]))
        pass_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def column_long_check(self, datatype_df, column):
        """
        Method to validate a column for long datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of long datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'LONG'

        # Long limits
        lower = -9223372036854775808
        upper = 9223372036854775807

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        # Regex to capture non-long values
        regex1 = r'^[\deE.+-]+$'
        regex2 = r'^[-+]?+\d+[.]?[0]?+$'
        regex3 = r'[+-]?\d(\.\d+)?[Ee][+-]?\d+'
        datatype_df = datatype_df.filter(datatype_df[column].rlike(regex1))\
                                .filter((datatype_df[column].rlike(regex2))\
                                | (datatype_df[column].rlike(regex3)))

        datatype_df = datatype_df.withColumn(column, col(column).cast(LongType()))
        datatype_df = datatype_df.where((upper >= datatype_df[column])\
                                        & (lower <= datatype_df[column]))
        pass_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def column_short_check(self, datatype_df, column):
        """
        Method to validate a column for short datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of short datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'SHORT'

        # Short limits
        lower_short = -32768
        upper_short = 32767

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        # Regex to capture non-integer values
        regex1 = r'^[\deE.+-]+$'
        regex2 = r'^[-+]?+\d+[.]?[0]?+$'
        regex3 = r'[+-]?\d(\.\d+)?[Ee][+-]?\d+'
        datatype_df = datatype_df.filter(datatype_df[column].rlike(regex1))\
                                 .filter((datatype_df[column].rlike(regex2))\
                                 | (datatype_df[column].rlike(regex3)))

        datatype_df = datatype_df.withColumn(column, col(column).cast(ShortType()))
        datatype_df = datatype_df.where( (upper_short >= datatype_df[column])\
                                        & (lower_short <= datatype_df[column]))
        pass_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def column_double_check(self, datatype_df, column):
        """
        Method to validate a column for double datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of double datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'DOUBLE'

        # Double limits
        lower_positive = 2.225e-307
        upper_positive = 1.79769e+308
        lower_negative = -1.79769E+308
        upper_negative = -2.225E-307

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        # Regex to capture non-integer values
        regex1 = r'^[\deE.+-]+$'
        regex2 = r'^[-+]?+\d+[.]?+\d*$'
        regex3 = r'[+-]?\d(\.\d+)?[Ee][+-]?\d+'
        datatype_df = datatype_df.filter(datatype_df[column].rlike(regex1))\
                                 .filter((datatype_df[column].rlike(regex2))\
                                 | (datatype_df[column].rlike(regex3)))

        datatype_df = datatype_df.withColumn(column, col(column).cast(DoubleType()))
        datatype_df = datatype_df.where(((upper_positive >= datatype_df[column])\
                                      & (lower_positive <= datatype_df[column]))\
                                 | ((upper_negative <= datatype_df[column])\
                                      & (lower_negative >= datatype_df[column]))\
                                 | (datatype_df[column] == 0))
        pass_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def column_float_check(self, datatype_df, column):
        """
        Method to validate a column for float datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of float datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'FLOAT'

        # Float limits
        lower_positive = 1.175494351e-38
        upper_positive = 3.402823466e38
        lower_negative = -1.175494351e-38
        upper_negative = -3.402823466e38

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        # Regex to capture non-integer values
        regex1 = r'^[\deE.+-]+$'
        regex2 = r'^[-+]?+\d+[.]?+\d*$'
        regex3 = r'[+-]?\d(\.\d+)?[Ee][+-]?\d+'
        datatype_df = datatype_df.filter(datatype_df[column].rlike(regex1))\
           .filter((datatype_df[column].rlike(regex2)) | (datatype_df[column].rlike(regex3)))

        datatype_df = datatype_df.withColumn(column, col(column).cast(FloatType()))
        datatype_df = datatype_df.where(((upper_positive >= datatype_df[column])\
                                         & (lower_positive <= datatype_df[column]))\
                                     | ((upper_negative <= datatype_df[column])\
                                         & (lower_negative >= datatype_df[column]))\
                                     | (datatype_df[column] == 0))
        pass_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def column_string_check(self, datatype_df, column):
        """
        Method to validate a column for string datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of string datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'STRING'

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        str_length = self.metadata_df[self.metadata_df['Attribute_Name']\
                     .str.lower() == column.lower()]['Data_Type_Limit'].drop_duplicates().values

        if not isnan(str_length) and datatype_df.count() != 0:
            datatype_df = datatype_df.withColumn(column, col(column).cast(StringType()))
            datatype_df = datatype_df.filter((length(trim(col(column))) > int(str_length)))
            fail_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        else:
            fail_row_id = []

        return validation, column, fail_row_id

    def column_varchar_check(self, datatype_df, column):
        """
        Method to validate a column for varchar datatype.

        Parameters:
        datatype_df - subset of dataframe with columns of varchar datatype
        column - name of column to be validated

        Returns:
        validation - type of validation
        column - name of validated column
        fail_row_id - list of row IDs that failed validation
        """

        validation = 'VARCHAR'

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        str_length = self.metadata_df[self.metadata_df['Attribute_Name']\
                     .str.lower() == column.lower()]['Data_Type_Limit'].drop_duplicates().values

        if not isnan(str_length) and datatype_df.count() != 0:
            datatype_df = datatype_df.withColumn(column, col(column).cast(StringType()))
            datatype_df = datatype_df.filter((length(trim(col(column))) > int(str_length)))
            fail_row_id = [data[0] for data in datatype_df.select('ROW_ID').collect()]

        else:
            fail_row_id = []

        return validation, column, fail_row_id

    def validation_function_by_datatype(self, datatype):
        """
        Method to identify validation fuction for a column based on its datatype.

        Parameters:
        datatype - column datatype based on metadata

        Returns:
        function - validation function
        """
        if datatype == 'integer':
            function = self.column_integer_check
        elif datatype == 'float':
            function = self.column_float_check
        elif datatype == 'double':
            function = self.column_double_check
        elif datatype == 'long':
            function = self.column_long_check
        elif datatype == 'short':
            function = self.column_short_check
        elif datatype == 'numeric':
            function = self.column_numeric_check
        elif datatype == 'string':
            function = self.column_string_check
        elif datatype == 'varchar':
            function = self.column_varchar_check
        else:
            function = None

        return function

    def main(self):
        """
        Method to create, populate and save validation report.
        """

        # Create a report dataframe template will be saved in S3 in the form of Report.csv
        report_df = pd.DataFrame(columns=['AWS_ACCOUNT_NAME', 'S3_BUCKET', 'TABLE_NAME',
            'COLUMN_NAME', 'VALIDATION_CATEGORY', 'VALIDATION_ID', 'VALIDATION_MESSAGE',
            'PRIMARY_KEY_COLUMN', 'PRIMARY_KEY_VALUE', 'TIMESTAMP'])

        # Result dataframe from column name validation checks
        result_df = self.table_validation_results(self.validate_data_columns)
        report_df = self.add_to_report_dataframe(result_df, report_df)
        result_df = self.table_validation_results(self.validate_metadata_columns)
        report_df = self.add_to_report_dataframe(result_df, report_df)

        #Assign unique ID to each row of data
        columns_in_both = self.validate_columns(self.data_df)
        datatype_column_dict = self.separate_columns_by_datatype(columns_in_both)
        self.data_df = self.assign_row_id(self.data_df)

        # Result dataframe form null validation check
        result_df = self.column_validation_results(self.data_df, self.column_null_check)
        report_df = self.add_to_report_dataframe(result_df, report_df)

        # Result dataframe from datatype specific validation check
        for key in datatype_column_dict:
            datatype_df = self.separate_df_by_datatype(self.data_df, datatype_column_dict, key)
            function = self.validation_function_by_datatype(key)

            if not isinstance(function, type(None)):
                result_df = self.column_validation_results(datatype_df, function)
                report_df = self.add_to_report_dataframe(result_df, report_df)

        self.save_report_to_s3(report_df= report_df)
