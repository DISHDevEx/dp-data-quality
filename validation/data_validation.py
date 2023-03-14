"""
Module with classes to run generic and datatype-specific validations on data
based on metadata.
"""
import logging
from math import isnan
import numpy as np
from pyspark.sql import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, col, \
                                    length, trim, collect_list
from pyspark.sql.types import StringType, IntegerType, LongType, ShortType, FloatType, DoubleType
from .read_data import ReadDataPyspark, ReadDataPandas

class GenericRulebook:
    """
    Class to run generic validations on data based on metadata.
    """

    def __init__(self, data_filepath, metadata_filepath):
        """
        Method to initiate class with data filepath and metadata filepath.
        """

        self.data_filepath = data_filepath
        self.metadata_filepath = metadata_filepath
        self.data_df = ReadDataPyspark(data_filepath).dataframe
        if not isinstance(self.data_df, type(None)):
            logging.info('Collected data file: %s', self.data_filepath)
        self.metadata_df = ReadDataPandas(metadata_filepath).dataframe
        if not isinstance(self.metadata_df, type(None)):
            logging.info('Collected metadata file: %s', self.metadata_filepath)

    def column_name_preprocess(self):
        """
        Method to preprocess column names in data to compare them to preprocessed metadata.

        Returns:
            data_df - dataframe of data
        """
        self.data_df = self.data_df.select([col(column).alias(column.replace('-', '_')) \
                                            for column in self.data_df.columns])
        self.data_df = self.data_df.select([col(column).alias(column.replace('@', '')) \
                                            for column in self.data_df.columns])

    def validate_data_columns(self):
        """
        Method to identify columns that are in data but not in metadata.

        Returns:
            columns - list of columns that are in data but not in metadata
        """
        validation = 1
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
        validation = 2
        metadata_columns = [i.upper() for i in self.metadata_df['Attribute_Name']]
        data_columns = [i.upper() for i in self.data_df.columns]
        columns = [i for i in metadata_columns if i not in data_columns]

        return columns, validation

    def validate_columns(self):
        """
        Method to identify columns that are both in data and metadata.

        Parameters:
            data_df - dataframe of data

        Returns:
            columns_in_both - list of columns that are both in data and metadata
        """

        metadata_columns = [i.upper() for i in self.metadata_df['Attribute_Name']]
        data_columns = [i.upper() for i in self.data_df.columns]
        columns_in_both = [i for i in metadata_columns if i in data_columns]

        return columns_in_both

    def assign_row_id(self, data_df):
        """
        Method to add a unique identifier to each row in the data.

        Parameters:
            data_df - dataframe of data

        Returns:
            data_df - dataframe of data with ROW_ID column added
        """

        data_df = data_df.withColumn("ROW_ID", row_number().over(Window\
                            .orderBy(monotonically_increasing_id())))

        return data_df

    def null_check(self, data_df, column):
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

        validation = 3
        data_df = data_df.select(column, 'ROW_ID').filter(col(column).isNull())
        fail_row_id = data_df.select(collect_list('ROW_ID')).first()[0]

        return validation, column, fail_row_id

class DatatypeRulebook(GenericRulebook):
    """
    Class to run datatype specific validations on data based on metadata.
    """

    def __init__(self, data_filepath, metadata_filepath):
        """
        Method to initiate class with data filepath and metadata filepath.
        """
        super().__init__(data_filepath, metadata_filepath)

    def separate_columns_by_datatype(self, columns_in_both):
        """
        Method to group columns by their datatypes.

        Parameters:
            columns_in_both - list of columns that are both in data and metadata

        Returns:
            datatype_column_dict - dictionary with datatypes as keys and list of column names
                                as values based on metadata
        """
        datatypes = list(self.metadata_df['Data_Type'].unique())
        datatype_column_dict = {}

        for datatype in datatypes:
            if not isinstance(datatype, type(np.nan)):
                datatype_column_dict[datatype] = [value.upper() for value in self.metadata_df[
                                        (self.metadata_df['Data_Type'] == datatype).fillna(False)]\
                                ['Attribute_Name'].values if value.upper() in columns_in_both]

        return datatype_column_dict

    def separate_df_by_datatype(self, data_df, datatype_column_dict, datatype):
        """
        Method to create a subset of dataframe with columns that have given datatype.

        Parameters:
            data_df - data dataframe
            datatype_column_dict - dictionary with datatypes as keys and list of column names
                                as values based on metadata
            datatype - unique datatype from metadata

        Returns:
            datatype_df - subset of dataframe with columns of given datatype
        """

        datatype_df = data_df[datatype_column_dict[datatype] + ['ROW_ID']]

        return datatype_df

    def numeric_check(self, datatype_df, column):
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

        validation = 4
        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

        # Regex to capture non-numeric values
        regex1 = r'^[\deE.+-]+$'
        regex2 = r'^[-+]?+\d+[.]?+\d*$'
        regex3 = r'[+-]?\d(\.\d+)?[Ee][+-]?\d+'

        datatype_df = datatype_df.filter(datatype_df[column]\
                                 .rlike(regex1)).filter((datatype_df[column]\
                        .rlike(regex2)) | (datatype_df[column].rlike(regex3)))

        pass_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def integer_check(self, datatype_df, column):
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

        validation = 5

        # Integer limits
        lower = -2147483648
        upper = 2147483647

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

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
        pass_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def long_check(self, datatype_df, column):
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

        validation = 7

        # Long limits
        lower = -9223372036854775808
        upper = 9223372036854775807

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

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
        pass_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def short_check(self, datatype_df, column):
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

        validation = 6

        # Short limits
        lower_short = -32768
        upper_short = 32767

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

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
        pass_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def double_check(self, datatype_df, column):
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

        validation = 9

        # Double limits
        lower_positive = 2.225e-307
        upper_positive = 1.79769e+308
        lower_negative = -1.79769E+308
        upper_negative = -2.225E-307

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

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
        pass_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def float_check(self, datatype_df, column):
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

        validation = 8

        # Float limits
        lower_positive = 1.175494351e-38
        upper_positive = 3.402823466e38
        lower_negative = -1.175494351e-38
        upper_negative = -3.402823466e38

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        non_null_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

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
        pass_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]
        fail_row_id = [row_id for row_id in non_null_row_id if row_id not in pass_row_id]

        return validation, column, fail_row_id

    def string_check(self, datatype_df, column):
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

        validation = 10

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        str_length = self.metadata_df[self.metadata_df['Attribute_Name']\
           .str.lower() == column.lower()]['Data_Type_Length_Total'].drop_duplicates().values

        if not isnan(str_length) and datatype_df.count() != 0:
            datatype_df = datatype_df.withColumn(column, col(column).cast(StringType()))
            datatype_df = datatype_df.filter((length(trim(col(column))) > int(str_length)))
            fail_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

        else:
            fail_row_id = []

        return validation, column, fail_row_id

    def varchar_check(self, datatype_df, column):
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

        validation = 11

        datatype_df = datatype_df.select(column, 'ROW_ID').na.drop(subset=[column])
        str_length = self.metadata_df[self.metadata_df['Attribute_Name']\
           .str.lower() == column.lower()]['Data_Type_Length_Total'].drop_duplicates().values

        if not isnan(str_length) and datatype_df.count() != 0:
            datatype_df = datatype_df.withColumn(column, col(column).cast(StringType()))
            datatype_df = datatype_df.filter((length(trim(col(column))) > int(str_length)))
            fail_row_id = datatype_df.select(collect_list('ROW_ID')).first()[0]

        else:
            fail_row_id = []

        return validation, column, fail_row_id

    def datatype_validation_functions(self, datatype):
        """
        Method to identify validation fuction for a column based on its datatype.

        Parameters:
            datatype - column datatype based on metadata

        Returns:
            function - validation function
        """

        function_dict= {
            'integer' : self.integer_check,
            'float' : self.float_check,
            'double' : self.double_check,
            'long' : self.long_check,
            'short' : self.short_check,
            'numeric' : self.numeric_check,
            'string' : self.string_check,
            'varchar' : self.varchar_check
        }

        return function_dict.get(datatype, None)
