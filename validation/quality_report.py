"""
Module with classes to combine generic, datatype-specific and generate data quality report
"""
from datetime import datetime
from pytz import timezone
import pandas as pd
import numpy as np
import boto3
from data_validation import DatatypeValidation

class QualityReport(DatatypeValidation):
    """
    Class to create and populate data quality report and save it to S3.
    """
    def __init__(self, data_filepath, metadata_filepath, report_filepath, bucket_name):
        """
        Method to initiate class with data_filepath, metadata_filepath, report_filepath
        and bucket_name.
        """
        super().__init__(data_filepath, metadata_filepath, report_filepath, bucket_name)
        self.table_name = self.data_filepath.split('/')[-1]
        self.aws_account_name = self.get_aws_account_name()
        self.generate_quality_report()
        
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
        return None

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
            result_df['PRIMARY_KEY_COLUMN'] = None
            result_df['PRIMARY_KEY_VALUE'] = None
            result_df['TIMESTAMP'] = datetime.now(timezone('US/Mountain'))\
                            .strftime("%Y-%m-%d %H:%M:%S")
            result_df['AWS_ACCOUNT_NAME'] = self.aws_account_name
            result_df['S3_BUCKET'] = self.bucket_name
            result_df['VALIDATION_CATEGORY'] = None
            result_df['VALIDATION_MESSAGE'] = None

            return result_df
        
        return pd.Dataframe()
        
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
                                        .strftime("%Y-%m-%d %H:%M:%S")
            result_df['AWS_ACCOUNT_NAME'] = self.aws_account_name
            result_df['S3_BUCKET'] = self.bucket_name
            result_df['VALIDATION_CATEGORY'] = None
            result_df['VALIDATION_MESSAGE'] = None

            # When unique identifier does not exist in data, ROW_ID acts as PRIMARY_KEY_COLUMN
            result_df['PRIMARY_KEY_COLUMN'] = 'ROW_ID'

            return result_df[[ 'AWS_ACCOUNT_NAME', 'S3_BUCKET', 'TABLE_NAME', 'COLUMN_NAME',
                               'VALIDATION_CATEGORY','VALIDATION_ID', 'VALIDATION_MESSAGE',
                              'PRIMARY_KEY_COLUMN', 'PRIMARY_KEY_VALUE', 'TIMESTAMP']]
        return pd.DataFrame()
    
    def add_to_report_dataframe(self, result_df, report_df):
        """
        Method to append validation results dataframe to report dataframe.

        Parameters:
            result_df - dataframe of validation results
            report_df - dataframe with validation results from separate validation functions

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

    def generate_quality_report(self):    
        """
        Method to create, populate and save data quality report.
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
            function = self.datatype_validation_functions(key)

            if not isinstance(function, type(None)):
                result_df = self.column_validation_results(datatype_df, function)
                report_df = self.add_to_report_dataframe(result_df, report_df)

        self.save_report_to_s3(report_df= report_df)
