"""
Module that combines results from generic and datatype-specific
validation, generates data quality report and saves the report to S3.
"""
import logging
from datetime import datetime
import boto3
from botocore.errorfactory import ClientError
from pytz import timezone
import pandas as pd
import numpy as np
from .validation_rulebook import DatatypeRulebook


class QualityReport(DatatypeRulebook):
    """
    Class to combine results from generic, datatype-specific and sensitive data
    validation, generate data quality report and save the report to S3.
    """
    def __init__(self, data_filepath, metadata_filepath, vendor_name, bucket_name):
        """
        Method to initiate class with data_filepath, metadata_filepath, vendor_name
        and bucket_name.
        """
        super().__init__(data_filepath, metadata_filepath)
        self.vendor_name = vendor_name
        self.bucket_name = bucket_name
        self.resource = boto3.resource('s3')
        self.report_url = None
        self.table_name = self.data_filepath.split('/')[-1].split('.')[0]
        self.aws_account_name = self.get_aws_account_name()
        self.generate_quality_report()

    def get_aws_account_name(self):
        """
        Method to get AWS account name of S3 bucket data being validated.
        """
        folder_path = self.data_filepath.split(self.table_name)[0]\
                                .split(self.bucket_name + '/')[-1]
        bucket = self.resource.Bucket(self.bucket_name)

        try:
            for obj in bucket.objects.filter(Prefix=folder_path):
                if self.table_name in obj.key:
                    return obj.owner['DisplayName']

        except ClientError as err:
            logging.exception('Unable to get AWS account that contains data to be validated')
            logging.exception('FAIL : %s', err)
            return None

        except self.resource.meta.client.exceptions.NoSuchBucket as err:
            logging.exception('Entered bucket does not exist: %s', {self.bucket_name})
            logging.exception('FAIL : %s', err)
            return None

    def category_message(self, validation):
        """
        Method to identify validation category and message based on validation ID.

        Parameters:
            validation: validation ID

        Returns:
            validation_category: validation category in data quality report
            validation_message: validation message in data quality report
        """
        validation_dict = {
            1 : ['Generic Validation', 'Column not present in Metadata'],
            2 : ['Generic Validation', 'Column not present in Data'],
            3 : ['Generic Validation', 'Null value'],
            4 : ['Datatype Specific', 'Expected numeric datatype'],
            5 : ['Datatype Specific', 'Expected integer datatype'],
            6 : ['Datatype Specific', 'Expected short datatype'],
            7 : ['Datatype Specific', 'Expected long datatype'],
            8 : ['Datatype Specific', 'Expected float datatype'],
            9 : ['Datatype Specific', 'Expected double datatype'],
            10 : ['Datatype Specific', 'Exceeded length limitation'],
            11 : ['Datatype Specific', 'Exceeded length limitation'],
            12 : ['Datatype Specific', 'Expected IPv4 datatype'],
            13 : ['Datatype Specific', 'Expected IPv6 datatype'],
            14 : ['Datatype Specific', 'Expected epoch datatype'],
            15 : ['Datatype Specific', 'Expected timestamp datatype'],
            16 : ['Sensitive Validation', 'Encountered sensitive information'],
            17 : ['Generic Validation', 'Duplicate row']
        }

        return validation_dict.get(validation, [None, None])

    def table_validation_results(self, function):
        """
        Method to create dataframe with results from table level validation check functions.

        Parameters:
            function - table level validation function

        Returns:
            result_df - dataframe with validation results
        """

        try:
            columns, validation, column_indicator = function()

            if len(columns) > 0:
                result_df = pd.DataFrame(columns=['AWS_ACCOUNT_NAME', 'S3_BUCKET', 'TABLE_NAME',
                'COLUMN_NAME', 'VALIDATION_CATEGORY', 'VALIDATION_ID', 'VALIDATION_MESSAGE',
                'PRIMARY_KEY_COLUMN', 'PRIMARY_KEY_VALUE', 'TIMESTAMP'])
                result_df[column_indicator] = columns
                result_df['VALIDATION_ID'] = validation
                result_df['TABLE_NAME'] = self.table_name
                result_df['PRIMARY_KEY_COLUMN'] = None
                result_df['PRIMARY_KEY_VALUE'] = None
                result_df['TIMESTAMP'] = datetime.now(timezone('US/Mountain'))\
                                .strftime("%Y-%m-%d %H:%M:%S")
                result_df['AWS_ACCOUNT_NAME'] = self.aws_account_name
                result_df['S3_BUCKET'] = self.bucket_name
                result_df['VALIDATION_CATEGORY'] = self.category_message(validation)[0]
                result_df['VALIDATION_MESSAGE'] = self.category_message(validation)[1]

                return result_df

            return pd.DataFrame()

        except Exception as err:
            logging.exception('FAIL : %s', err)
            return None

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

        try:
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
                result_df['VALIDATION_CATEGORY'] = self.category_message(validation)[0]
                result_df['VALIDATION_MESSAGE'] = self.category_message(validation)[1]

                # When unique identifier does not exist in data, ROW_ID acts as PRIMARY_KEY_COLUMN
                result_df['PRIMARY_KEY_COLUMN'] = 'ROW_ID'

                return result_df[[ 'AWS_ACCOUNT_NAME', 'S3_BUCKET', 'TABLE_NAME', 'COLUMN_NAME',
                                   'VALIDATION_CATEGORY','VALIDATION_ID', 'VALIDATION_MESSAGE',
                                  'PRIMARY_KEY_COLUMN', 'PRIMARY_KEY_VALUE', 'TIMESTAMP']]
            return pd.DataFrame()

        except Exception as err:
            logging.exception('FAIL : %s', err)
            return None

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
        Method to add Report ID to quality report and save report to S3
        in CSV format if there are any data quality issues or text file
        if there are no data quality issues.

        Parameters:
            report_df - dataframe with validation results from different validation functions
        """

        now = datetime.now(timezone('US/Mountain')).strftime("%Y-%m-%d")
        report_df['DQ_REPORT_ID'] = np.arange(1,len(report_df)+1)
        report_df.set_index('DQ_REPORT_ID', inplace=True)
        report_filepath = \
        f's3a://{self.bucket_name}/qualityreport/{self.vendor_name}/{self.table_name}_report_{now}'

        if report_df.shape[0] > 0:
            try:
                report_df.to_csv(f'{report_filepath}.csv')
                self.report_url = f'{report_filepath}.csv'

            except Exception as err:
                logging.exception('Unable to save report to given S3 bucket: %s', self.bucket_name)
                logging.exception('Fail: %s', err)
        else:
            report_data = \
f'As of {now}, {self.table_name} from {self.vendor_name} does not have any data quality issues.'
            report_object = self.resource.Object(bucket_name = self.bucket_name, \
            key = f'qualityreport/{self.vendor_name}/{self.table_name}_report_{now}.txt')
            report_object.put(Body=report_data.encode())
            self.report_url = \
f's3a://{self.bucket_name}/qualityreport/{self.vendor_name}/{self.table_name}_report_{now}.txt'

    def generate_quality_report(self):
        """
        Method to create, populate and save data quality report.
        """
        # Clean column names
        self.column_name_preprocess()

        # Create a report dataframe template that will be saved in S3
        report_df = pd.DataFrame(columns=['AWS_ACCOUNT_NAME', 'S3_BUCKET', 'TABLE_NAME',
            'COLUMN_NAME', 'VALIDATION_CATEGORY', 'VALIDATION_ID', 'VALIDATION_MESSAGE',
            'PRIMARY_KEY_COLUMN', 'PRIMARY_KEY_VALUE', 'TIMESTAMP'])

        # Result dataframe from column name validation checks
        result_df = self.table_validation_results(self.validate_data_columns)
        report_df = self.add_to_report_dataframe(result_df, report_df)
        result_df = self.table_validation_results(self.validate_metadata_columns)
        report_df = self.add_to_report_dataframe(result_df, report_df)

        # Assign unique ID to each row of data
        columns_in_both = self.validate_columns()
        datatype_column_dict = self.separate_columns_by_datatype(columns_in_both)
        self.data_df = self.assign_row_id(self.data_df)

        # Result dataframe from duplicate row check
        result_df = self.table_validation_results(self.duplicate_check)
        report_df = self.add_to_report_dataframe(result_df, report_df)

        # Result dataframe form null validation check
        result_df = self.column_validation_results(self.data_df, self.null_check)
        report_df = self.add_to_report_dataframe(result_df, report_df)

        # Result dataframe form sensitive information validation check
        result_df = self.column_validation_results(self.data_df, self.sensitive_information_check)
        report_df = self.add_to_report_dataframe(result_df, report_df)

        # Result dataframe from datatype specific validation check
        for key in datatype_column_dict:
            datatype_df = self.separate_df_by_datatype(self.data_df, datatype_column_dict, key)
            function = self.datatype_validation_functions(key)

            if not isinstance(function, type(None)):
                result_df = self.column_validation_results(datatype_df, function)
                report_df = self.add_to_report_dataframe(result_df, report_df)

        self.save_report_to_s3(report_df= report_df)
