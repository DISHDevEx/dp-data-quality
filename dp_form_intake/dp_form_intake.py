"""Module to host DpFormIntake class and main function"""
import json
import boto3
import pandas as pd
from jira import JIRA

class DpFormIntake:
    '''
    Class to handle processing of Data Platform intake form.
    Saves the filled form to S3.
    Saves a JSON of the submission to S3.
    Saves a JSON of any user additions for domain/category options to S3.
    Creates a Jira issue in the Data Product Intake epic with info from form.
    '''
    def __init__(self, filled_form_name, submission_name, additions_name):
        '''
        Method to initialize DpFormIntake class.

        INPUT:
            filled_form_name - folder path and name of the filled form
            submission_name - folder path and name of submission JSON
            additions_name - folder path and name of additions JSON
        '''

        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.bucket = 'dp-intake-form'
        self.form_key = f'filled-forms/{filled_form_name}'
        self.submission_path = f'submissions/{submission_name}'
        self.additions_path = f'additions/{additions_name}'
        self.jira = JIRA(
            server='https://dish-wireless-network.atlassian.net/',
            basic_auth=('tomer.danon@dish.com', 'RmVDsNs2ozj62XbdTwI7FD99')
        )

    def form_to_df(self):
        '''
        Method that creates a dataframe from the filled form.

        INPUT:
            self

        OUTPUT:
            form_df - dataframe of form
        '''

        s3_form = self.client.get_object(Bucket=self.bucket, Key=self.form_key)
        s3_form_body = s3_form['Body'].read()
        form_df = pd.read_excel(bytes(s3_form_body), sheet_name='Form', header=None)
        return form_df

    def wireless_domain_selection(self, form_df):
        '''
        Method to collect the user selections for Wireless Domain.

        INPUT:
            form_df - dataframe of form

        OUTPUT:
            wireless_domain - list of user selections
        '''

        wireless_domain = form_df.iloc[6:32, 0:2]
        wireless_domain = wireless_domain[wireless_domain[1].notna()][0].tolist()
        return wireless_domain

    def data_domain_selection(self, form_df):
        '''
        Method to collect the user selections for Data Domain.

        INPUT:
            form_df - dataframe of form

        OUTPUT:
            data_domain - list of user selections
        '''

        data_domain = form_df.loc[6:32, 2:4]
        data_domain = data_domain[data_domain[3].notna()][2].tolist()
        return data_domain

    def data_category_selection(self, form_df):
        '''
        Method to collect the user selections for Data Category.

        INPUT:
            form_df - dataframe of form

        OUTPUT:
            data_category - list of user selections
        '''

        data_category = form_df.iloc[6:32, 4:6]
        data_category = data_category[data_category[5].notna()][4].tolist()
        return data_category

    def wireless_domain_adds(self, form_df):
        '''
        Method to collect the user additions for Wireless Domain.

        INPUT:
            form_df - dataframe of form

        OUTPUT:
            wireless_domain_add - list of user additions to Wireless Domain section
        '''

        wireless_domain_add = form_df.iloc[22:32, 0:2]
        wireless_domain_add = wireless_domain_add[wireless_domain_add[1].notna()][0].tolist()
        return wireless_domain_add

    def data_domain_adds(self, form_df):
        '''
        Method to collect the user additions for Data Domain.

        INPUT:
            form_df - dataframe of form.

        OUTPUT:
            data_domain_add - list of user additions to Data Domain section
        '''

        data_domain_add = form_df.iloc[22:32, 2:4]
        data_domain_add = data_domain_add[data_domain_add[3].notna()][2].tolist()
        return data_domain_add

    def data_category_adds(self, form_df):
        '''
        Method to collect the user additions for Data Category.

        INPUT:
            form_df - dataframe of form

        OUTPUT:
            data_category_add - list of user additions to Data Category section
        '''

        data_category_add = form_df.iloc[22:32, 4:6]
        data_category_add = data_category_add[data_category_add[5].notna()][4].tolist()
        return data_category_add

    def save_submission(self, submission):
        '''
        Method to convery 'submission' dictionary into JSON file and save to s3.

        INPUT:
            submission - dictionary of the user submission

        OUTPUT:
            None
        '''

        submission_json = json.dumps(submission, indent=4).encode('UTF-8')
        submission_object = self.resource.Object(self.bucket, self.submission_path)
        submission_object.put(Body=bytes(submission_json))

    def save_additions(self, additions):
        '''
        Method to convery 'additions' dictionary into JSON file and save to s3.

        INPUT:
            additions - dictionary of the user additions

        OUTPUT:
            None
        '''

        additions_json = json.dumps(additions, indent=4).encode('UTF-8')
        additions_object = self.resource.Object(self.bucket, self.additions_path)
        additions_object.put(Body=bytes(additions_json))

    def create_jira_issue(self, submission):
        '''
        Method to create a Jira issue based on the information collected from the form.

        INPUT:
            submission - dictionary of the user submission

        OUTPUT:
            None
        '''

        issue_description = f"""
        *Requester Name*: {submission['*Requester Name']}
        *Requester Email*: {submission['*Requester Email']}
        *Requester Organization*: {submission['*Requester Organization']}
        *Requester Team*: {submission['*Requester Team']}
        *Problem Statement*: {submission['*Problem Statement']}
        *Wireless Domain*: {', '.join(submission['Wireless Domain'])}
        *Data Domain*: {', '.join(submission['Data Domain'])}
        *Data Category*: {', '.join(submission['Data Category'])}
        """

        self.jira.create_issue(
            project = {'key':'MSS'},
            issuetype ={ 'name':'Story'},
            summary = 'Data Product Form Request',
            description = issue_description,
            parent = {'key':'MSS-2697'},
        )

def main():
    '''
    Main function to initiate variables and class then call methods in order.
    '''
    # Template gets downloaded to user with id
    # S3 bucket will notify lambda that an object was added.
    # Get object name
    filled_form_name = 'dpi_form_tomer.xlsx' # object name
    submission_name = 'dpi_form_id_submission.json' # id of template
    additions_name = 'dpi_form_id_additions.json' # id of template
    dpf = DpFormIntake(filled_form_name, submission_name, additions_name)

    form_df = dpf.form_to_df()
    submission = form_df.iloc[0:5,0:2].set_index(0).to_dict()[1]
    submission['Wireless Domain'] = dpf.wireless_domain_selection(form_df)
    submission['Data Domain'] = dpf.data_domain_selection(form_df)
    submission['Data Category'] = dpf.data_category_selection(form_df)

    additions = {}
    additions['Wireless Domain'] = dpf.wireless_domain_adds(form_df)
    additions['Data Domain'] = dpf.data_domain_adds(form_df)
    additions['Data Category'] = dpf.data_category_adds(form_df)

    dpf.save_submission(submission)
    dpf.save_additions(additions)
    dpf.create_jira_issue(submission)

if __name__ == '__main__':
    main()
    