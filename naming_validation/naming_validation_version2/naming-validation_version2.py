"""
!/usr/bin/env python3  
#--------------------------------------------------------------------
# File    :   glue-catalog-valiation
# Time    :   2022/12/28 10:59:01
# Author  :   Zheng Liu
# Version :   1.0
# Desc    :   function to check if names are valid from file or manually.

---------------------------Version History---------------------------
SrNo    DateModified    ModifiedBy   Description
1       2022/12/28      Zheng        Initial Version
2       2023/01/03      Zheng        Complete update based on the experience from version 0
3       2023/01/10      Zheng        Save to current folder instead of a sub folder
4       2023/01/11      Zheng        Delete user inputs for saving locations

#--------------------------------------------------------------------
"""

import json
import pandas as pd
import pprint
import logging

logging.basicConfig(filename="naming-validation.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.info("new validation started")

# with open(rule_file, 'r') as infile:
#     rule_json = json.load(infile)

def read_rule(rule_path):
    """
    change format of the rule from json to list and return it to use in the validation funcitons 
    EXAMPLE:
    input = naming_validation_rules_template.json
    output = com_key_list(components names), com_value_list(components values), length of rule_list and mandatory_format
    """
    logger.info("reading rules")
    # example: rule_path = "naming_validation_rules_template.json"
    try:
        # Read the rule from json file
        with open(rule_path, 'r') as infile:
            rule_json = json.load(infile)
        com_key_list = list(rule_json.keys())
        com_value_list = list(rule_json.values())
        length_rule_list = len(com_key_list)
        logger.info('com_value_list')
        logger.info(com_value_list)
        # If there is empty value at one component, the rule is invalid
        for item in com_value_list:
            if len(item)==0:
                return
        mandatory_format = '.'.join(str(sub_list) for sub_list in com_value_list)
        logger.info("rules read in JSON")
        logger.info('mandatory_format')
        logger.info(mandatory_format)
        return com_value_list, length_rule_list, mandatory_format 

    except:
        logger.info(f"No rule file found in path {rule_path}")
        print(f"No rule file found in path {rule_path}")
        return

# Generate a list of components based on the ingested S3 name.
def split_by_dot(aString):
    """
    separate string by dot into a list of values
    EXAMPLE:
    input = "'a'.'b'.'c'.'d'"
    output = ['a','b','c','d']
    """
    aString = aString.split('.')
    return aString

# Batch validaiton with S3 names as a column in a csv file, "result/failed_s3name.txt" example of failed_s3name_path
# "result/validated_s3name.txt" example of validated_s3name_path, resourceName is an example of column_name
def file_validation (file_path, column_name, length_rule_list, com_value_list, mandatory_format, failed_s3name_path = 'failed_s3name.txt', validated_s3name_path = 'valildated_s3name.txt'):
    """
    read in file with names in lines and check each of them based on rule_list, save invalid ones in invalid file
    with mandatory_format, and save valid in valid file.
    EXAMPLE:
    input = test.csv
    output = appended in failed_s3name.txt and validated_s3name.txt
    """
    try:
        input_df = pd.read_csv(file_path)
    except:
        logger.info(f'No file found in path {file_path}')
        print(f'No file found in path {file_path}')
        return None

    # Empty value on a row, the data type is float, must convert it to string for validation
    for i in range(len(input_df)):
        if type(input_df[column_name][i]) == float:
            input_df.loc[i, column_name]=str(input_df[column_name][i])
        if not isinstance(input_df[column_name][i],str):
            logger.info(f'{i} not string')

    # Iterate all rows of S3 names for validation
    for i in range(len(input_df)):
        fail_validation = False
        one_s3_name_string= input_df[column_name][i].strip()
        # If length of S3 name does not match with the required length, it is invalid
        if len(one_s3_name_string) > 0:
            # one_s3_name_string is one S3 name on a row
            print(f'preprocessing line is {one_s3_name_string} on {i} row')
            one_s3_name_list = split_by_dot(input_df[column_name][i])
            print(f'this s3 name as list is {one_s3_name_list} on {i} row')
            length_one_s3_name_list = len(one_s3_name_list)
            if length_one_s3_name_list == length_rule_list:
                for j in range(len(one_s3_name_list)):
                    if one_s3_name_list[j] not in com_value_list[j]:
                        fail_validation = True
                        output_result = f"position {j+1}:{one_s3_name_list[j]}, required:{com_value_list[j]}"
                        logger.info(f'position {j+1} -> {one_s3_name_list[j]}\nrequired -> {com_value_list[j]}\n')
                        try:
                            with open(failed_s3name_path, "a") as outfile:
                                outfile.write(output_result+'\n')
                        except:
                            with open(failed_s3name_path, "w") as outfile:
                                outfile.write(output_result+'\n')
                        print(f'failed names saved at {failed_s3name_path}')
                        break
                if fail_validation == False:
                    try:
                        with open(validated_s3name_path, "a") as outfile:
                            outfile.write(one_s3_name_string+'\n')
                    except:
                        with open(validated_s3name_path, "w") as outfile:
                            outfile.write(one_s3_name_string+'\n')
                    print(f'validated names saved at {validated_s3name_path}')
            else:
                output_result = f"mismatch length: actual->{length_one_s3_name_list} required->{length_rule_list}, actual:{one_s3_name_string}, required:{mandatory_format}"
                try:
                    with open(failed_s3name_path, "a") as outfile:
                        outfile.write(output_result+'\n')
                except:
                    with open(failed_s3name_path, "w") as outfile:
                        outfile.write(output_result+'\n')
                print(f'failed names saved at {failed_s3name_path}')
                        
# Single validation based on user input
def manual_validation(s3name, length_rule_list, com_value_list, mandatory_format, failed_s3name_path = 'failed_s3name.txt', validated_s3name_path = 'valildated_s3name.txt'):
    """
    let user to type in a s3name and check it based on rule_list, save invalid ones in invalid file
    with mandatory_format, and save valid in valid file.
    EXAMPLE:
    input = a.b.c.d.e.f
    output = appended in failed_s3name.txt and validated_s3name.txt
    """
    fail_validation = False
    one_s3_name_string= s3name
    logger.info('s3name in manual')
    logger.info(one_s3_name_string)
    # If length of S3 name does not match with the required length, it is invalid
    if len(one_s3_name_string) > 0:
        # one_s3_name_string is the userinput
        one_s3_name_list = split_by_dot(one_s3_name_string)
        logger.info(f'this s3 name as list is {one_s3_name_list}')
        length_one_s3_name_list = len(one_s3_name_list)
        if length_one_s3_name_list == length_rule_list:
            for j in range(len(one_s3_name_list)):
                if one_s3_name_list[j] not in com_value_list[j]:
                    fail_validation = True
                    output_result = f"position {j+1}:{one_s3_name_list[j]}, required:{com_value_list[j]}"
                    print('output_result')
                    print(output_result)
                    logger.info(f'position {j+1} -> {one_s3_name_list[j]}\nrequired -> {com_value_list[j]}\n')
                    try:
                        print(f'appending to {failed_s3name_path}')
                        with open(failed_s3name_path, "a") as outfile:
                            outfile.write(output_result+'\n')
                        print(f'appended to {failed_s3name_path}')
                    except:
                        print(f'writing to {failed_s3name_path}')
                        with open(failed_s3name_path, "w") as outfile:
                            outfile.write(output_result+'\n')
                        print(f'written to {failed_s3name_path}')
                    print(f'failed names saved at {failed_s3name_path}')
                    
                    break
            if fail_validation == False:
                try:
                    print(f'appending to {validated_s3name_path}')
                    with open(validated_s3name_path, "a") as outfile:
                        outfile.write(one_s3_name_string+'\n')
                    print(f'appended to {validated_s3name_path}')
                except:
                    print(f'writing to {validated_s3name_path}')
                    with open(validated_s3name_path, "w") as outfile:
                        outfile.write(one_s3_name_string+'\n')
                    print(f'written to {validated_s3name_path}')
                print(f'validated names saved at {validated_s3name_path}')
        else:
            output_result = f"mismatch length: actual->{length_one_s3_name_list} required->{length_rule_list}, actual:{one_s3_name_string}, required:{mandatory_format}"
            try:
                with open(failed_s3name_path, "a") as outfile:
                    outfile.write(output_result+'\n')
            except:
                with open(failed_s3name_path, "w") as outfile:
                    outfile.write(output_result+'\n')
            print(f'failed names saved at {failed_s3name_path}')


def main() -> None:
    """
    change format of the rule from json to list and pass it to the funcitons above
    EXAMPLE:
    input = the rule_file
    output = invoke functions above
    """
    logger.info("reading rules in main")
    rule_path_input=''
    rule_path_input = input('Please input the rule path:\n')
    while len(rule_path_input)==0:
        rule_path_input = input('Please input the rule path:\n')
    rule_path = rule_path_input
    # rule_path = "naming_validation_rules_version1.json"
    try:
        com_value_list, length_rule_list, mandatory_format = read_rule(rule_path)
    except:
        logger.info(f"No rule file found in path {rule_path}")
        print(f"No rule file found in path {rule_path}")
        return
    logger.info("rules read in JSON in main")

    # Let user to choose between manual validaiton (single S3 name) or file validation (multi S3 names).
    input_info = "Please select option below:\n\t \
        1. validate one S3 name manually. (type 1 and then enter)\n\t \
        2. validate S3 names from a csv file. (type 2 and then enter)\n \
        (click 'n or N' to quit)"
    userinput = None
    while userinput not in ("1", "2", "n", "N"):
        userinput = input(input_info)
    if userinput == "2":
        input_info_2 = '''
Please input file_path, column_name
Separated by comma.
e.g. test.csv,resourceName
'''

        userconfirm = None
        while userconfirm not in ("y","Y"):
            required_arquements = input(input_info_2)
            if required_arquements == "n" or required_arquements =="N":
                logger.info("User terminaled validation.")
                return
            elif len(required_arquements) == 0:
                logger.info("No value from user input in main.")
                return
            userconfirm = input(f'Please type y or Y to confirm the arquements:\n\t \
            \nor type a new file path.\n \
            (click "n or N" to quit)\n')
            if userconfirm in ("n", "N"):
                logger.info("User terminaled validation in main.")
                return              
            required_arquements_list = required_arquements.split(',')
            if len(required_arquements_list) != 2:
                logger.info("missing arguement from user input in main.")
                return
            for item in required_arquements_list:
                if len(item) == 0:
                    logger.info("missing arguement from user input in main.")
                    return    
        file_path, column_name = required_arquements_list
        try:
            file_validation (file_path, column_name, length_rule_list, com_value_list, mandatory_format)
        except:
            print('Cannot do file validaiton based on provided info')
            logger.info("Cannot do file validaiton based on provided info in main.")
    elif userinput == "1":
        input_info_1 = '''
Please input s3name
invalid example: dish.ran.bhla.bhla.etc
valid example: d.use1.dish.ran.aws.b.dp.subs.fm.r
'''

        userconfirm = None
        while userconfirm not in ("y","Y"):
            required_arquements = input(input_info_1)
            if required_arquements == "n" or required_arquements =="N":
                logger.info("User terminaled validation.")
                return
            elif len(required_arquements) == 0:
                logger.info("No value from user input in main.")
                return
            userconfirm = input(f'Please type y or Y to confirm the arquements:\n\t \
            \nor type a new file path.\n \
            (click "n or N" to quit)\n')
            if userconfirm in ("n", "N"):
                logger.info("User terminaled validation in main.")
                return              
 
        logger.info('required_arquements in manual from main')
        logger.info(required_arquements)
        s3name = required_arquements
        
        try:
            print('doing manual validation')
            manual_validation(s3name, length_rule_list, com_value_list, mandatory_format)
            print('manual validaiton is done')
        except:
            print('Cannot do manual validaiton based on provided info')
            logger.info("Cannot do manual validaiton based on provided info in main.")

    elif userinput == "n" or userinput =="N":
        print("User termianted validation.")
        logger.info("User terminaled validation in main.")
        return
    else:
        logger.info("Validation terminated unexpected in main.")     
        return
if __name__ == "__main__":
    main()
    print('successful execution')