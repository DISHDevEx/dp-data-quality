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



# input_df = pd.read_csv('AWS- Storage Inventory.xlsx - Inventory of Data Resources.csv')
# input_df = pd.read_excel('AWS- Storage Inventory.xlsx', sheet_name = 'Inventory of Data Resources')
# input_df = pd.read_excel('AWS- Storage Inventory.xlsx', sheet_name = 'Inventory of Data Resources', usecols = 'G')
# print(input_df.head())
# print(len(input_df))
# print(input_df["resourceName"][80])
# print(input_df["resourceName"][81])
# print(input_df["resourceName"][82])
# print(input_df["resourceName"][83])
# print(input_df["resourceName"][84])
# print(input_df["resourceName"][85])
# print(input_df["resourceName"][86])
# print(input_df["resourceName"][87])
# print(input_df["resourceName"][88])
# print(input_df["resourceName"][89])
# print(input_df["resourceName"][90])
# print('\n')

def split_by_dot(aString):
    """
    separate string by dot into a list of values
    EXAMPLE:
    input = "'a'.'b'.'c'.'d'"
    output = ['a','b','c','d']
    """
    aString = aString.split('.')
    return aString

# need to filter s3 resoucetype. refer to unittest1.py
def file_validation (file_path, rule_list, mandatory_format):
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

    for i in range(len(input_df)):
        if type(input_df['resourceName'][i]) == float:
            input_df.loc[i, 'resourceName']=str(input_df['resourceName'][i])
        if not isinstance(input_df['resourceName'][i],str):
            logger.info(f'{i} not string')




    # for i in range(85,90):
    for i in range(len(input_df)):
        fail_validation = False
        validation_result = {}
        write_line = {}
        if len(input_df['resourceName'][i].strip()) > 0:
            preline= input_df['resourceName'][i]
            print(f'preprocessing line is {preline} on {i} row')
            line = split_by_dot(input_df['resourceName'][i])
            print(f'line is {line} on {i} row')
            if len(line) < 6:
                fail_validation = True
                write_line = {"required":mandatory_format, "actual":line}
                validation_result[i+2] = write_line
                outfile_write = f"Minimum first 6 components  with dot as separator:\n \
                    validating -> {line}\nrequired -> {write_line['required']}\n\n"
                try:
                    print('saving failed result')
                    output_txt = "result/failed_s3name.txt"
                    with open(output_txt, "a") as outfile:
                        outfile.write(f"actual -> {line}\nat least -> 6 components seprated by dot\n\n")


                except:
                    output_txt = "result/failed_s3name.txt"
                    with open(output_txt, "w") as outfile:
                        outfile.write(f"actual -> {line}\nat least -> 6 components seprated by dot\n\n")


            else:
                for j in range(len(line)):
                    if line[j] not in rule_list[j]:
                        fail_validation = True
                        write_line = {f"position {j+1}":rule_list[j], "required":rule_list[j]}
                        logger.info(f'position {j+1} -> {line[j]}\nrequired -> {rule_list[j]}\n')
                        validation_result[i+2] = write_line
                        outfile_write = f"position {j+1} -> {line[j]}\nrequired -> {write_line['required']}\n\n"
                        output_txt = "result/failed_s3name.txt"
                        try:
                            output_txt = "result/failed_s3name.txt"
                            with open(output_txt, "a") as outfile:
                                outfile.write(outfile_write)

                        except:
                            output_txt = "result/failed_s3name.txt"
                            with open(output_txt, "w") as outfile:
                                outfile.write(outfile_write)
                        break
            if fail_validation == False:
                validated_txt = "result/validated_s3name.txt"
                try:
                    with open(validated_txt, "a") as outfile:
                        outfile.write(input_df['resourceName'][i]+'\n')
                except:
                    with open(validated_txt, "w") as outfile:
                        outfile.write(input_df['resourceName'][i]+'\n')


                    
        # if len(write_line) == 0:
        #     logger.info(f'{input_df["resourceName"][i]} is a valid S3 bucket name.')
        #     validated_txt = "result/validated_s3name.txt"
        #     with open(validated_txt, "a") as outfile:
        #         outfile.write(input_df['resourceName'][i]+'\n')

        #     validation_result = {"validated": input_df['resourceName'][i]}
        #     output_file = "result/validated_s3name.json"
        #     with open(output_file, "a") as outfile:
        #         json.dump(validation_result, outfile)

        # else:
        #     failed_txt = "result/failed_s3name.txt"
        #     with open(failed_txt, "a") as outfile:
        #         outfile.write(outfile_write)

        #     failed_json = f"result/naming_validation_result_{file_path}.json"
        #     with open(failed_json, "a") as outfile:
        #         json.dump(write_line, outfile)


            # try:
            #     with open(failed_json, "r+") as infile:
            #         jsondata = json.load(infile)
            #         jsondata.append(write_line)
            #         infile.seek(0)
            #         json.dump(jsondata,infile)
            # except:
            #     with open(failed_json, "w") as outfile:
            #         json.dump(write_line, outfile)

            # with open(failed_json, 'r') as infile:
            #     json_object = json.load(infile)

            # pprint.pprint(json_object)


def manual_validation(s3name, rule_list, mandatory_format):
    """
    let user to type in a s3name and check it based on rule_list, save invalid ones in invalid file
    with mandatory_format, and save valid in valid file.
    EXAMPLE:
    input = a.b.c.d.e.f
    output = appended in failed_s3name.txt and validated_s3name.txt
    """
    fail_validation = False
    validation_result={}
    if len(s3name) > 0:
        line = split_by_dot(s3name)
        if len(line) < 6:
            fail_validation = True
            write_line = {"required":mandatory_format, "actual":line}
            validation_result = write_line
            outfile_write = f"Minimum first 6 components with dot as separator:\n \
                validating -> {line}\nrequired -> {write_line['required']}\n\n"
        else:
            for j in range(len(line)):
                if line[j] not in rule_list[j]:
                    fail_validation = True
                    write_line = {f"position {j+1}":line[j], "required":rule_list[j]}
                    # write_line = {"position":j+1, "required":rule_list[j], "actual":line[j]}
                    logger.info(f'position {j+1} -> {line[j]}\nrequired -> {rule_list[j]}\n')
                    validation_result= write_line
 
                    outfile_write = f"position {j+1} -> {line[j]}\nrequired -> {write_line['required']}\n\n"
                    break

        
        if fail_validation == False:
            print(f'{s3name} is a valid S3 bucket name.')
            validated_txt = "result/validated_s3name.txt"
            try:
                with open(validated_txt, "a") as outfile:
                    outfile.write(s3name+'\n')
            except:
                with open(validated_txt, "w") as outfile:
                    outfile.write(s3name+'\n')

        else:
            failed_txt = "result/failed_s3name.txt"
            try:
                with open(failed_txt, "a") as outfile:
                    outfile.write(outfile_write)

            except:
                with open(failed_txt, "w") as outfile:
                    outfile.write(outfile_write)



def main() -> None:
    """
    change format of the rule from json to list and pass it to the funcitons above
    EXAMPLE:
    input = the rule_file
    output = invoke functions above
    """
    logger.info("reading rules")
    rule_file = "naming_validation_rules_version1.json"
    try:
        with open(rule_file, 'r') as infile:
            rule_json = json.load(infile)
    except:
        logger.info(f"No rule file found in path {rule_file}")
        print(f"No rule file found in path {rule_file}")
        return

    logger.info("rules read in JSON")

    # rule_json["Deployment Stage/ Environment"]
    # rule_json["Region"]
    # rule_json["Organization"]
    # rule_json["Wireless Domain"]
    # rule_json["Functional Area"]
    # rule_json["Data Centre/Platform"]
    # rule_json["Capability"]
    # rule_json["Data Domain"]
    # rule_json["DataType"]
    # rule_json["Data State"]

    mandatory_format = rule_json["Deployment Stage/ Environment"]+'.'+rule_json["Region"]+'.' \
        +rule_json["Organization"]+'.'+rule_json["Wireless Domain"]+'.'+rule_json["Functional Area"]+'.' \
        +rule_json["Data Centre/Platform"]+'.'+rule_json["Capability"]+'.'+rule_json["Data Domain"]+'.'+rule_json["DataType"] \
        +'.'+rule_json["Data State"] 

    rule_list = []
    rule_list.append(rule_json["Deployment Stage/ Environment"])
    rule_list.append(rule_json["Region"])
    rule_list.append(rule_json["Organization"])
    rule_list.append(rule_json["Wireless Domain"])
    rule_list.append(rule_json["Functional Area"])
    rule_list.append(rule_json["Data Centre/Platform"])
    rule_list.append(rule_json["Capability"])
    rule_list.append(rule_json["Data Domain"])
    rule_list.append(rule_json["DataType"])
    rule_list.append(rule_json["Data State"])

    input_info = "Please select option below:\n\t \
        1. validate one S3 name manually. (type 1 and then enter)\n\t \
        2. validate S3 names from a csv file. (type 2 and then enter)\n \
        (click 'n or N' to quit)"
    userinput = None
    while userinput not in ("1", "2", "n", "N"):
        userinput = input(input_info)
    if userinput == "2":
        input_info_2 = "Please input the file path to start validation.\n(click 'n or N' to quit)\n"
        userconfirm = None
        s3file = None
        while userconfirm not in ("y","Y"):
            s3file = input(input_info_2)
            if s3file == "n" or s3file =="N":
                logger.info("User terminaled validation.")
                return
            userconfirm = input(f'Please type y or Y to confirm the file path:\n\t \
            {s3file}\nor type a new file path.\n \
            (click "n or N" to quit)\n')
            if userconfirm in ("n", "N"):
                logger.info("User terminaled validation.")
                return
            #example file path s3file = "naming_validation_rules.json"

        file_validation (s3file, rule_list, mandatory_format)

    elif userinput == "1":
        input_info_1 = "Please input S3 bucket name (cannot be empty) to start validation.\n(click 'n or N' to quit)\n"
        userconfirm = None
        s3name = "sample"
        while userconfirm not in ("y","Y") and len(s3name.strip()) > 0:
            s3name = input(input_info_1)
            if s3name in ("n", "N"):
                logger.info("User terminaled validation.")
                return
            userconfirm = input(f'Please type y or Y to confirm the s3 bucket name:\n\t \
            {s3name}\nor type any other key to type a new file path.\n(click "n or N" to quit)\n')
            if userconfirm in ("n", "N"):
                logger.info("User terminaled validation.")
                return
        manual_validation(s3name, rule_list, mandatory_format)

    elif userinput == "n" or userinput =="N":
        print("User termianted validation.")
        logger.info("User terminaled validation.")
        return
    else:
        logger.info("Validation terminated unexpected.")     
        return
if __name__ == "__main__":
    main()
    print('successful execution')