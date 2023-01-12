# Naming_Validation
___


## Version 2:
___
Environment prerequisite: Python 3.X and Padas intalled. Under this folder, run 'python namingvalidation_version2' and follow the console instruction to do the validaiton. .csv file is required to be placed in the same folder. 
Provide a JSON template (naming_validation_rules_template.json) as an example for users to generate new rules under agreed format. On the same folder, please create a sub folder called result.
Naming Convention Rules link: https://docs.google.com/document/d/1uuzWIaokjJ2azOyCiFFwXN3-WlIqP8eFHR3K0Vvb2kE/edit?usp=sharing
Please reach out to neha.yalamanchi@dish.com if you need permission to read it.
The components should be separated by symbol of ./_/-, user needs to input delimiter from 1 of these three in main function. Validation will start from the first component and then the second...until the last component. If all components are valid, the result is saved in validated_s3name.txt. If any component is invalid, the result is saved in failed_s3name.txt and end the execution.
The path of the rule should be passed to read_rule function, so that the values can be used for validation.
Only correct format is useable.
All components become mandatory for validation.
Example of how to use naming-validation_version2.py script: naming_validation_rules_template.json: is the input file for the rules. manual_validation(): dish.ran.bhla.bhla.etc,result/failed_s3name.txt,result/validated_s3name.txt (as an invalid example) d.use1.dish.ran.aws.b.dp.subs.fm.r,result/failed_s3name.txt,result/validated_s3name.txt (as a valid example) file_validation(): test.csv,resourceName,result/failed_s3name.txt,result/validated_s3name.txt (test.csv should be in the same folder of naming-validation_version2.py while executing the py file on that folder level)
Manual method inputs: rule_path and S3 name
File method inputs: rule_path, actual_file_path and column_name in actual_file
Outputs: failed_s3name.txt if invalid and validated_s3name.txt if valid
## Step to use this code:
1. Use your local machine to change working directory to this folder.
2. Make sure most recent Python installed on the working environment.
3. In terminal, run command: python naming-validation_version2.py.
4. Input path of the rule for validation.
5. Follow instructions displayed on screen to proceed for either manual input (one at a time) or file input (batch processing).
6. Can exit execution by input 'n' or 'N' during step 4.
7. By completing execution, the valid S3 names are saved in validated_s3name.txt and the invalid S3 names are saved in failed_s3name.txt.

## Inputs:
path of the rule
s3 name for manual method
path of the file for file method
column name for file method

## Outputs:
validated_s3name.txt
failed_s3name.txt

___