___
# What is S3 to S3 Validation:
## This is a processing validation. When someone needs to check they have gotten required objects with correct size in Target S3 bucket under a specific folder, a .csv file needs to provided for this validation.
## This version mainly support .csv in format provided by DISH Drone team: no header, 4 columns, 
1st column: Site
2nd column: Accessment
3rd column: Path
4th column: Size
## 3rd and 4th columns will be used for validation in one dataframe
## Actual values for objects on Path, Size and Date under Target folder in Target S3 bucket will be used for validation in another PySpark dataframe
## Two results dataframe:
missing_df -> values in this dataframe exist in provided file, but not in target S3 bucket's folder.
wrong_size_df -> values in this dataframe exist in both file and target S3 bucket's folder, but file size is inconsistent.

___
# How to use S3 to S3 Validation:
## Deploy resources in the same account in the same region in AWS by create CloudFormation stacks using three yaml files below.
s3validation_new.yaml -> upload and generate stack for Glue Job (function), Lambda (as a trigger from S3) and SNS (for notification)
s3validation_import.yaml -> import existing Target S3 bucket into stack above
s3validation_update.yaml -> update stack above by adding the trigger from S3 to Lambda
## Upload s3_to_s3_validation_script.py to Target folder under Target S3 bucket
## The provided file should be uploaded to trigger folder with Target S3 bucket to start the validation
## This file's name must be s3_to_s3_validation.csv (s3_to_s3_validation.csv in this folder is an example template)
## The results will be saved in S3 at this path: <TargetS3>/s3_to_s3_validation_result_<TargetS3_TargetFolder>/<results>
## Alert emails will be sent to subscribers of SNS (Target S3 name without symbol of dot, e.g.: '.')

___
# Future work:
1. File template will be more generic, not only for DISH Drone team
2. Currenly executing by AWS console, AWS CLI should be doable in order to generate work flow automatically
3. Need to explore cross accounts execution
4. Place code in GitHub and fetch by an initial trigger automatically from AWS, if cross accounts is realized
5. Be packed up as a module, so this can be imported by other developers if possible

___
