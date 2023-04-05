# DISHDataQuality


## Glue catalog validation: 
___
## Main purpose:
The main purpose of Glue Validation module is to validate Glue Crawler by comparing Glue Database with S3 Bucket. When a user uses Glue Crawler to generate Glue Database based on S3 bucket, scans the S3 bucket and compared the tables under this bucket with tables under Glue Database. The result of comparison is saved in the same S3 bucket and an alert email is sent to the subscribers of SNS.


## 2 files:
glue_catalog_validation.py:
This file will generate two lists by scanning target S3 bucket and read from Glue Catalog Database. Glue Catalog Database and target S3 bucket should have the same name.
Because Glue Catalog Crawler will transform all punctuations into underscore when crawlering S3 to generate tables in database, as well as convert all uppercase to lowercase.
Validation result will be save in the same S3 bucket under folder of 'glue_database_validation' and an alert email will be sent to the subscribers of the SNS.

glue_catalog_validation.yaml:
This file can be used to create Glue Job role, Glue Job and SNS topic, and their name should follow naming convention rules.
Python code should sit (glue_catalog_validation.py) at the S3 bucket's top level.
SNS topic will send out results to subscribers by emails.
Sample stackname: aws-5g--dp--mss--data-science--dev---gluevalidation (Details in README)

## Step to implement:
Prerequisites:
Access to AWS account, AWS Glue, AWS S3, AWS SNS and AWS CloudFormation. Glue Database has the same name as the S3 bucket.

Steps:
1. Upload Python file (glue_catalog_validation.py) to S3 bucket's top level.
2. Upload YAML file (glue_catalog_validation.yaml) from local machine (or S3 if it is saved there) to CloudFormation to deploy stack
to create Glue Job role, Glue Job and SNS topic.
stackname should be <dish aws account name (replace dot with two dashes)>---gluevalidation
3. Go to glue job with name as stackname and then click run button on right top conner to trigger the validation work.
4. Results should be saved in same S3 under 'glue_database_validation' folder, and an alert will be sent to metadq@dish.com. (more emails can be added in SNS)
5. Once one validation work is done, the stack should be deleted in CloudFormation.

Example of stackname for glue_catalog_validation.yaml:
account name -> aws-5g.dp.mss.data-science.dev
stack name -> aws-5g--dp--mss--data-science--dev---gluevalidation

## Future work:
Should learn how to get Glue Job run automatically from some kind of trigger.
___

## Pylint score:
pylint glue_validation/glue_catalog_validation.py

-------------------------------------------------------------------
Your code has been rated at 10.00/10 (previous run: 9.78/10, +0.22)