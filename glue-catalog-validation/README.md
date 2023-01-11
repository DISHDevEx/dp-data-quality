# DISHDataQuality


## Glue catalog validation: 
___
## Main purpose:
If user use Glue Crawler to generate Glue Database based on S3 bucket, this code can be used to validate the tables under Glue Database by reading tables from it as well as scanning associated S3 bucket, and then do validaiton work. The result will saved in result S3 and alert emails would be sent to subscribers.


## 3 files:
glue-catalog-validation.yaml:
This file can be used to create result S3 and SNS topic, and their name should follow naming convention rules.
Result S3 will hold the validation python code (glue-catalog-validation.py) under folder of validationfile, and save validation results.
SNS topic will send out results to subscribers by emails.
Sample stackname: aws-5g--dp--mss--data-science--dev---gluevalidation (Details in README)

glue-catalog-validation-glue-job.yaml:
After creating result S3 and SNS topic, this file can be used to create glue job to do the validation work.
Glue job file is saved in Result S3 and will be fetched during resource deployment.
Sample stackname: aws-5g--dp--mss--data-science--dev---s3-validation-demo (Details in README)

glue-catalog-validation.py:
This file will generate two lists by scanning target S3 bucket and read from Glue Catalog Database. Glue Catalog Database and target S3 bucket should have the same name.
There is a function called remove_punctuation is to modify the list values on S3 side, because Glue Catalog Crawler will transform all punctuations into underscore when crawlering S3 to generate tables in database, as well as convert all uppercase to lowercase.
Validation result will be save in result S3 bucket and an alert email will be sent to the subscribers of the SNS.

## Step to implement:
Prerequisites:
Access to AWS account, AWS Glue, AWS S3, AWS SNS and AWS CloudFormation. Glue Database has the same name as target S3 bucket.

Steps:
1. Upload file (glue-catalog-validation.yaml) from local machine (or S3 if it is saved there) to CloudFormation to deploy stack
to create result S3 and SNS topic.
stackname should be <dish aws account name (replace dot with two dashes)>---gluevalidation
2. Create a folder called validationfile in result S3 and upload file (glue-catalog-validation.py) under that folder
3. Upload file (glue-catalog-validation-glue-job.yaml) from local machine (or S3 if it is saved there) to CloudFormation to deploy stack to create glue job. 
stackname should be <dish aws account name (replace dot with underscore)>---<target S3 bucket>
4. Go to glue job called <dish aws account name (replace dot with underscore)>---<target S3 bucket> and then click run button on right top conner to trigger the validation work.
5. Results should be saved in result S3 under test-result folder, and an alert will be sent to metadq@dish.com. (more emails can be added in SNS)
6. Once one validation work is done, the glue job stack (<dish aws account name (replace dot with underscore)>---<target S3 bucket>) should be deleted in CloudFormation.

Example of stackname for glue-catalog-validation.yaml:
account name -> aws-5g.dp.mss.data-science.dev
stack name -> aws-5g--dp--mss--data-science--dev---gluevalidation

Example of stackname for glue-catalog-validation-glue-job.yaml:
account name -> aws-5g.dp.mss.data-science.dev
target S3 name -> d.use1.dish-boost.cxo.obf.g
stack name -> aws-5g--dp--mss--data-science--dev---d--use1--dish-boost--cxo--obf--g

## Future work:
If got permission, it is possible to create role for glue job in yaml file.
___

