# DISHDataQuality1


## Glue catalog validation (3 files): 
___

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
___

