# Naming_Validation
___


## Version 2:
___
Environment prerequisite: Python 3.X and Padas intalled. Under this folder, run 'python namingvalidation_version2' and follow the console instruction to do the validaiton. .csv file is required to be placed in the same folder. 
Provide a JSON template (naming_validation_rules_template.json) as an example for users to generate new rules under agreed format.
Naming Convention Rules link: https://docs.google.com/document/d/1uuzWIaokjJ2azOyCiFFwXN3-WlIqP8eFHR3K0Vvb2kE/edit?usp=sharing
Please reach out to neha.yalamanchi@dish.com if you need permission to read it.
The components should be separated by symbol of ./_/-, user needs to input delimiter from 1 of these three in main function. Validation will start from the first component and then the second...until the last component. If all components are valid, the result is saved in validated_s3name.txt. If any component is invalid, the result is saved in failed_s3name.txt and end the execution.
The path of the rule should be passed to read_rule function, so that the values can be used for validation.
Only correct format is useable.
All components become mandatory for validation.
Example of how to use naming-validation_version2.py script: naming_validation_rules_template.json: is the input file for the rules. manual_validation(): dish.ran.bhla.bhla.etc,result/failed_s3name.txt,result/validated_s3name.txt (as an invalid example) d.use1.dish.ran.aws.b.dp.subs.fm.r,result/failed_s3name.txt,result/validated_s3name.txt (as a valid example) file_validation(): test.csv,resourceName,result/failed_s3name.txt,result/validated_s3name.txt (test.csv should be in the same folder of naming-validation_version2.py while executing the py file on that folder level)
Tested on my SageMaker instance, same issue if create under folder of result. However it works well if deleting 'result/' for the paths from the inputs. 
___