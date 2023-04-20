# Contributing to Data Quality RuleBook

## Introduction
Adding your data quality rulecheck to _dp-data-quality_ is easy with the following steps.

## Adding Your RuleCheck/RuleBook to _dp-data-quality_

### Cloning dp-data-quality repository
It is assumed that you have already cloned the _dp-data-quality_ __main__ branch to your local machine or in AWS environment for your use. If you have not done this already, perfrom the following command on either your local machine or AWS environment.
```console
$ git clone git@github.com:DISHDevEx/dp-data-quality.git
```

### Creating a Branch
As standard with _git_, you need to have the main branch cloned in order to create a branch. Perform the following commands within your _dp-data-quality_ repository to create your branch:

1. Navigate into the root _dp-data-quality_ directory.
```console
$ cd dp-data-quality
```

2. Get a fresh pull on the the _dp-data-quality_ repository:
```console
$ git pull
```

3. Next, create a branch from _main_ on your local machine by executing following command; you will need to replace the fields with your first name and the name of your algorithm:
```console
$ git branch firstname/name_of_feature
```

4. You will now need to switch your branch to the new branch you created. To do this, execute the following command, replacing _firstname/name_of_feature_ with your previously defined branch name:
```console
$ git checkout firstname/name_of_feature
```
5. Confirm you are now working in your branch by issuing the branch command:
```console
$ git branch
```
6. Now, you will need to push your current branch and set it as the upstream equivalent.  This will add your branch held on your local machine to the repository online:
```console
$ git push --set-upstream origin firstname/name_of_feature
```

### Adding Your Rule Check to Data Quality Validation Module
Adding your data quality rule check to the _dp-data-quality_ library is a relatively easy process; however, it is important that the steps below are followed closely to ensure the new rule check fits correctly within the existing framework. To make things easy to follow, an example class ___circles___ has been included as an example to follow.

#### 1. [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory in [_dp-data-quality_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Directory
Navigate to the _data_qaulity_validation_ sub-directory within the _dp-data-quality_ repository:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        spark_setup.py
        read_data.py
        json_to_dataframe.py
        validation_rulebook.py
        quality_report.py
    > [OtherPackages]
```
#### 2a. Add a New Generic Rule Check to [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory
In order to add a generic rule check to the sub-directory, update __GenericRulebook__ class in [__validation_rulebook.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/data_validation.py) by adding your generic rule check. For a new function named generic_check_function, the data_quality_validation sub-directy will be updated as follows:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        spark_setup.py
        read_data.py
        json_to_dataframe.py
        validation_rulebook.py
            class GenericRulebook:
                def generic_check_function():
                    pass
        quality_report.py
            class QualityReport(DatatypeRulebook):
                def category_message():
                    """
                    Update validation_dict with validation category and
                    message of your rule check for data quality report.
                    """
                    pass
    > [OtherPackages]
```
#### 2b. Add a New Datatype-Specific Rule Check to [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory
In order to add a datatype-specific rule check to the data quality rulebook, update __DatatypeRulebook__ class in [__validation_rulebook.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/data_validation.py) by adding your datatype specific rule check. For a new function named datatype_check_function, the data_quality_validation sub-directy will be updated as follows:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        spark_setup.py
        read_data.py
        json_to_dataframe.py
        data_rulebook.py
            class DatatypeRulebook(GenericRuleBook):
                def datatype_check_function():
                    pass
                def datatype_validation_functions:
                    """
                    Update function_dict with new datatype_check_function
                    """
                    pass
        quality_report.py
            class QualityReport(DatatypeRulebook):
                def category_message():
                    """
                    Update validation_dict with validation category and
                    message of your rule check for data quality report.
                    """
                    pass
    > [OtherPackages]
```

#### 2c. Add a New Business Usecase Specific Rulebook to [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory
In order to add a new rulebook with checks specific to your business usecase, create a new class in [__validation_rulebook.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/data_validation.py) that inherits from GenericRulebook class and add rule checks in it. For a new rulebook named Business Usecase Rulebook, the data_quality_validation sub-directy will be updated as follows:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        spark_setup.py
        read_data.py
        json_to_dataframe.py
        data_rulebook.py
            class BusinessUsecaseRulebook(GenericRulebook):
                def business_rulecheck1():
                    pass
                def business_rulecheck2():
                    pass
                def business_usecase_validation_functions:
                    """
                    Update dictionary with new business usecase functions
                    """
                    pass
        quality_report.py
            class QualityReport(DatatypeRulebook, BusinessUsecaseRulebook):
                def category_message():
                    """
                    Update validation_dict with validation category and
                    message of your rule checks for data quality report.
                    """
                    pass
    > [OtherPackages]
```

#### 3. Add Newly Added Rulebook to __init__ File
The __init__ file is used in Python to give access to aspects of your code from a higher level.  The general process is to explicitly elevate functions and classes that should be accessible at the level of the directory in-which the __init__ file resides.

This step is very important for ensuring your newly added rulebook is able to interact correctly with the rest of the _dp-data-quality_ structure and be available for use when the package is installed. For this step, update __init__ file in data_quality_validation sub-directory with rulebook's class name.

**EXAMPLE OF NEW CLASS WITH A FUNCTION OR TWO HERE**

#### 4. Add Dependencies to [_requirements.txt_](requirements.txt)

Add all dependencies included in your algorithm / sub-package in the [_requirements.txt_](requirements.txt) file.  You must also include the package version number for your dependency.  This locks down the version you are using and prevents version conflicts if present.

Add your dependencies with the following format, this is taken directly from the existing [_requirements.txt_](requirements.txt) file:

```python
pandas==1.4.3
numpy==1.23.1
```
If your version specific dependencies are already included in the list, do not duplicate them.

## Adding Your Test Cases

**EXAMPLE OF TEST FUNCTIONS FOR FUNCTIONS ADDED ABOVE**

## README.md File Requierments

Each subpackage contributed to *dp-data-quality* must have a README.md file included. This tells other users how to sussecfully use your functions, the use cases for each function, and the expected outputs.  The README.md files should be included in the following location:

```console
> dp-data-quality
    > data_quality_validation
        __init__.py
-->     README.md
        [OtherFiles]
    > [OtherPackages]
```
The minimum topics for inclusion in the README file are as follows:

1. Detailed description of each input
2. Detailed description of the expected output
3. An example call for each function you are contributing

## Submitting a Pull Request
Once all of the above steps are completed, your unit tests are all passing, and you are able to succesfully run *data_quality_validation* module on your local machine or in AWS environment, it's now time to submit a pull request.

Pull requests are easy to complete on Github. Perform the following steps:

1. Commit and push your branch to the *dp-data-quality* repository
2. On Github, navigate to the *dp-data-quality* repository and then to your branch firstname/name_of_feature
3. On the right-hand-side, below Code icon, select **Contribute --> Open Pull Request**
4. Select your branch as compare, select *main* as the base
5. Submit the pull request
6. A senior member of the team will approve the pull request and reach out if any alterations are needed
