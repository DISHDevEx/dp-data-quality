# Contributing to Data Quality RuleBook

## Introduction
Adding your data quality rulecheck or rulebook to _dp-data-quality_ is easy with the following steps.

## Adding Your RuleCheck/RuleBook to _dp-data-quality_

### Cloning dp-data-quality repository
It is assumed that you have already cloned the _dp-data-quality_ __main__ branch to your local machine or in AWS environment for use. If you have not done this already, run the following command on either your local machine or AWS environment.
```console
$ git clone git@github.com:DISHDevEx/dp-data-quality.git
```

### Creating a Branch
As standard with _git_, in order to contibute to a repo you need to creare your own branch that is cloned from the main. Run the following commands within your _dp-data-quality_ repository to create your branch:

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
Adding your data quality rule check to the _dp-data-quality_ library is a relatively easy process; however, it is important that the steps below are followed closely to ensure the new rule check fits correctly within the existing framework. To make things easy to follow, an example function ___sensitive_information_validation___ has been included as an example to follow.

#### 1. [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory in [_dp-data-quality_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Directory
Navigate to the _data_qaulity_validation_ sub-directory within the _dp-data-quality_ repository:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        README.md
        spark_setup.py
        json_to_dataframe.py
        read_data.py
        validation_rulebook.py
        quality_report.py
    > [OtherPackages]
```
#### 2a. Add a New Generic Rule Check to [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory
In order to add a generic rule check to the sub-directory, update __GenericRulebook__ class in [__validation_rulebook.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/data_validation.py) by adding your generic rule check. Then, update __QualityReport__ class in [__quality_report.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/quality_report.py) by updating _category_message_ function with validation category and message for your rule check that will be included in data quality report. For a new function named generic_check_function, the data_quality_validation sub-directy will be updated as follows:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        README.md
        spark_setup.py
        read_data.py
        json_to_dataframe.py

        validation_rulebook.py
            class GenericRulebook:
                def generic_check_function():
                    """Add your rule check."""
                    pass

        quality_report.py
            class QualityReport(DatatypeRulebook):
                def category_message():
                    """Add validation category and message for your rule check in data quality report."""
                    pass

    > [OtherPackages]
```
#### 2b. Add a New Datatype-Specific Rule Check to [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory
In order to add a datatype-specific rule check to the data quality rulebook, update __DatatypeRulebook__ class in [__validation_rulebook.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/data_validation.py) by adding your datatype specific rule check. Additionally, update dictionary in _datatype_validation_functions_ to map a datatype to your datatype-specific rule check. Then, update __QualityReport__ class in [__quality_report.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/quality_report.py) by updating _category_message_ function with validation category and message for your rule check that will be displayed in data quality report. For a new function named datatype_check_function, the data_quality_validation sub-directy will be updated as follows:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        README.md
        spark_setup.py
        read_data.py
        json_to_dataframe.py

        validation_rulebook.py
            class DatatypeRulebook(GenericRuleBook):
                def datatype_check_function():
                    """Add your datatype-specific rule check."""
                    pass
                def datatype_validation_functions:
                    """Update dictionary to map a datatype to your datatype-specific rule check."""
                    pass

        quality_report.py
            class QualityReport(DatatypeRulebook):
                def category_message():
                    """Add validation category and message for your rule check in data quality report."""
                    pass

    > [OtherPackages]
```

#### 2c. Add a New Business Usecase Specific Rulebook to [_data_quality_validation_](https://github.com/DISHDevEx/dp-data-quality/tree/main/validation) Sub-Directory
In order to add a new rulebook with checks specific to your business usecase, create a new class in [__validation_rulebook.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/data_validation.py) that inherits from GenericRulebook class and add your rule checks in it. Then, update __QualityReport__ class in [__quality_report.py__](https://github.com/DISHDevEx/dp-data-quality/blob/main/validation/quality_report.py) by updating _category_message_ function with validation category and message for your rule check that will be displayed in data quality report. For a new rulebook named Business Usecase Rulebook, the data_quality_validation sub-directy will be updated as follows:
```console
> dp-data-quality
    > data_quality_validation
        __init__.py
        spark_setup.py
        read_data.py
        json_to_dataframe.py

        validation_rulebook.py
            class BusinessUsecaseRulebook(GenericRulebook):
                def business_rulecheck1():
                    """Add your usecase specific rule check."""
                    pass
                def business_rulecheck2():
                    """Add your usecase specific rule check."""
                    pass

        quality_report.py
            class QualityReport(DatatypeRulebook, BusinessUsecaseRulebook):
                def category_message():
                    """Add validation category and message for your rule check in data quality report."""
                    pass

    > [OtherPackages]
```

#### 3. Add Newly Added Rulebook to __init__ File
The __init__ file is used in Python to give access to aspects of your code from a higher level.  The general process is to explicitly elevate functions and classes that should be accessible at the level of the directory in-which the __init__ file resides.

This step is very important for ensuring your newly added rulebook is able to interact correctly with the rest of the _dp-data-quality_ structure and be available for use when the package is installed. For this step, update __init__ file in data_quality_validation sub-directory with rulebook's class name.

Add new rule check to _validation_rulebook.py_ in appropriate class.

```console
> validation_rulebook.py
    class GenericRulebook:

        def sensitive_information_check(self, data_df, column):
            """
            Method to check for presence of Personal Identifiable Information in dataframe.

            Parameters:
                data_df - dataframe of data
                column - name of column to be validated

            Returns:
                validation - validation ID
                column - name of validated column
                fail_row_id - list of row IDs that failed validation
            """

            validation = 16


            data_df = data_df.select(column, 'ROW_ID').na.drop(subset=[column])

            # Regex to capture phone numbers with or without hyphens and parenthesis
            phone_regex = r'^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$'
            email_regex = r'^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$'

            data_df  = data_df.filter(data_df[column].rlike(phone_regex) |
                                      data_df[column].rlike(email_regex))
            # Collect row IDs in the column that match regex pattern
            row_id = [data[0] for data in data_df.select('ROW_ID').collect()]

            return validation, column, row_id
```

Update _QualityReport_ class in _quality_report.py_ file.

```console
> quality_report.py
    class QualityReport:

    def category_message(self, validation):
        """
        Method to identify validation category and message based on validation ID.

        Parameters:
            validation: validation ID

        Returns:
            validation_category: validation category in data quality report
            validation_message: validation message in data quality report
        """
        validation_dict = {
            .....
            .....
            .....
            16 : ['Sensitive Validation', 'Sensitive information'],
            .....
        }

        return validation_dict.get(validation, [None, None])
```

#### 4. Add Dependencies to [_requirements.txt_](requirements.txt)

Add all dependencies included in your algorithm / sub-package in the [_requirements.txt_](requirements.txt) file.  You must also include the package version number for your dependency.  This locks down the version you are using and prevents version conflicts if present.

Add your dependencies with the following format, this is taken directly from the existing [_requirements.txt_](requirements.txt) file:

```python
pandas>=1.3.5
numpy>=1.21.6
pyspark>=3.3.0
```
If your version specific dependencies are already included in the list, do not duplicate them.

## Adding Your Test Cases

In order to maintain the integrity of the dp-data-quality library, grow the set of rulechecks and rulebooks sustainably, and future proof the code with increased maintainability. For this reason, all new rulechecks and rulebooks are required to include unit tests. This is the process of testing each function of the code individually to gain insight to how each aspect of the code is performing and make the process of debugging much easier.

In dp-data-quality, pytest framework is used to create a simple and scalable testing environment.

All unit tests must be stored in the tests directory at the root of the dp-data-quality structure. In this directory, create a Python file with the naming format as follows:
```console
> dp-data-quality
    > data_quality_validation
    > tests
        test_<FeatureName>.py
```
Replace FeatureName with the name of your rulecheck or rulebook, this should match the feature name you included on your branch.

For the sensitive_information_validation example, the test file must look as follows:
```console
> dp-data-quality
    > data_quality_validation
    >tests
        test_sensitive_information_validation.py
```
## README.md File Requirements

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
