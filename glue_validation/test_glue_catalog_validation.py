"""
Pytest file to test glue_catalog_validation.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""
import pytest
from glue_catalog_validation import *

# def test_get_stack_name():
#     pass
#     # From glue job instance, not able to test in sagemaker

# def test_get_glue_database_name():
#     pass
#     # From glue job instance, not able to test in sagemaker

# # Because there is no correct permission from SageMaker to Glue Database,
# # it is not possible to test it here.
# @pytest.mark.test_glue_database_list
# @pytest.mark.parametrize(
#     "glue_database_name",
#     [
#         "lambdaglue2"
#     ]
# )
# def test_glue_database_list_correct(glue_database_name):
#     """
#     Test function bucket_validation with correct input:
#         glue_database_name
#     Pass criteria:
#         result_set equals to expected_set
#     """
#     expected_set = set(['boost_xp_aggregated',
#                         'boost_xp_data',
#                         'boost_xp_voice',
#                         'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a',
#                         'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_ab_b_b_b_b_b_b_b_b',
#                         'unsaved'])
#     result_set = set(glue_database_list(glue_database_name))
#     assert expected_set == result_set