"""
Pytest file to test glue_catalog_validation.py
Need to execute: pip install pytest, pip install fsspec and pip install s3fs
Need to execute pip install pylint for code score on tested code itself
"""
import pytest
from glue_catalog_validation import * # pylint: disable=wildcard-import unused-wildcard-import

# Need to test this function at first, because it may lead to time discrepancy from SageMaker.
@pytest.mark.test_get_current_denver_time
@pytest.mark.parametrize(
    ["time_zone", "time_format"],
    [
        ("US/Mountain", "%Y%m%d_%H%M%S_%Z_%z")
    ]
)
def test_get_current_denver_time_correct(time_zone, time_format):
    """
    Test function get_current_denver_time with correct input:
        time_zone
        time_format
    Pass criteria:
        result is current_denver_time
    """
    current_denver_time = datetime.now().astimezone(pytz.timezone(time_zone)).strftime(time_format)
    assert get_current_denver_time(time_zone, time_format) == current_denver_time

@pytest.mark.test_get_current_denver_time
@pytest.mark.parametrize(
    ["time_zone", "time_format"],
    [
        ("fake_timezone", "%Y%m%d_%H%M%S_%Z_%z")
    ]
)
def test_get_current_denver_time_incorrect(time_zone, time_format):
    """
    Test function get_current_denver_time with incorrect input:
        time_zone
        time_format
    Pass criteria:
        result is None
    """
    assert get_current_denver_time(time_zone, time_format) == 'cannot_get_timestamp'


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
#     Test function glue_database_list with correct input:
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

@pytest.mark.test_glue_database_list
@pytest.mark.parametrize(
    "glue_database_name",
    [
        "fake",
        ["fake"]
    ]
)
def test_glue_database_list_incorrect(glue_database_name):
    """
    Test function glue_database_list with incorrect input:
        glue_database_name
    Pass criteria:
        result is None
    """
    result = glue_database_list(glue_database_name)
    assert result is None

@pytest.mark.test_bucket_validation
@pytest.mark.parametrize(
    "s3_bucket",
    [
        "s3-validation-demo"
    ]
)
def test_bucket_validation_correct(s3_bucket):
    """
    Test function bucket_validation with correct input:
        s3_bucket
    Pass criteria:
        result is a dict
    """
    result = bucket_validation(s3_bucket)
    assert isinstance(result, dict)

@pytest.mark.test_bucket_validation
@pytest.mark.parametrize(
    "s3_bucket",
    [
        "fake.s3bucket.fake",
        ['fakes3']
    ]
)
def test_bucket_validation_incorrect(s3_bucket):
    """
    Test function bucket_validation with incorrect input:
        s3_bucket
    Pass criteria:
        result is None
    """
    result = bucket_validation(s3_bucket)
    assert result is None

@pytest.mark.test_generate_result_location
@pytest.mark.parametrize(
    "target_bucket",
    [
        "examples3",
        "anotherexample"
    ]
)
def test_generate_result_location_correct(target_bucket):
    """
    Test function generate_result_location with correct input:
        target_bucket
    Pass criteria:
        result equals to expected_string
    """
    expected_string = target_bucket + '/glue_database_validation/'
    result = generate_result_location(target_bucket)
    assert result == expected_string

@pytest.mark.test_generate_result_location
@pytest.mark.parametrize(
    "target_bucket",
    [
        123,
        ['fake']
    ]
)
def test_generate_result_location_incorrect(target_bucket):
    """
    Test function generate_result_location with incorrect input:
        target_bucket
    Pass criteria:
        result is None
    """
    result = bucket_validation(target_bucket)
    assert result is None

@pytest.mark.test_remove_punctuation
@pytest.mark.parametrize(
    "a_string",
    [
        "a,b.c;d:e'f=g+h-i_j)k(l*m&n^o%p$q#r@s!t~u`v"
    ]
)
def test_remove_punctuation_correct(a_string):
    """
    Test function remove_punctuation with correct input:
        a_string
    Pass criteria:
        result equals to expected_string
    """
    expected_string = "a_b_c_d_e_f_g_h_i_j_k_l_m_n_o_p_q_r_s_t_u_v"
    result = remove_punctuation(a_string)
    assert result == expected_string

@pytest.mark.test_remove_punctuation
@pytest.mark.parametrize(
    "a_string",
    [
        123,
        ['fake']
    ]
)
def test_remove_punctuation_incorrect(a_string):
    """
    Test function remove_punctuation with incorrect input:
        a_string
    Pass criteria:
        result is None
    """
    result = remove_punctuation(a_string)
    assert result is None

@pytest.mark.test_scan_s3_bucket_folder_to_list
@pytest.mark.parametrize(
    "target_bucket",
    [
        "d.use1.dish-boost.cxo.obf.g"
    ]
)
def test_scan_s3_bucket_folder_to_list_correct(target_bucket):
    """
    Test function scan_s3_bucket_folder_to_list with correct input:
        target_bucket
    Pass criteria:
        result_set equals to expected_set
    """
    expected_set = set(['unsaved',
        'boost_xp_aggregated', 'boost_xp_data',
        'boost_xp_voice',
        'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a',
        'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_ab_b_b_b_b_b_b_b_b',
        'glue_database_validation'])
    result_set = set(scan_s3_bucket_folder_to_list(target_bucket))
    assert result_set == expected_set

@pytest.mark.test_scan_s3_bucket_folder_to_list
@pytest.mark.parametrize(
    "target_bucket",
    [
        123,
        ['fake'],
        'fake'
    ]
)
def test_scan_s3_bucket_folder_to_list_incorrect(target_bucket):
    """
    Test function scan_s3_bucket_folder_to_list with incorrect input:
        target_bucket
    Pass criteria:
        result is None
    """
    result = scan_s3_bucket_folder_to_list(target_bucket)
    assert result is None

@pytest.mark.test_get_missing_sets
@pytest.mark.parametrize(
    ["list_a", "list_b"],
    [
        (
            ['boost_xp_aggregated',
            'boost_xp_data',
            'boost_xp_voice',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_ab_b_b_b_b_b_b_b_b',
            'unsaved'],
            ['unsaved',
            'boost_xp_aggregated',
            'boost_xp_data',
            'boost_xp_voice',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_ab_b_b_b_b_b_b_b_b',
            'glue_database_validation']
            )
    ]
)
def test_get_missing_sets_correct(list_a, list_b):
    """
    Test function get_missing_sets with correct input:
        list_a, list_b
    Pass criteria:
        result_set_a equals to expected_set_a
        result_set_b equals to expected_set_b
    """
    expected_set_a = {'glue_database_validation'}
    expected_set_b = set()
    result_set_a, result_set_b = get_missing_sets(list_a, list_b)
    assert result_set_a == expected_set_a
    assert result_set_b == expected_set_b

@pytest.mark.test_get_missing_sets
@pytest.mark.parametrize(
    ["list_a", "list_b"],
    [
        (123,[1,2,3]),
        ({'fake'}, ['f','a','k','e']),
        ('fake', ['f','a','k','e'])
    ]
)
def test_get_missing_sets_incorrect(list_a, list_b):
    """
    Test function get_missing_sets with incorrect input:
        list_a, list_b
    Pass criteria:
        result is None
    """
    result = get_missing_sets(list_a, list_b)
    assert result is None

@pytest.mark.test_save_validation_missing_result
@pytest.mark.parametrize(
    ["missing_in_s3",
    "missing_in_glue_database",
    "saving_location",
    "current"],
    [
        (
            ['boost_xp_aggregated',
            'boost_xp_data'],
            ['boost_xp_voice',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a'],
            'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
            '20230310_001157_MST_-0700'
        ),
        (
            'boost_xp_aggregated',
            'boost_xp_voice',
            'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
            '20230310_001158_MST_-0700'
        )
    ]
)
def test_save_validation_missing_result_correct(missing_in_s3,
                                missing_in_glue_database,
                                saving_location,
                                current):
    """
    Test function save_validation_missing_result with correct input:
        missing_in_s3,
        missing_in_glue_database,
        saving_location,
        current
    Pass criteria:
        result is Ture
    """
    result = save_validation_missing_result(missing_in_s3,
                                missing_in_glue_database,
                                saving_location,
                                current)
    assert result is True

@pytest.mark.test_save_validation_missing_result
@pytest.mark.parametrize(
    ["missing_in_s3",
    "missing_in_glue_database",
    "saving_location",
    "current"],
    [
        (
            {'boost_xp_aggregated',
            'boost_xp_data'},
            ['boost_xp_voice',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a'],
            'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
            '20230310_001157_MST_-0700'
        ),
        (
            ['boost_xp_aggregated',
            'boost_xp_data'],
            {'boost_xp_voice',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a'},
            'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
            '20230310_001157_MST_-0700'
        ),
        (
            ['boost_xp_aggregated',
            'boost_xp_data'],
            ['boost_xp_voice',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a'],
            'totallyafakes3bucket/glue_database_validation/',
            '20230310_001157_MST_-0700'
        ),
        (
            ['boost_xp_aggregated',
            'boost_xp_data'],
            ['boost_xp_voice',
            'd_use1_dish_5g_core_orch_b_fhaul_cust_fm_a'],
            'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
            ['20230310_001157_MST_-0700']
        )
    ]
)
def test_save_validation_missing_result_incorrect(missing_in_s3,
                                missing_in_glue_database,
                                saving_location,
                                current):
    """
    Test function save_validation_missing_result with incorrect input:
        missing_in_s3,
        missing_in_glue_database,
        saving_location,
        current
    Pass criteria:
        result is None
    """
    result = save_validation_missing_result(missing_in_s3,
                                missing_in_glue_database,
                                saving_location,
                                current)
    assert result is None

@pytest.mark.test_get_sns_arn
@pytest.mark.parametrize(
    "sns_name",
    [
        "s3-validation-demo"
    ]
)
def test_get_sns_arn_correct(sns_name):
    """
    Can only test same region SNS name.
    Test function get_sns_arn with correct input:
        sns_name
    Pass criteria:
        result_arn equals expected_arn
    """
    expected_arn = "arn:aws:sns:us-west-2:064047601590:s3-validation-demo"
    result_arn = get_sns_arn(sns_name)
    assert result_arn == expected_arn

@pytest.mark.test_get_sns_arn
@pytest.mark.parametrize(
    "sns_name",
    [
        123,
        ['fake'],
        'fake'
    ]
)
def test_get_sns_arn_incorrect(sns_name):
    """
    Test function get_sns_arn with incorrect input:
        sns_name
    Pass criteria:
        result is None
    """
    result = get_sns_arn(sns_name)
    assert result is None


# An error occurred (AuthorizationError)
# when calling the Publish operation:
# User: arn:aws:sts::064047601590:
# assumed-role/AmazonSageMakerServiceCatalogProductsUseRole
# /SageMaker is not authorized to perform: SNS:
# Publish on resource: arn:aws:sns:us-west-2:064047601590:
# s3-validation-demo because no identity-based policy
# allows the SNS:Publish action
# "send_sns_to_subscriber" function completed unsuccessfully.

# @pytest.mark.test_send_sns_to_subscriber
# @pytest.mark.parametrize(
#     ["saving_location", "current",
#     "sns_topic_arn", "message"],
#     [
#         (
#             'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
#             '20230310_001157_MST_-0700',
#             'arn:aws:sns:us-west-2:064047601590:s3-validation-demo',
#             {'correct_1':1, 'correct_2':2}
#         )
#     ]
# )
# def test_send_sns_to_subscriber_correct(saving_location, current,
#         sns_topic_arn, message):
#     """
#     Test function send_sns_to_subscriber with correct input:
#         saving_location, current,
#         sns_topic_arn, message
#     Pass criteria:
#         result['ResponseMetadata']['HTTPStatusCode'] is 200
#     """
#     result = send_sns_to_subscriber(saving_location, current,
#         sns_topic_arn, message)
#     assert result['ResponseMetadata']['HTTPStatusCode'] == 200

# @pytest.mark.test_send_sns_to_subscriber
# @pytest.mark.parametrize(
#     ["saving_location", "current",
#     "sns_topic_arn", "message"],
#     [
#         (
#             ['fake_location'],
#             '20230310_001157_MST_-0700',
#             'arn:aws:sns:us-west-2:064047601590:s3-validation-demo',
#             {'correct_1':1, 'correct_2':2}
#         ),
#         (
#             'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
#             ['fake_time'],
#             'arn:aws:sns:us-west-2:064047601590:s3-validation-demo',
#             {'correct_1':1, 'correct_2':2}
#         ),
#         (
#             'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
#             '20230310_001157_MST_-0700',
#             'arn:aws:sns:us-west-2:064047601590:fakeawssnsarn',
#             {'correct_1':1, 'correct_2':2}
#         ),
#         (
#             'd.use1.dish-boost.cxo.obf.g/glue_database_validation/',
#             '20230310_001157_MST_-0700',
#             'arn:aws:sns:us-west-2:064047601590:s3-validation-demo',
#             'fake_message'
#         )
#     ]
# )
# def test_send_sns_to_subscriber_incorrect(saving_location, current,
#         sns_topic_arn, message):
#     """
#     Test function send_sns_to_subscriber with incorrect input:
#         saving_location, current,
#         sns_topic_arn, message
#     Pass criteria:
#         result is None
#     """
#     result = send_sns_to_subscriber(saving_location, current,
#         sns_topic_arn, message)
#     assert result is None
