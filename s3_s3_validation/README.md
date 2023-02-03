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
# Pylint and Pytest results:
## Pylint:
(virenv) zheng.liu@D4XYQQTVGF production % pylint forpull/s3_to_s3_validation_script.py 
************* Module s3_to_s3_validation_script
forpull/s3_to_s3_validation_script.py:14:0: C0301: Line too long (123/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:15:0: C0301: Line too long (106/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:28:0: C0301: Line too long (119/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:33:0: C0301: Line too long (106/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:75:0: C0301: Line too long (124/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:97:0: C0301: Line too long (119/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:171:0: C0301: Line too long (133/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:241:0: C0301: Line too long (132/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:326:0: C0301: Line too long (103/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:356:0: C0301: Line too long (103/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:539:0: C0301: Line too long (138/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:597:0: C0301: Line too long (127/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:598:0: C0301: Line too long (127/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:599:0: C0301: Line too long (127/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:615:0: C0301: Line too long (132/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:647:0: C0301: Line too long (111/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:649:0: C0301: Line too long (120/100) (line-too-long)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'abs' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'max' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'min' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'sum' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'pow' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'round' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'hash' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'ascii' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'bin' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'hex' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'slice' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:45:0: W0622: Redefining built-in 'filter' (redefined-builtin)
forpull/s3_to_s3_validation_script.py:42:0: E0401: Unable to import 'awsglue.utils' (import-error)
forpull/s3_to_s3_validation_script.py:44:0: E0401: Unable to import 'awsglue.context' (import-error)
forpull/s3_to_s3_validation_script.py:45:0: W0401: Wildcard import pyspark.sql.functions (wildcard-import)
forpull/s3_to_s3_validation_script.py:50:0: R0902: Too many instance attributes (12/7) (too-many-instance-attributes)
forpull/s3_to_s3_validation_script.py:54:4: R0913: Too many arguments (13/5) (too-many-arguments)
forpull/s3_to_s3_validation_script.py:75:4: R0913: Too many arguments (7/5) (too-many-arguments)
forpull/s3_to_s3_validation_script.py:93:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:112:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:138:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:157:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:147:4: R1710: Either all return statements in a function should return an expression, or none of them should. (inconsistent-return-statements)
forpull/s3_to_s3_validation_script.py:172:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:165:4: R1710: Either all return statements in a function should return an expression, or none of them should. (inconsistent-return-statements)
forpull/s3_to_s3_validation_script.py:188:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:185:12: C0103: Variable name "sc" doesn't conform to snake_case naming style (invalid-name)
forpull/s3_to_s3_validation_script.py:186:12: C0103: Variable name "glueContext" doesn't conform to snake_case naming style (invalid-name)
forpull/s3_to_s3_validation_script.py:202:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:216:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:229:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:223:4: R1710: Either all return statements in a function should return an expression, or none of them should. (inconsistent-return-statements)
forpull/s3_to_s3_validation_script.py:242:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:236:4: R1710: Either all return statements in a function should return an expression, or none of them should. (inconsistent-return-statements)
forpull/s3_to_s3_validation_script.py:260:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:270:23: C0103: Argument name "df" doesn't conform to snake_case naming style (invalid-name)
forpull/s3_to_s3_validation_script.py:280:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:270:4: R1710: Either all return statements in a function should return an expression, or none of them should. (inconsistent-return-statements)
forpull/s3_to_s3_validation_script.py:300:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:324:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:354:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:384:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:375:4: R1710: Either all return statements in a function should return an expression, or none of them should. (inconsistent-return-statements)
forpull/s3_to_s3_validation_script.py:401:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:392:4: R1710: Either all return statements in a function should return an expression, or none of them should. (inconsistent-return-statements)
forpull/s3_to_s3_validation_script.py:426:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:422:12: C0103: Variable name "joinType" doesn't conform to snake_case naming style (invalid-name)
forpull/s3_to_s3_validation_script.py:440:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:451:4: R0913: Too many arguments (7/5) (too-many-arguments)
forpull/s3_to_s3_validation_script.py:469:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:466:12: C0103: Variable name "joinType" doesn't conform to snake_case naming style (invalid-name)
forpull/s3_to_s3_validation_script.py:483:31: C0103: Argument name "df" doesn't conform to snake_case naming style (invalid-name)
forpull/s3_to_s3_validation_script.py:493:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:504:57: C0103: Argument name "df" doesn't conform to snake_case naming style (invalid-name)
forpull/s3_to_s3_validation_script.py:528:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:515:16: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:546:8: W0702: No exception type(s) specified (bare-except)
forpull/s3_to_s3_validation_script.py:50:0: R0904: Too many public methods (23/20) (too-many-public-methods)
forpull/s3_to_s3_validation_script.py:556:0: R0914: Too many local variables (25/15) (too-many-locals)
forpull/s3_to_s3_validation_script.py:556:0: R0915: Too many statements (52/50) (too-many-statements)
forpull/s3_to_s3_validation_script.py:44:0: C0412: Imports from package awsglue are not grouped (ungrouped-imports)
forpull/s3_to_s3_validation_script.py:45:0: C0412: Imports from package pyspark are not grouped (ungrouped-imports)
forpull/s3_to_s3_validation_script.py:45:0: W0614: Unused import(s) inspect, functools, warnings, lit, col, column, asc, desc, sqrt, abs, max, min, max_by, min_by, count, sum, avg, mean, sumDistinct, sum_distinct, product, acos, acosh, asin, asinh, atan, atanh, cbrt, ceil, cos, cosh, cot, csc, exp, expm1, floor, log, log10, log1p, rint, sec, signum, sin, sinh, tan, tanh, toDegrees, toRadians, bitwiseNOT, bitwise_not, asc_nulls_first, asc_nulls_last, desc_nulls_first, desc_nulls_last, stddev, stddev_samp, stddev_pop, variance, var_samp, var_pop, skewness, kurtosis, collect_list, collect_set, degrees, radians, atan2, hypot, pow, row_number, dense_rank, rank, cume_dist, percent_rank, approxCountDistinct, approx_count_distinct, broadcast, coalesce, corr, covar_pop, covar_samp, countDistinct, count_distinct, first, grouping, grouping_id, input_file_name, isnan, isnull, last, monotonically_increasing_id, nanvl, percentile_approx, rand, randn, round, bround, shiftLeft, shiftleft, shiftRight, shiftright, shiftRightUnsigned, shiftrightunsigned, spark_partition_id, expr, struct, greatest, least, when, log2, conv, factorial, lag, lead, nth_value, ntile, current_date, current_timestamp, date_format, year, quarter, month, dayofweek, dayofmonth, dayofyear, hour, minute, second, weekofyear, make_date, date_add, date_sub, datediff, add_months, months_between, to_date, to_timestamp, trunc, date_trunc, next_day, last_day, from_unixtime, unix_timestamp, from_utc_timestamp, to_utc_timestamp, timestamp_seconds, window, session_window, crc32, md5, sha1, sha2, hash, xxhash64, assert_true, raise_error, upper, lower, ascii, base64, unbase64, ltrim, rtrim, trim, concat_ws, decode, encode, format_number, format_string, instr, overlay, sentences, substring, substring_index, levenshtein, locate, lpad, rpad, repeat, split, regexp_extract, regexp_replace, initcap, soundex, bin, hex, unhex, length, octet_length, bit_length, translate, create_map, map_from_arrays, array, array_contains, arrays_overlap, slice, array_join, concat, array_position, element_at, array_remove, array_distinct, array_intersect, array_union, array_except, explode, posexplode, explode_outer, posexplode_outer, get_json_object, json_tuple, from_json, to_json, schema_of_json, schema_of_csv, to_csv, size, array_min, array_max, sort_array, array_sort, shuffle, reverse, flatten, map_keys, map_values, map_entries, map_from_entries, array_repeat, arrays_zip, map_concat, sequence, from_csv, transform, exists, forall, filter, aggregate, zip_with, transform_keys, transform_values, map_filter, map_zip_with, years, months, days, hours, bucket, udf, Any, cast, Callable, Dict, List, Iterable, overload, Optional, Tuple, TYPE_CHECKING, Union, ValuesView, since, PythonEvalType, Column, DataFrame, ArrayType, DataType, StringType, StructType, UserDefinedFunction, pandas_udf, PandasUDFType, to_str, ColumnOrName, ColumnOrName_, DataTypeOrString and UserDefinedFunctionLike from wildcard import of pyspark.sql.functions (unused-wildcard-import)

------------------------------------------------------------------
Your code has been rated at 7.65/10 (previous run: 7.35/10, +0.30)
## Pytest:
(venv) sagemaker-user@studio$ pytest test_s3_to_s3_validation_script.py --verbose
================================== test session starts ===================================
platform linux -- Python 3.9.15, pytest-7.2.1, pluggy-1.0.0 -- /home/sagemaker-user/git/skynet/venv/bin/python
cachedir: .pytest_cache
rootdir: /home/sagemaker-user/git/skynet
collected 17 items                                                                       

test_s3_to_s3_validation_script.py::test_get_current_denver_time PASSED            [  5%]
test_s3_to_s3_validation_script.py::test_generate_result_location PASSED           [ 11%]
test_s3_to_s3_validation_script.py::test_initial_boto3_client PASSED               [ 17%]
test_s3_to_s3_validation_script.py::test_initial_boto3_resource PASSED             [ 23%]
test_s3_to_s3_validation_script.py::test_get_sns_name PASSED                       [ 29%]
test_s3_to_s3_validation_script.py::test_get_sns_arn PASSED                        [ 35%]
test_s3_to_s3_validation_script.py::test_rename_columns PASSED                     [ 41%]
test_s3_to_s3_validation_script.py::test_file_to_pyspark_df PASSED                 [ 47%]
test_s3_to_s3_validation_script.py::test_list_to_pyspark_df PASSED                 [ 52%]
test_s3_to_s3_validation_script.py::test_s3_obj_to_pyspark_df PASSED               [ 58%]
test_s3_to_s3_validation_script.py::test_get_script_prefix PASSED                  [ 64%]
test_s3_to_s3_validation_script.py::test_remove_script_from_df PASSED              [ 70%]
test_s3_to_s3_validation_script.py::test_get_missing_objects PASSED                [ 76%]
test_s3_to_s3_validation_script.py::test_get_df_count PASSED                       [ 82%]
test_s3_to_s3_validation_script.py::test_get_match_objects PASSED                  [ 88%]
test_s3_to_s3_validation_script.py::test_get_wrong_size_objects PASSED             [ 94%]
test_s3_to_s3_validation_script.py::test_save_result PASSED                        [100%]
___
