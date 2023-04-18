"""
Module to test methods from PayloadValidation class.
"""
import pytest
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list

