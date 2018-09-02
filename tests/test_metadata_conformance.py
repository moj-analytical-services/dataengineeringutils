

import unittest
from dataengineeringutils.metadata_conformance import _pd_df_contains_same_columns_as_metadata
from dataengineeringutils.metadata_conformance import _pd_df_cols_matches_metadata_column_ordered
from dataengineeringutils.metadata_conformance import _check_pd_df_contains_same_columns_as_metadata
from dataengineeringutils.metadata_conformance import _check_pd_df_cols_matches_metadata_column_ordered
from dataengineeringutils.metadata_conformance import *
import pandas as pd
import os
import json

def read_json_from_path(path):
    with open(path) as f:
        return_json = json.load(f)
    return return_json

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

def td_path(path):
    return os.path.join(THIS_DIR, "test_data", path)

class ConformanceTest(unittest.TestCase) :
    """
    Test packages utilities functions
    """
    def test_pd_df_contains_same_columns_as_metadata(self) :

        df = pd.read_csv(td_path("test_csv_data_valid.csv"))
        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))

        self.assertTrue(_pd_df_contains_same_columns_as_metadata(df, table_metadata))
        _check_pd_df_contains_same_columns_as_metadata(df, table_metadata)

        df = pd.read_csv(td_path("test_csv_data_additional_col.csv"))
        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))

        self.assertFalse(_pd_df_contains_same_columns_as_metadata(df, table_metadata))
        with self.assertRaises(ValueError):
            _check_pd_df_contains_same_columns_as_metadata(df, table_metadata)


    def test_pd_df_cols_matches_metadata_column_ordered(self) :

        df = pd.read_csv(td_path("test_csv_data_valid.csv"))
        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))

        self.assertTrue(_pd_df_cols_matches_metadata_column_ordered(df, table_metadata))
        _check_pd_df_cols_matches_metadata_column_ordered(df, table_metadata)

        df = pd.read_csv(td_path("test_csv_data_additional_col.csv"))
        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))

        self.assertFalse(_pd_df_cols_matches_metadata_column_ordered(df, table_metadata))
        with self.assertRaises(ValueError):
            _check_pd_df_cols_matches_metadata_column_ordered(df, table_metadata)

    def test_pd_df_matches_metadata_data_types(self):
        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))
        df = pd_read_csv_using_metadata(td_path("test_csv_data_valid.csv"), table_metadata)

        # df = pd.read_csv(td_path("test_csv_data_valid.csv"))
        # table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))

        pd_df_matches_metadata_data_types(df, table_metadata)
