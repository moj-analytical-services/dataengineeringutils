

import unittest
from dataengineeringutils.metadata_conformance import _pd_df_contains_same_columns_as_metadata
from dataengineeringutils.metadata_conformance import _pd_df_cols_matches_metadata_column_ordered
from dataengineeringutils.metadata_conformance import _check_pd_df_contains_same_columns_as_metadata
from dataengineeringutils.metadata_conformance import _check_pd_df_cols_matches_metadata_column_ordered
from dataengineeringutils.metadata_conformance import _check_pd_df_conforms_to_metadata_data_types
from dataengineeringutils.metadata_conformance import *
import pandas as pd
import os
import json
import random

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

    def test_pd_df_conforms_to_metadata_data_types(self):

        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))
        df = pd_read_csv_using_metadata(td_path("test_csv_data_valid.csv"), table_metadata)
        self.assertTrue(pd_df_conforms_to_metadata_data_types(df, table_metadata))

        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))
        df = pd.read_csv(td_path("test_csv_data_valid.csv"))
        self.assertFalse(pd_df_conforms_to_metadata_data_types(df, table_metadata))

    def test_impose_metadata_column_order_on_pd_df(self):

        # Test tables with right number of cols, possibly in wrong order

        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))
        df = pd_read_csv_using_metadata(td_path("test_csv_data_valid.csv"), table_metadata)

        # This should be a no-op
        df = impose_metadata_column_order_on_pd_df(df, table_metadata)
        _check_pd_df_cols_matches_metadata_column_ordered(df, table_metadata)

        df = pd_read_csv_using_metadata(td_path("test_csv_data_valid_wrong_order.csv"), table_metadata)

        with self.assertRaises(ValueError):
            _check_pd_df_cols_matches_metadata_column_ordered(df, table_metadata)

        df = impose_metadata_column_order_on_pd_df(df, table_metadata)
        self.assertTrue(_pd_df_cols_matches_metadata_column_ordered(df, table_metadata))

        # Test tables with missing col
        df = pd_read_csv_using_metadata(td_path("test_csv_data_missing_col.csv"), table_metadata)

        with self.assertRaises(ValueError):  # Shouldn't add cols unless explicitly specified
            impose_metadata_column_order_on_pd_df(df, table_metadata)

        df = impose_metadata_column_order_on_pd_df(df, table_metadata, create_cols_if_not_exist=True)
        self.assertTrue(_pd_df_cols_matches_metadata_column_ordered(df, table_metadata))

        # Test tables with superflouous column
        df = pd_read_csv_using_metadata(td_path("test_csv_data_additional_col.csv"), table_metadata)

        with self.assertRaises(ValueError):  # Shouldn't remove cols unless explicitly specified
            impose_metadata_column_order_on_pd_df(df, table_metadata,delete_superflous_colums=False)

        df = impose_metadata_column_order_on_pd_df(df, table_metadata)

        self.assertTrue(_pd_df_cols_matches_metadata_column_ordered(df, table_metadata))

        # Superflous and missing columns in random order
        df = pd_read_csv_using_metadata(td_path("test_csv_data_additional_col.csv"), table_metadata)

        cols = list(df.columns)
        random.shuffle(cols)
        df = df[cols]
        del df["myint"]

        df = impose_metadata_column_order_on_pd_df(df, table_metadata, create_cols_if_not_exist=True)
        self.assertTrue(_pd_df_cols_matches_metadata_column_ordered(df, table_metadata))

    def test_impose_metadata_data_types_on_pd_df(self):
        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))
        df = pd_read_csv_using_metadata(td_path("test_csv_data_valid.csv"), table_metadata)
        df = impose_metadata_data_types_on_pd_df(df, table_metadata)

        self.assertTrue(pd_df_conforms_to_metadata_data_types(df, table_metadata))

        df = pd.read_csv(td_path("test_csv_data_valid.csv"))

        df = impose_metadata_data_types_on_pd_df(df, table_metadata)

        self.assertTrue(pd_df_conforms_to_metadata_data_types(df, table_metadata))


    def test_impose_exact_conformance_on_pd_df(self):
        table_metadata = read_json_from_path(td_path("test_table_metadata_valid.json"))
        df = pd_read_csv_using_metadata(td_path("test_csv_data_additional_col.csv"), table_metadata)
        df = impose_exact_conformance_on_pd_df(df, table_metadata)
        check_pd_df_exactly_conforms_to_metadata(df, table_metadata)









