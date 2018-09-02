import pkg_resources
import json
import pandas as pd
import numpy as np

def _pd_dtype_dict_from_metadata(table_metadata):
    """
    Convert the table metadata to the dtype dict
    """

    with pkg_resources.resource_stream(__name__, "data/data_type_conversion.csv") as io:
        type_conversion = pd.read_csv(io)

    type_conversion = type_conversion.set_index("metadata")

    type_conversion_dict = type_conversion.to_dict(orient="index")

    table_metadata = table_metadata["columns"]
    dtype = {}
    parse_dates = []

    for c in table_metadata:
        colname = c["name"]
        coltype = c["type"]
        coltype = type_conversion_dict[coltype]['pandas']
        dtype[colname] = np.typeDict[coltype]

    return dtype

def _pd_date_parse_list_from_metadatadata(table_metadata):
    """
    Get list of columns to pass to the pandas.to_csv date_parse argument from table metadata
    """

    table_metadata = table_metadata["columns"]

    parse_dates = []

    for c in table_metadata:
        colname = c["name"]
        coltype = c["type"]

        if coltype in ["date", "datetime"]:
            parse_dates.append(colname)

    return parse_dates


def pd_read_csv_using_metadata(filepath_or_buffer, table_metadata, *args, **kwargs):
    """
    Use pandas to read a csv imposing the datatypes specified in the table_metadata

    Passes through kwargs to pandas.read_csv
    """
    dtype = _pd_dtype_dict_from_metadata(table_metadata)
    parse_dates = _pd_date_parse_list_from_metadatadata(table_metadata)

    return pd.read_csv(filepath_or_buffer, dtype = dtype, parse_dates = parse_dates, *args, **kwargs)

def _pd_df_contains_same_columns_as_metadata(df, table_metadata):
    """
    Check a pandas dataframe contains the same columns as those specified in the table metadata
    """

    pd_columns = set(df.columns)
    md_columns = set([c["name"] for c in table_metadata["columns"]])

    return pd_columns == md_columns

def _check_pd_df_contains_same_columns_as_metadata(df, table_metadata):

    if not _pd_df_contains_same_columns_as_metadata(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")


def _pd_df_cols_matches_metadata_column_ordered(df, table_metadata):

    pd_columns = list(df.columns)
    md_columns = [c["name"] for c in table_metadata["columns"]]

    return pd_columns == md_columns

def _check_pd_df_cols_matches_metadata_column_ordered(df, table_metadata):

    if not _pd_df_contains_same_columns_as_metadata(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")

    if not _pd_df_cols_matches_metadata_column_ordered(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")


def pd_df_matches_metadata_data_types(df, table_metadata):

    expected_dtypes = _pd_dtype_dict_from_metadata(table_metadata)

    date_cols = _pd_date_parse_list_from_metadatadata(table_metadata)

    for col in date_cols:
        expected_dtypes[col] = np.typeDict["datetime64"]

    actual_numpy_types = dict(df.dtypes)

    for dt in actual_numpy_types:
        actual_numpy_types[dt] = actual_numpy_types[dt].type


    return actual_numpy_types == expected_dtypes



    pass

def impose_metadata_column_order_on_pd_df(create_cols_if_not_exist):
    pass

def impose_metadata_data_types_on_pd_df():
    pass

